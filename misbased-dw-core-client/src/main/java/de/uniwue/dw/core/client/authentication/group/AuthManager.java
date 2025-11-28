package de.uniwue.dw.core.client.authentication.group;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.AuthenticationService;
import de.uniwue.dw.core.client.authentication.ProketPasswordManager;
import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.client.authentication.UserSettings;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.manager.adapter.IDWClientAdapterFactory;
import de.uniwue.dw.core.model.manager.adapter.IUserManager;
import de.uniwue.dw.core.model.manager.adapter.IUserSettingsAdapter;
import de.uniwue.misc.util.ConfigException;

import javax.security.auth.login.AccountException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class AuthManager implements IUserManager {

  public static final String BLACK_LIST = "b";

  public static final String WHITE_LIST = "w";

  private IGroupAdapter groupAdapter;

  private IUserInGroupAdapter userInGroupAdaper;

  private IGroupCatalogPermissionAdapter groupCatalogPermissionAdapter;

  private IGroupCasePermissionAdapter groupCasePermissionAdapter;

  private IUserAdapter userAdapter;

  private IUserSettingsAdapter userSettingsAdapter;

  private final HashMap<String, Group> name2group = new HashMap<>();

  private boolean groupOrPermissionDBchanged = false;

  private final List<IAuthManagerListener> listeners = new ArrayList<>();

  public AuthManager(IDWClientAdapterFactory adapterFactory) throws SQLException {
    initializeAdapters(adapterFactory);
    initializeData();
  }

  public void initializeAdapters(IDWClientAdapterFactory adapterFactory) throws SQLException {
    groupCatalogPermissionAdapter = adapterFactory.createGroupCatalogPermissionAdapter(this);
    userInGroupAdaper = adapterFactory.createUserInGroupAdapter(this);
    groupAdapter = adapterFactory.createGroupAdapter(this);
    groupCasePermissionAdapter = adapterFactory.createGroupCasePermissionAdapter(this);
    userAdapter = adapterFactory.createUserAdapter(this);
    userSettingsAdapter = adapterFactory.createUserSettingsAdapter();
  }

  public void truncateTables() throws SQLException {
    groupAdapter.truncateTable();
    userInGroupAdaper.truncateTable();
    groupCatalogPermissionAdapter.truncateTable();
    groupCasePermissionAdapter.truncateTable();
    userAdapter.truncateTable();
    initializeData();
  }

  private void groupOrPermissionDBchanged() {
    groupOrPermissionDBchanged = true;
    listeners.forEach(IAuthManagerListener::groupOrPermissionDBchanged);
  }

  private void initializeData() throws SQLException {
    name2group.clear();
    loadGroups();
  }

  public List<User> selectAllUser() throws SQLException {
    return userAdapter.selectAllUser();
  }

  public List<String> getUsernames() throws SQLException {
    return userAdapter.getUsernames();
  }

  public void insertUser2Group(String group, String userName) throws SQLException {
    Group selectGroupsByName = groupAdapter.selectGroupsByName(group);
    userInGroupAdaper.insertUser2Group(selectGroupsByName.getId(), userName);
  }

  /**
   * Groups will not be added to user!
   *
   * @param username
   * @return
   * @throws SQLException
   */
  public Optional<User> selectUserByUsername(String username) throws SQLException {
    return userAdapter.selectUserByUsername(username);
  }

  /**
   * Groups will not be added to user!
   *
   * @param username
   * @param password
   * @param service
   * @return
   * @throws AccountException
   * @throws ConfigException
   */
  public User authenticate(String username, String password, AuthenticationService service) throws AccountException {
    try {
      String hashedPassword = getSecureHash(password);
      if (DwClientConfiguration.getInstance().useProketPasswords()) {
        String salt = userAdapter.getSaltForUsername(username);
        if (salt == null) {
          throw new SQLException("No salt could be found for the specified username");
        }
        hashedPassword = ProketPasswordManager.hash(password, salt);
      }
      User user = getUserWithGroups(username, hashedPassword);
      user.setSystem(service);
      return user;
    } catch (SQLException e) {
      throw new AccountException(e.getMessage());
    } catch (NoSuchAlgorithmException e) {
      throw new AccountException(e.getMessage());
    }
  }

  public User getUserWithGroups(String username, String hashedPassword) throws SQLException, AccountException {
    Optional<User> userOp = userAdapter.selectUserByUsername(username, hashedPassword);
    User user = userOp.orElseThrow((() -> new AccountException(AuthenticationService.INVALID_CREDENTIALS)));
    List<Group> groupsOfUser = loadGroups(user);
    user.setGroups(groupsOfUser);
    return user;
  }

  public void addUserWithHashedPasswordAndSalt(String username, String hashedPassword, String salt, String first,
          String last, String email) throws NoSuchAlgorithmException, SQLException {
    if (DwClientConfiguration.getInstance().useProketPasswords()) {
      userAdapter.addUserWithSalt(username, hashedPassword, salt, first, last, email);
    } else {
      addUser(username, hashedPassword, first, last, email, true);
    }
  }

  public void addUser(String username, String password, String first, String last, String email,
          boolean passwordAlreadyHashed) throws NoSuchAlgorithmException, SQLException {
    String hashedPassword;
    if (!passwordAlreadyHashed) {
      if (!DwClientConfiguration.getInstance().useProketPasswords()) {
        hashedPassword = getSecureHash(password);
      } else {
        String salt = ProketPasswordManager.generateSalt();
        hashedPassword = ProketPasswordManager.hash(password, salt);
        userAdapter.addUserWithSalt(username, hashedPassword, salt, first, last, email);
        return;
      }
    } else {
      hashedPassword = password;
    }
    userAdapter.addUser(username, hashedPassword, first, last, email);
  }

  public void addUser(String username, String unHashedPassword, String first, String last, String email)
          throws NoSuchAlgorithmException, SQLException {
    addUser(username, unHashedPassword, first, last, email, false);
  }

  private String getSecureHash(String password) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("md5");
    password = convertByteArrayToHexString(md.digest(password.getBytes()));
    return password;
  }

  public String convertByteArrayToHexString(byte[] arrayBytes) {
    StringBuffer stringBuffer = new StringBuffer();
    for (int i = 0; i < arrayBytes.length; i++) {
      stringBuffer.append(Integer.toString((arrayBytes[i] & 0xff) + 0x100, 16).substring(1));
    }
    return stringBuffer.toString();
  }

  public List<Group> loadGroups(User user) throws SQLException {
    List<Group> groupsOfUser = loadGroupsWithoutPermissions(user.getUsername());
    reinitializeNecessary();
    return groupsOfUser.stream().map(Group::getName).map(n -> getGroup(n)).filter(Objects::nonNull)
            .collect(Collectors.toList());
    // loadAndAddPermissions2Groups(groupsOfUser);
    // return groupsOfUser;
  }

  private void loadAndAddPermissions2Groups(List<Group> groups) throws SQLException {
    loadAndAddCatalogPermissions2Groups(groups);
    loadAndAddCasePermission2Groups(groups);
  }

  public Group addGroup(String name, int kAnonymity, boolean caseQuery, boolean admin) throws SQLException {
    Group result = groupAdapter.insertGroup(name, kAnonymity, caseQuery, admin);
    groupAdapter.commit();
    groupOrPermissionDBchanged();
    name2group.put(name, result);
    return result;
  }

  public void deleteGroup(int groupId) throws SQLException {
    groupAdapter.deleteGroupById(groupId);
    groupAdapter.commit();
    userInGroupAdaper.removeRowByGroup(groupId);
    userInGroupAdaper.commit();
    groupOrPermissionDBchanged();
  }

  public void updateGroup(int groupId, String name, int kAnonymity, Boolean case_query, Boolean admin)
          throws SQLException {
    groupAdapter.updateGroupById(groupId, name, kAnonymity, case_query, admin);
    groupAdapter.commit();
    groupOrPermissionDBchanged();
  }

  /**
   * Returns the group with the given name or null if it doesn't exist.
   *
   * @param name
   * @return group or null
   */
  public Group getGroup(String name) {
    try {
      reinitializeNecessary();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return name2group.get(name);
  }

  public void addUser2Group(String userName, String groupName) throws SQLException {
    Group result = getGroup(groupName);
    if (result == null) {
      throw new SQLException("Group does not exist.");
    }
    userInGroupAdaper.insertUser2Group(result.getId(), userName);
    groupOrPermissionDBchanged();
  }

  private void loadAndAddCasePermission2Groups(List<Group> groups) throws SQLException {
    for (Group group : groups) {
      loadCasePermissionsForGroup(group);
    }
  }

  private void reinitializeNecessary() throws SQLException {
    if (groupOrPermissionDBchanged) {
      initializeData();
    }
  }

  public Collection<Group> loadGroups() throws SQLException {
    List<Group> groups = groupAdapter.selectAllGroups();
    loadAndAddPermissions2Groups(groups);
    groups.forEach(g -> name2group.put(g.getName(), g));
    return name2group.values();
  }

  public Collection<Group> getGroups() throws SQLException {
    reinitializeNecessary();
    return name2group.values();
  }

  private void loadAndAddCatalogPermissions2Groups(List<Group> groups) throws SQLException {
    for (Group group : groups) {
      loadCatalogPermissionsForGroup(group);
    }
  }

  private void loadCasePermissionsForGroup(Group group) throws SQLException {
    Set<String> blackList = loadCasePermissionList(group, BLACK_LIST);
    group.setCaseBlackList(blackList);
    Set<String> whiteList = loadCasePermissionList(group, WHITE_LIST);
    group.setCaseWhiteList(whiteList);
  }

  private void loadCatalogPermissionsForGroup(Group group) throws SQLException {
    Set<CatalogEntry> blackList = loadCatalogPermissionList(group, BLACK_LIST);
    group.setCatalogBlackList(blackList);
    Set<CatalogEntry> whiteList = loadCatalogPermissionList(group, WHITE_LIST);
    group.setCatalogWhiteList(whiteList);
  }

  private Set<String> loadCasePermissionList(Group group, String listType) throws SQLException {
    return groupCasePermissionAdapter.selectPermission(group.getId(), listType);
  }

  private Set<CatalogEntry> loadCatalogPermissionList(Group group, String listType) throws SQLException {
    return groupCatalogPermissionAdapter.selectPermission(group.getId(), listType);
  }

  private List<Group> loadGroupsWithoutPermissions(String username) throws SQLException {
    List<Integer> groupIDs = userInGroupAdaper.selectGroupsByUser(username);
    return groupAdapter.selectGroupsByID(groupIDs);
  }

  public List<Group> getGroups(List<String> groupNames) throws SQLException {
    return groupNames.stream().map(n -> getGroup(n)).filter(Objects::nonNull).collect(Collectors.toList());
  }

  public boolean insertGroupCasePermission(long groupID, long caseID, String listType) throws SQLException {
    groupOrPermissionDBchanged();
    return groupCasePermissionAdapter.insert(groupID, caseID, listType);
  }

  /**
   * @param groupId
   * @return usernames
   * @throws SQLException
   */
  public List<String> getMembersByGroupId(int groupId) throws SQLException {
    return userInGroupAdaper.selectUsersByGroupId(groupId);
  }

  public boolean userIsMemberInGroup(int groupId, String username) throws SQLException {
    return userInGroupAdaper.selectUserIsMemberInGroup(groupId, username);
  }

  public boolean userIsMemberInGroup(String groupName, String username) throws SQLException {
    Group group = getGroup(groupName);
    if (group != null) {
      return userIsMemberInGroup(group.getId(), username);
    }
    return false;
  }

  public List<String> getMembersByGroupName(String groupName) throws SQLException {
    Group group = getGroup(groupName);
    if (group != null) {
      return getMembersByGroupId(group.getId());
    }
    return new ArrayList<>();
  }

  public void updateUsersInGroup(int groupId, List<String> userList) throws SQLException {
    userInGroupAdaper.updateUsersInGroup(groupId, userList);
    userInGroupAdaper.commit();
  }

  public void deleteUserInGroupByUser(String username) throws SQLException {
    userInGroupAdaper.deleteUser(username);
    userInGroupAdaper.commit();
  }

  public void deleteUserFromSingleGroup(String username, String groupName) throws SQLException {
    Group result = getGroup(groupName);
    if (result == null) {
      throw new SQLException("Group does not exist.");
    }
    userInGroupAdaper.deleteUserFromSingleGroup(username, result.getId());
    userInGroupAdaper.commit();
  }

  public void deleteUser(String username) throws SQLException {
    userAdapter.deleteUser(username);
  }

  public String selectCatalogPermission(int groupId, String extid, String project) throws SQLException {
    return groupCatalogPermissionAdapter.selectCatalogPermission(groupId, extid, project);
  }

  @Override
  public void saveUserSettings(User user, UserSettings settings) throws SQLException {
    userSettingsAdapter.saveUserSettings(user, settings);
  }

  public UserSettings loadUserSettings(User user) throws SQLException {
    UserSettings settings = null;
    settings = userSettingsAdapter.getUserSettings(user);
    if (settings == null)
      settings = UserSettings.createDefaultUserSettings(user.isAdmin());
    return settings;
  }

  public void insertGroupCasePermission(String groupName, long caseID, String listType) throws SQLException {
    Group group = getGroup(groupName);
    insertGroupCasePermission(group.getId(), caseID, listType);
    groupOrPermissionDBchanged();
  }

  public void removeGroupCasePermission(String groupName, long caseID, String listType) throws SQLException {
    Group group = getGroup(groupName);
    groupCasePermissionAdapter.removeGroupCasePermission(group.getId(), caseID, listType);
    groupOrPermissionDBchanged();
  }

  public void removeGroupCasePermission(String groupName, long caseID) throws SQLException {
    Group group = getGroup(groupName);
    groupCasePermissionAdapter.removeGroupCasePermission(group.getId(), caseID);
    groupOrPermissionDBchanged();
  }

  public void removeAllGroupCasePermissions(String groupName) throws SQLException {
    Group group = getGroup(groupName);
    groupCasePermissionAdapter.removeAllGroupCasePermissions(group.getId());
    groupOrPermissionDBchanged();
  }

  public void addGroupCatalogPermission(String groupName, String extID, String project, String listType)
          throws SQLException {
    Group group = getGroup(groupName);
    groupCatalogPermissionAdapter.insertGroupCatalogPermission(group.getId(), extID, project, listType);
    groupOrPermissionDBchanged();

  }

  public void removeGroupCatalogPermission(String groupName, String extID, String project, String listType)
          throws SQLException {
    Group group = getGroup(groupName);
    groupCatalogPermissionAdapter.removeGroupCatalogPermission(group.getId(), extID, project, listType);
    groupOrPermissionDBchanged();
  }

  public void removeGroupCatalogPermission(String groupName, String extID, String project) throws SQLException {
    Group group = getGroup(groupName);
    groupCatalogPermissionAdapter.removeGroupCatalogPermission(group.getId(), extID, project);
    groupOrPermissionDBchanged();
  }

  public void removeAllGroupCatalogPermissions(String groupName) throws SQLException {
    Group group = getGroup(groupName);
    groupCatalogPermissionAdapter.removeAllGroupCatalogPermissions(group.getId());
    groupOrPermissionDBchanged();
  }

  public void deleteAllUsersAndGroups() throws SQLException {
    userAdapter.truncateTable();
    userAdapter.commit();
    groupAdapter.truncateTable();
    groupAdapter.commit();
    groupCasePermissionAdapter.truncateTable();
    groupCasePermissionAdapter.commit();
    groupCatalogPermissionAdapter.truncateTable();
    groupCatalogPermissionAdapter.commit();
    initializeData();
  }

  public void addListener(IAuthManagerListener listener) {
    listeners.add(listener);
  }

  public void removeListener(IAuthManagerListener listener) {
    listeners.remove(listener);
  }

}
