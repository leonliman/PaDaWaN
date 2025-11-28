package de.uniwue.dw.core.model.manager;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.CountType;
import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.client.authentication.group.AuthManager;
import de.uniwue.dw.core.client.authentication.group.Group;
import de.uniwue.dw.core.client.authentication.group.IAuthManagerListener;
import de.uniwue.dw.core.model.data.CatalogEntry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class CompleteCatalogClientManager extends AbstractCatalogClientManager implements IAuthManagerListener {

  private static Logger logger = LogManager.getLogger(CompleteCatalogClientManager.class);

  private CatalogManager catalogManager;

  private AuthManager groupManager;

  private HashMap<CatalogEntry, Set<Group>> entries2entitledGroups = new HashMap<>();

  public CompleteCatalogClientManager() throws SQLException {
    this(DwClientConfiguration.getInstance().getCatalogManager());
  }

  public CompleteCatalogClientManager(CatalogManager catalogManager) throws SQLException {
    this(catalogManager, DwClientConfiguration.getInstance().getAuthManager());
  }

  public CompleteCatalogClientManager(CatalogManager catalogManager, AuthManager groupManager) throws SQLException {
    this.catalogManager = catalogManager;
    this.groupManager = groupManager;
    if (groupManager != null) {
      buildCatalogEntry2entitledGroupsMap();
      this.groupManager.addListener(this);
    }
  }

  private void buildCatalogEntry2entitledGroupsMap() throws SQLException {
    logger.debug(this);
    entries2entitledGroups = new HashMap<>();
    Collection<Group> groups = groupManager.loadGroups();
    for (Group group : groups) {
      if (group.catalogWhiteListIsActive()) {
        for (CatalogEntry entry : group.getCatalogWhiteList()) {
          Set<CatalogEntry> entitledEntries = new HashSet<>();
          entitledEntries.addAll(entry.getAncestors());
          entitledEntries.addAll(entry.getAllChildren());
          entitledEntries.add(entry);
          entitleEntriesForGroup(entitledEntries, group, entries2entitledGroups);
        }
      } else {
        Set<CatalogEntry> entitledEntries = new HashSet<>(catalogManager.getEntries());
        for (CatalogEntry entry : group.getCatalogBlackList()) {
          entitledEntries.removeAll(entry.getAllChildren());
          entitledEntries.remove(entry);
        }
        entitleEntriesForGroup(entitledEntries, group, entries2entitledGroups);
      }
    }
  }

  private static void entitleEntriesForGroup(Set<CatalogEntry> entitledEntries, Group group,
          HashMap<CatalogEntry, Set<Group>> entries2entitledGroups) {
    for (CatalogEntry entry : entitledEntries) {
      Set<Group> entitledGroups = entries2entitledGroups.get(entry);
      if (entitledGroups == null) {
        entitledGroups = new HashSet<>();
        entries2entitledGroups.put(entry, entitledGroups);
      }
      entitledGroups.add(group);
    }
  }

  @Override
  public CatalogEntry getEntryByRefID(String extID, String project, User user) throws DataSourceException {
    return getEntryByRefID(extID, project, user, true);
  }

  @Override
  public CatalogEntry getEntryByRefID(String extID, String project, User user, boolean throwExceptionIfNotExists)
          throws DataSourceException {
    try {
      CatalogEntry entry = catalogManager.getEntryByRefID(extID, project, throwExceptionIfNotExists);
      if (entry == null || user == null)
        return entry;
      if (userIsEntitledForCatalogEntry(user, entry))
        return entry;
      else
        throw new DataSourceException("User (" + user + ") not entitled for catalog entry (" + entry + ").");
    } catch (SQLException e) {
      throw new DataSourceException(e);
    }
  }

  private static List<CatalogEntry> truncate(List<CatalogEntry> list, int limit) {
    int newSize = Math.min(limit, list.size() - 1);
    return list.subList(0, newSize);
  }

  @Override
  public CatalogEntry getRoot() {
    return catalogManager.getRoot();
  }

  public List<CatalogEntry> getEntries(User user) {
    return applyFiltersOnList(catalogManager.getEntries(), getUserPriviligesFilter(user));
  }

  public List<CatalogEntry> getEntries(User user, CountType countType, int minOccurrence) {
    return applyFiltersOnList(catalogManager.getEntries(), getUserPriviligesFilter(user),
            filterByOccurrence(countType, minOccurrence));
  }

  @Override
  public CatalogEntry getAllAncestorsAndSiblings(CatalogEntry entry, User user, CountType countType,
          int minOccurrence) {
    HashMap<CatalogEntry, List<CatalogEntry>> partent2childs = new HashMap<>();
    CatalogEntry parent = entry.getParent();
    while (parent != null) {
      List<CatalogEntry> sons = getChildsOf(parent, user, countType, minOccurrence);
      partent2childs.put(parent, sons);
      parent = parent.getParent();
    }
    CatalogEntry filteredTree = catalogManager.getRoot().copyWihtoutReferences();
    buildTree(filteredTree, partent2childs);
    return filteredTree;
  }

  @Override
  public List<CatalogEntry> getChildsOf(CatalogEntry parent, User user) {
    return applyFiltersOnList(parent.getChildren(), getUserPriviligesFilter(user));
  }

  @Override
  public List<CatalogEntry> getChildsOf(CatalogEntry parent, User user, CountType countType, int minOccurrence) {
    return applyFiltersOnList(parent.getChildren(), getUserPriviligesFilter(user),
            filterByOccurrence(countType, minOccurrence));
  }

  @Override
  public List<CatalogEntry> getEntriesByWordFilter(String word, User user, int limit) {
    List<CatalogEntry> filtered = applyFiltersOnList(catalogManager.getEntries(),
            CatalogManager.createFilterPredicate(word), getUserPriviligesFilter(user));
    return truncate(filtered, limit);
  }

  @Override
  public List<CatalogEntry> getEntriesByWordFilter(String word, User user, CountType countType, int minOccurrence,
          int limit) {
    List<CatalogEntry> filtered = applyFiltersOnList(catalogManager.getEntries(),
            CatalogManager.createFilterPredicate(word), getUserPriviligesFilter(user),
            filterByOccurrence(countType, minOccurrence));
    return truncate(filtered, limit);
  }

  private static Predicate<CatalogEntry> filterByOccurrence(CountType countType, int minOccurence) {
    switch (countType) {
      case absolute:
        return n -> n.getCountAbsolute() >= minOccurence;
      case distinctPID:
        return n -> n.getCountDistinctPID() >= minOccurence;
      case distinctCaseID:
        return n -> n.getCountDistinctCaseID() >= minOccurence;
      default:
        return n -> n.getCountDistinctCaseID() >= minOccurence;
    }
  }

  /**
   * Retrieves a catalog tree that contains all nodes that match the given search filter and that
   * are visible for the user.
   *
   * @param searchPhrase search filter. can contain a single word or whitespace separated words.
   * @param user
   * @return
   */
  @Override
  public CatalogEntry getTreeByWordFilter(String searchPhrase, User user) {
    Predicate<CatalogEntry> filter = and(CatalogManager.createFilterPredicate(searchPhrase),
            getUserPriviligesFilter(user));
    return getTreeByFilters(filter);
    // TODO Sibling nodes, that don't contain a matching child, will get a indicating dummy child
    // node.
  }

  @Override
  public CatalogEntry getTreeByWordFilter(String searchPhrase, User user, CountType countType, int minOccurrence) {
    Predicate<CatalogEntry> filters = and(CatalogManager.createFilterPredicate(searchPhrase),
            getUserPriviligesFilter(user), filterByOccurrence(countType, minOccurrence));
    return getTreeByFilters(filters);
  }

  private CatalogEntry getTreeByFilters(Predicate<CatalogEntry> filters) {
    List<CatalogEntry> hits = applyFiltersOnList(catalogManager.getEntries(), filters);
    return buildTreeForHits(hits);
  }

  public CatalogEntry buildTreeForHits(List<CatalogEntry> hits) {
    HashMap<CatalogEntry, List<CatalogEntry>> partent2childs = new HashMap<>();
    hits.forEach(c -> addAllAncestorsToHM(c, partent2childs));
    CatalogEntry filteredTree = catalogManager.getRoot().copyWihtoutReferences();
    buildTree(filteredTree, partent2childs);
    return filteredTree;
  }

  private static void buildTree(CatalogEntry parent, HashMap<CatalogEntry, List<CatalogEntry>> partent2childs) {
    List<CatalogEntry> originalTreechilds = partent2childs.get(parent);
    if (originalTreechilds != null) {
      List<CatalogEntry> newTreeChilds = originalTreechilds.stream().map(n -> n.copyWihtoutReferences())
              .collect(Collectors.toList());
      newTreeChilds.forEach(n -> n.setParent(parent));
      parent.setChildren(newTreeChilds);
      newTreeChilds.forEach(n -> buildTree(n, partent2childs));
    } else {
      parent.setChildren(new ArrayList<>());
    }
  }

  private static void addAllAncestorsToHM(CatalogEntry entry,
          HashMap<CatalogEntry, List<CatalogEntry>> partent2childs) {
    CatalogEntry parent = entry.getParent();
    if (parent != null) {
      List<CatalogEntry> filteredChilds = Optional.ofNullable(partent2childs.get(parent)).orElse(new ArrayList<>());
      if (!filteredChilds.contains(entry))
        filteredChilds.add(entry);
      partent2childs.put(parent, filteredChilds);
      addAllAncestorsToHM(parent, partent2childs);
    }
  }

  private void addAllSiblingsToHM(HashMap<CatalogEntry, List<CatalogEntry>> partent2childs, User user,
          CountType countType, int minOccurrence) {
    for (Entry<CatalogEntry, List<CatalogEntry>> entry : partent2childs.entrySet()) {
      CatalogEntry parent = entry.getKey();
      final List<CatalogEntry> childs = Optional.ofNullable(partent2childs.get(parent)).orElse(new ArrayList<>());
      List<CatalogEntry> siblings = getChildsOf(parent, user, countType, minOccurrence);
      siblings.stream().filter(n -> !childs.contains(n)).forEach(n -> childs.add(n));
    }
  }

  @SafeVarargs
  private static List<CatalogEntry> applyFiltersOnList(Collection<CatalogEntry> entries,
          Predicate<CatalogEntry>... filters) {
    Predicate<CatalogEntry> predicate = and(filters);
    return entries.parallelStream().filter(predicate).collect(Collectors.toList());
  }

  // @SafeVarargs
  // private static <T> Predicate<T> and(Predicate<T>... filters) {
  // Predicate<T> result = null;
  // for (Predicate<T> filter : filters) {
  // if (result == null)
  // result = filter;
  // else
  // result.and(filter);
  // }
  // return result;
  // }

  @SafeVarargs
  private static Predicate<CatalogEntry> and(Predicate<CatalogEntry>... filters) {
    return Arrays.stream(filters).reduce(Predicate::and).orElse(x -> true);
    // If this method does not compile: (a) use java 1.8 (b) clean your workspace (c) use the method
    // above.
  }

  @Override
  public void dispose() {
    if (catalogManager != null) {
      catalogManager.dispose();
    }
    catalogManager = null;
    if (this.groupManager != null) {
      this.groupManager.removeListener(this);
    }
    super.dispose();
  }

  @Override
  public CatalogEntry getEntryByID(int attrId, User user) throws DataSourceException {
    CatalogEntry entry;
    try {
      entry = catalogManager.getEntryByID(attrId);
    } catch (SQLException e) {
      throw new DataSourceException(e);
    }
    if (entry == null) {
      return null;
    }
    if (userIsEntitledForCatalogEntry(user, entry))
      return entry;
    else
      throw new UnauthorizedException();
  }

  private Predicate<CatalogEntry> getUserPriviligesFilter(User user) {
    // return UserPrivileges.entitledForCatalogEntry(user);
    return entitledForCatalogEntry(user);
  }

  private Predicate<CatalogEntry> entitledForCatalogEntry(User user) {
    return entry -> {
      return userIsEntitledForCatalogEntry(user, entry);
    };
  }

  private boolean userIsEntitledForCatalogEntry(User user, CatalogEntry entry) {
    boolean isEntitled = user.getGroups().stream().anyMatch(g -> groupIsEntitledForCatalogEntry(g, entry));
    return isEntitled;
  }

  private boolean groupIsEntitledForCatalogEntry(Group group, CatalogEntry entry) {
    Set<Group> entitledGroups = entries2entitledGroups.get(entry);
    if (entitledGroups == null)
      return false;
    else
      return entitledGroups.contains(group);
  }

  public Optional<Set<Group>> getEntitledGroupsForCatalogEntry(CatalogEntry entry) {
    if (entry == null)
      return Optional.empty();
    Set<Group> groups = entries2entitledGroups.get(entry);
    if (groups == null || groups.isEmpty())
      logger.warn("Entry is not visible for any group: " + entry.getName() + " " + entry.getExtID() + " "
              + entry.getProject());
    return Optional.ofNullable(groups);
  }

  @Override
  public void groupOrPermissionDBchanged() {
    try {
      buildCatalogEntry2entitledGroupsMap();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void addGroupPerm(String extId, String project, String groupName, String type) throws SQLException {
    groupManager.addGroupCatalogPermission(groupName, extId, project, type);
  }

  public void deleteAllGroupPermsForEntry(String extId, String project) throws SQLException {
    List<Group> allGroups = new ArrayList<>(groupManager.getGroups());
    for (Group group : allGroups) {
      groupManager.removeGroupCatalogPermission(group.getName(), extId, project);
    }
  }

  @Override
  public void reinitialize() throws DataSourceException {
    try {
      buildCatalogEntry2entitledGroupsMap();
    } catch (SQLException e) {
      throw new DataSourceException(e);
    }
  }

}
