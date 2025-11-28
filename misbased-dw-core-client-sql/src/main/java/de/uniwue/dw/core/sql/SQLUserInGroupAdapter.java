package de.uniwue.dw.core.sql;

import de.uniwue.dw.core.client.authentication.group.IUserInGroupAdapter;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

public abstract class SQLUserInGroupAdapter extends DatabaseManager
        implements IDwSqlSchemaConstant, IUserInGroupAdapter {

  private static final Logger logger = LogManager.getLogger(SQLUserInGroupAdapter.class);

  public SQLUserInGroupAdapter(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
    createSQLTables();
  }

  @Override
  public String getTableName() {
    return T_USER_IN_GROUP;
  }

  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
    // ignore
  }

  @Override
  protected String getCreateTableString() {
    // @formatter:off
    String sql = 
            "CREATE TABLE " + getTableName() + "( " + 
            "id INT " + SQLTypes.incrementFlagStartingWith1(sqlManager.config) + " PRIMARY KEY NOT NULL, " + 
            "username VARCHAR(255) NOT NULL, " + 
            "groupId INT NOT NULL,"+
             SQLTypes.createUniqueConstraint(sqlManager.config, "username_groupid", "username","groupid")+ ");";
    // @formatter:on
    return sql;
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.core.client.authentication.group.IUserInGroupAdapter#selectGroupsByUser(java.lang.String)
   */
  @Override
  public List<Integer> selectGroupsByUser(String username) throws SQLException {
    String sql = "select groupId from " + getTableName() + " where username=?";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setString(1, username);
    ResultSet rs = stmt.executeQuery();
    List<Integer> groupIds = new LinkedList<>();
    while (rs.next()) {
      int groupId = rs.getInt("groupId");
      groupIds.add(groupId);
    }
    stmt.close();
    rs.close();
    return groupIds;
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.core.client.authentication.group.IUserInGroupAdapter#selectUsersByGroupId(int)
   */
  @Override
  public List<String> selectUsersByGroupId(int groupId) throws SQLException {
    String sql = "select username from " + getTableName() + " where groupId=?";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setInt(1, groupId);
    ResultSet rs = stmt.executeQuery();
    List<String> users = new LinkedList<>();
    while (rs.next()) {
      String username = rs.getString("username");
      users.add(username);
    }
    stmt.close();
    rs.close();
    return users;
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.core.client.authentication.group.IUserInGroupAdapter#selectUserIsMemberInGroup(int, java.lang.String)
   */
  @Override
  public boolean selectUserIsMemberInGroup(int groupId, String username) throws SQLException {
    String sql = "select username from " + getTableName() + " where groupId=? and username=?";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setInt(1, groupId);
    stmt.setString(2, username);
    ResultSet rs = stmt.executeQuery();
    boolean isMember = false;
    if (rs.next()) {
      isMember = true;
    }
    stmt.close();
    rs.close();
    return isMember;
  }

  @Override
  public abstract void insertUser2Group(int groupID, String userName) throws SQLException;

  /* (non-Javadoc)
   * @see de.uniwue.dw.core.client.authentication.group.IUserInGroupAdapter#updateUsersInGroup(int, java.util.List)
   */
  @Override
  public void updateUsersInGroup(int groupId, List<String> userList) throws SQLException {
    deleteUsersInGroup(groupId);
    for (String user : userList) {
      insertUser2Group(groupId, user);
    }
  }

  private void deleteUsersInGroup(int groupId) throws SQLException {
    String sql = "delete from " + getTableName() + " where groupId=?";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setInt(1, groupId);
    stmt.executeUpdate();
    stmt.close();
    logger.info("deleted group by id " + groupId);
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.core.client.authentication.group.IUserInGroupAdapter#removeRowByGroup(int)
   */
  @Override
  public void removeRowByGroup(int groupId) throws SQLException {
    String sql = "delete from " + getTableName() + " where groupId=?";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setInt(1, groupId);
    stmt.executeUpdate();
    stmt.close();
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.core.client.authentication.group.IUserInGroupAdapter#deleteUser(java.lang.String)
   */
  @Override
  public void deleteUser(String username) throws SQLException {
    String sql = "delete from " + getTableName() + " where username=?";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setString(1, username);
    stmt.executeUpdate();
    stmt.close();
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.core.client.authentication.group.IUserInGroupAdapter#deleteUserFromSingleGroup(java.lang.String, int)
   */
  @Override
  public void deleteUserFromSingleGroup(String username, int groupId) throws SQLException {
    String sql = "delete from " + getTableName() + " where username=? and groupId=?";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setString(1, username);
    stmt.setInt(2, groupId);
    stmt.executeUpdate();
    stmt.close();
  }
}
