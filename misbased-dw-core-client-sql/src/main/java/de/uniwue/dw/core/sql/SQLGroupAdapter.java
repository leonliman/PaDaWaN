package de.uniwue.dw.core.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.core.client.authentication.group.Group;
import de.uniwue.dw.core.client.authentication.group.IGroupAdapter;
import de.uniwue.dw.core.model.hooks.IDwCatalogHooks;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;

public abstract class SQLGroupAdapter extends DatabaseManager implements IDwSqlSchemaConstant, IGroupAdapter {

  private static final Logger logger = LogManager.getLogger(SQLGroupAdapter.class);
  
  public SQLGroupAdapter(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
    createSQLTables();
  }

  @Override
  public String getTableName() {
    return T_GROUP;
  }

  @Override
  protected void readResult(ResultSet rs) throws SQLException {
    // ignore

    // while (rs.next()) {
    // String name = rs.getString("name");
    // int kAnonymity = rs.getInt("k_anonymity");
    // short caseQueryShort = rs.getShort("case_query");
    // boolean caseQuery = caseQueryShort == 1;
    // Group group = new Group(name, kAnonymity, caseQuery);
    // manger.add(group);
    // }
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.core.client.authentication.group.IGroupAdapter#selectAllGroups()
   */
  public List<Group> selectAllGroups() throws SQLException {
    String sql = "select * from " + getTableName();
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    List<Group> groups = executeStatement(stmt);
    stmt.close();
    return groups;
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.core.client.authentication.group.IGroupAdapter#deleteGroupById(int)
   */
  public void deleteGroupById(int groupId) throws SQLException {
    String sql = "delete from " + getTableName() + " where id=?";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setInt(1, groupId);
    stmt.executeUpdate();
    stmt.close();
    logger.info("Deleted group by id " + groupId);
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.core.client.authentication.group.IGroupAdapter#updateGroupById(int, java.lang.String, int, java.lang.Boolean, java.lang.Boolean)
   */
  public void updateGroupById(int groupId, String name, int kAnonymity, Boolean case_query,
          Boolean admin) throws SQLException {
    String sql = "update " + getTableName()
            + " set name=?, kAnonymity=?, case_query=?, admin=? where id=?";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setString(1, name);
    stmt.setInt(2, kAnonymity);
    stmt.setBoolean(3, case_query);
    stmt.setBoolean(4, admin);
    stmt.setInt(5, groupId);
    stmt.executeUpdate();
    stmt.close();
    logger.info("Updated group by id " + groupId);
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.core.client.authentication.group.IGroupAdapter#selectGroupsByID(java.util.List)
   */
  public List<Group> selectGroupsByID(List<Integer> groupIDs) throws SQLException {
    if (groupIDs.isEmpty())
      return new ArrayList<>();
    String idsAsList = "(" + groupIDs.stream().map(String::valueOf).collect(Collectors.joining(","))
            + ")";
    String sql = "select * from " + getTableName() + " where id  in " + idsAsList;
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    List<Group> groups = executeStatement(stmt);
    stmt.close();
    return groups;
  }

  public abstract Group insertGroup(String name, int kAnonymity, boolean caseQuery, boolean admin)
          throws SQLException;

  private List<Group> executeStatement(PreparedStatement stmt) throws SQLException {
    ResultSet rs = stmt.executeQuery();
    List<Group> groups = new ArrayList<>();
    while (rs.next()) {
      int id = rs.getInt("id");
      String name = rs.getString("name");
      int kAnonymity = rs.getInt("kAnonymity");
      short caseQueryShort = rs.getShort("case_query");
      boolean caseQuery = caseQueryShort == 1;
      short adminShort = rs.getShort("admin");
      boolean admin = adminShort == 1;
      Group group = new Group(id, name, kAnonymity, caseQuery, admin);
      groups.add(group);
    }
    rs.close();
    return groups;
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.core.client.authentication.group.IGroupAdapter#selectGroupsByName(java.lang.String)
   */
  public Group selectGroupsByName(String groupName) throws SQLException {
    String sql = "select * from " + getTableName() + " where name=?";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setString(1, groupName);
    List<Group> groups = executeStatement(stmt);
    stmt.close();
    if (groups.isEmpty()) {
      return null;
    } else {
      return groups.get(0);
    }
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.core.client.authentication.group.IGroupAdapter#integrateAllGroupsToCatalog()
   */
  public void integrateAllGroupsToCatalog() throws SQLException {
    List<Group> groups = selectAllGroups();
    for (Group group : groups) {
      if (!groupExtisAsCatalogEntry(group)) {
        createCatalogEntryForGroup(group);
      }
    }
  }

  private void createCatalogEntryForGroup(Group group) {
    // TODO Auto-generated method stub

  }

  private boolean groupExtisAsCatalogEntry(Group group) throws SQLException {
    String sql = "SELECT * from " + T_CATALOG + " WHERE extId=? AND project='"
            + IDwCatalogHooks.PROJECT_HOOK_GROUP_ID + "'";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setString(1, group.getName());
    ResultSet rs = stmt.executeQuery();
    if (rs.next()) {

    }
    return false;
  }

}
