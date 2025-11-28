package de.uniwue.dw.core.sql;

import de.uniwue.dw.core.client.authentication.group.IGroupCasePermissionAdapter;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public abstract class SQLGroupCasePermission extends DatabaseManager
        implements IDwSqlSchemaConstant, IGroupCasePermissionAdapter {

  private static final Logger logger = LogManager.getLogger(SQLGroupCasePermission.class);

  public static final String BLACK_LIST = "b";

  public static final String WHITE_LIST = "w";

  public SQLGroupCasePermission(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
    createSQLTables();
  }

  @Override
  public String getTableName() {
    return T_GROUP_CASE_PERMISSION;
  }

  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
    // ignore
  }

  @Override
  protected String getCreateTableString() {
    //@formatter:off
    String sql = "CREATE TABLE " + getTableName() + "( " 
//            + "id int " + SQLTypes.incrementFlagStartingWith0(sqlManager.config) + " PRIMARY KEY NOT NULL, "
            + "groupId int NOT NULL, " 
            + "caseID varchar(200) NOT NULL, "
            + "type char(1) DEFAULT '" + BLACK_LIST + "', "
            + "primary key (groupId, caseID) "
            + ");";
    //@formatter:on
    return sql;
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.core.client.authentication.group.IGroupCasePermission#selectPermission(int, java.lang.String)
   */
  @Override
  public Set<String> selectPermission(int groupId, String listType) throws SQLException {
    String sql = "select caseID from " + getTableName() + " where groupId=? and type=?";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setInt(1, groupId);
    stmt.setString(2, listType);
    ResultSet rs = stmt.executeQuery();
    Set<String> permissionList = new HashSet<String>();
    while (rs.next()) {
      String caseID = rs.getString("caseID");
      permissionList.add(caseID);
    }
    rs.close();
    stmt.close();
    return permissionList;
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.core.client.authentication.group.IGroupCasePermission#insert(long, long, java.lang.String)
   */
  @Override
  public abstract boolean insert(long groupID, long caseID, String listType) throws SQLException;

  @Override
  public void removeGroupCasePermission(int id, long caseID, String listType) throws SQLException {
    String sql = "DELETE FROM " + getTableName() + " WHERE groupID=? AND caseID = ? AND type like ?";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setInt(1, id);
    stmt.setLong(2, caseID);
    stmt.setString(3, listType);
    stmt.execute();
    stmt.close();
  }

  @Override
  public void removeAllGroupCasePermissions(int id) throws SQLException {
    String sql = "DELETE FROM " + getTableName() + " WHERE groupID=?";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setInt(1, id);
    stmt.execute();
    stmt.close();
  }

  @Override
  public void removeGroupCasePermission(int id, long caseID) throws SQLException {
    String sql = "DELETE FROM " + getTableName() + " WHERE groupID=? AND caseID = ?";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setInt(1, id);
    stmt.setLong(2, caseID);
    stmt.execute();
    stmt.close();
  }
}