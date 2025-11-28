package de.uniwue.dw.core.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.group.AuthManager;
import de.uniwue.dw.core.client.authentication.group.IGroupCatalogPermissionAdapter;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;

public class SQLGroupCatalogPermission extends DatabaseManager
        implements IDwSqlSchemaConstant, IGroupCatalogPermissionAdapter {

  private static final Logger logger = LogManager.getLogger(SQLGroupCatalogPermission.class);

  public SQLGroupCatalogPermission(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
    createSQLTables();
  }

  @Override
  public String getTableName() {
    return T_GROUP_CATALOG_PERMISSION;
  }

  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
    // ignore
  }

  @Override
  protected String getCreateTableString() {
    // @formatter:off
    String sql = "CREATE TABLE " + getTableName() + "( " + "id int "
            + SQLTypes.incrementFlagStartingWith1(sqlManager.config) + " PRIMARY KEY NOT NULL, "
            + "groupId int NOT NULL, " + "extID varchar(200) NOT NULL, "
            + "project varchar(200) NOT NULL, " + "type char(1) DEFAULT '" + AuthManager.BLACK_LIST + "' "
            + ");";
    // @formatter:on
    return sql;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * de.uniwue.dw.core.client.authentication.group.IGroupCatalogPermission#selectPermission(int,
   * java.lang.String)
   */
  public Set<CatalogEntry> selectPermission(int groupId, String listType) throws SQLException {
    String sql = "select extID, project from " + getTableName() + " where groupId=? and type=?";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setInt(1, groupId);
    stmt.setString(2, listType);
    ResultSet rs = stmt.executeQuery();
    Set<CatalogEntry> permissionList = new HashSet<CatalogEntry>();
    CatalogManager catalogManager = DwClientConfiguration.getInstance().getCatalogManager();
    while (rs.next()) {
      String extId = rs.getString("extID");
      String project = rs.getString("project");
      try {
        CatalogEntry entry = catalogManager.getEntryByRefID(extId, project);
        permissionList.add(entry);
      } catch (SQLException e) {
        logger.error("Catalogentry (" + extId + ", " + project
                + ") was not found and could not be added to group list (id:" + groupId + ", type:"
                + listType + ")");
      }
    }
    rs.close();
    stmt.close();
    return permissionList;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * de.uniwue.dw.core.client.authentication.group.IGroupCatalogPermission#selectCatalogPermission(
   * int, java.lang.String, java.lang.String)
   */
  public String selectCatalogPermission(int groupId, String extid, String project)
          throws SQLException {
    String sql = "select type from " + getTableName()
            + " where groupId=? and extID=? AND project=?";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setInt(1, groupId);
    stmt.setString(2, extid);
    stmt.setString(3, project);
    ResultSet rs = stmt.executeQuery();
    String type = null;
    if (rs.next()) {
      type = rs.getString("type");
    }
    rs.close();
    stmt.close();
    return type;
  }

  @Override
  public void insertGroupCatalogPermission(int groupId, String extID, String project,
          String listType) throws SQLException {
    String sql = " INSERT INTO " + getTableName()
            + " (groupId, extID, project, type) values (?,?,?,?)";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setInt(1, groupId);
    stmt.setString(2, extID);
    stmt.setString(3, project);
    stmt.setString(4, listType);
    stmt.execute();
    stmt.close();
  }

	@Override
	public void removeGroupCatalogPermission(int id, String extID, String project, String listType) throws SQLException {
		String sql = "DELETE FROM "+getTableName() + " WHERE groupID=? AND extID like ? AND project like ? AND type like ?";
		PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
		stmt.setInt(1, id);
		stmt.setString(2, extID);
		stmt.setString(3, project);
		stmt.setString(4, listType);
		stmt.execute();
		stmt.close();
	}

	@Override
	public void removeAllGroupCatalogPermissions(int id) throws SQLException {
		String sql = "DELETE FROM "+getTableName() + " WHERE groupID=?";
		PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
		stmt.setInt(1, id);
		stmt.execute();
		stmt.close();
	}

	@Override
	public void removeGroupCatalogPermission(int id, String extID, String project) throws SQLException {
		String sql = "DELETE FROM "+getTableName() + " WHERE groupID=? AND extID like ? AND project like ?";
		PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
		stmt.setInt(1, id);
		stmt.setString(2, extID);
		stmt.setString(3, project);
		stmt.execute();
		stmt.close();
	}

}
