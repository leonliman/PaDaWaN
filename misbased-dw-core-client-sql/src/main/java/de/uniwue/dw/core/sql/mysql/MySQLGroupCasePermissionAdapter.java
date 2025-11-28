package de.uniwue.dw.core.sql.mysql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import de.uniwue.dw.core.sql.SQLGroupCasePermission;
import de.uniwue.misc.sql.SQLManager;

public class MySQLGroupCasePermissionAdapter extends SQLGroupCasePermission {

  public MySQLGroupCasePermissionAdapter(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
    // TODO Auto-generated constructor stub
  }

  @Override
  public boolean insert(long groupID, long caseID, String listType) throws SQLException {
    String sql = "REPLACE INTO " + getTableName() + " SET groupId = ?, caseId = ?, type = ?;";
    // @formatter:on
    PreparedStatement stmt = this.sqlManager.createPreparedStatement(sql);
    stmt.setLong(1, groupID);
    stmt.setLong(2, caseID);
    stmt.setString(3, listType);
    boolean result = stmt.execute();
    stmt.close();
    return result;
    // @formatter:off
  }

}
