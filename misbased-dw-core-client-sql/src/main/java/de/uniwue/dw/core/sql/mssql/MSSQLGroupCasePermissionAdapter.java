package de.uniwue.dw.core.sql.mssql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import de.uniwue.dw.core.sql.SQLGroupCasePermission;
import de.uniwue.misc.sql.SQLManager;

public class MSSQLGroupCasePermissionAdapter extends SQLGroupCasePermission {

  public MSSQLGroupCasePermissionAdapter(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
  }

  @Override
  public boolean insert(long groupID, long caseID, String listType) throws SQLException {
    // // MySQL Syntax is nicer: INSERT ON DUPLICATE KEY UPDATE
    // @formatter:off
      String sql = "MERGE INTO " + getTableName() + " AS target " 
              + "USING (select ? as groupId, ? as caseId, ? as  type) as source "
              + "on (target.groupId = source.groupId AND target.caseId=source.caseId) "
              + "WHEN MATCHED THEN UPDATE SET type = source.type "
              + "WHEN NOT MATCHED THEN INSERT (groupId, caseId, type) "
              + "VALUES (?, ?, ?);";
      // @formatter:on
    PreparedStatement stmt = this.sqlManager.createPreparedStatement(sql);
    stmt.setLong(1, groupID);
    stmt.setLong(2, caseID);
    stmt.setString(3, listType);
    stmt.setLong(4, groupID);
    stmt.setLong(5, caseID);
    stmt.setString(6, listType);
    boolean result = stmt.execute();
    stmt.close();
    return result;
  }

}
