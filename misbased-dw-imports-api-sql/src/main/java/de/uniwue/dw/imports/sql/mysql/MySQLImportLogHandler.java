package de.uniwue.dw.imports.sql.mysql;

import java.sql.SQLException;

import de.uniwue.dw.imports.sql.SQLImportLogHandler;
import de.uniwue.misc.sql.SQLManager;

public class MySQLImportLogHandler extends SQLImportLogHandler {

  public MySQLImportLogHandler(SQLManager aSqlManager, String aTableName) throws SQLException {
    super(aSqlManager, aTableName);
  }

  public MySQLImportLogHandler(SQLManager aSqlManager, String aTableName, boolean dropLogTable)
          throws SQLException {
    super(aSqlManager, aTableName, false);
  }

  public MySQLImportLogHandler(SQLManager aSqlManager, String aTableName, String aPurpose,
          boolean dropLogTable) throws SQLException {
    super(aSqlManager, aTableName, aPurpose, dropLogTable);
  }

  @Override
  protected String getCreateTableString() {
    String command = "";

    command = getCreateTableStub();
    command += "ID BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY, \n";
    command += "Time DATETIME, \n";
    command += "message MEDIUMTEXT, \n";
    command += "purpose VARCHAR(100), \n";
    command += "errorType VARCHAR(100), \n";
    command += "project VARCHAR(100), \n";
    command += "level VARCHAR(100), \n";
    command += "filename VARCHAR(100), \n";
    command += "filetime DATETIME, \n";
    command += "fileline bigint, \n";
    command += "INDEX " + getTableName() + "_time (time), \n";
    command += "INDEX " + getTableName() + "_project (project), \n";
    command += "INDEX " + getTableName() + "_level (level), \n";
    command += "INDEX " + getTableName() + "_errorType (errorType)\n" + ")";
    return command;
  }

}
