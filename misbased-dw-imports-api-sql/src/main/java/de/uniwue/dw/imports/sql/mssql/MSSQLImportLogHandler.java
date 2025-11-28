package de.uniwue.dw.imports.sql.mssql;

import java.sql.SQLException;

import de.uniwue.dw.imports.sql.SQLImportLogHandler;
import de.uniwue.misc.sql.SQLManager;

public class MSSQLImportLogHandler extends SQLImportLogHandler {

  public MSSQLImportLogHandler(SQLManager aSqlManager, String aTableName) throws SQLException {
    super(aSqlManager, aTableName);
  }

  public MSSQLImportLogHandler(SQLManager aSqlManager, String aTableName, boolean dropLogTable)
          throws SQLException {
    super(aSqlManager, aTableName, false);
  }

  public MSSQLImportLogHandler(SQLManager aSqlManager, String aTableName, String aPurpose,
          boolean dropLogTable) throws SQLException {
    super(aSqlManager, aTableName, aPurpose, dropLogTable);
  }

  @Override
  protected String getCreateTableString() {
    String command = "";

    command = getCreateTableStub();
    command += "ID BIGINT IDENTITY(0, 1) NOT NULL PRIMARY KEY, \n";
    command += "Time DATETIME2(0), \n";
    command += "message VARCHAR(MAX), \n";
    command += "purpose VARCHAR(100), \n";
    command += "errorType VARCHAR(100), \n";
    command += "project VARCHAR(100), \n";
    command += "level VARCHAR(100), \n";
    command += "filename VARCHAR(500), \n";
    command += "filetime DATETIME, \n";
    command += "fileline bigint \n" + ")";
    command += "CREATE INDEX " + getTableName() + "_Time ON "
            + getTableName() + " (Time) \n";
    command += "CREATE INDEX " + getTableName() + "_project_errorType ON "
            + getTableName() + " (project, errorType) \n";
    command += "CREATE INDEX " + getTableName() + "_Level ON "
            + getTableName() + " (level) \n";
    command += "CREATE INDEX " + getTableName() + "_ErrorType ON "
            + getTableName() + " (errorType) \n";
    return command;
  }

}
