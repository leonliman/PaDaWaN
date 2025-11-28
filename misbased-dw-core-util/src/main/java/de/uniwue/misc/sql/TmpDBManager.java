package de.uniwue.misc.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class TmpDBManager extends DatabaseManager {

  public TmpDBManager(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
  }

  public abstract String getTmpTableName();

  protected abstract String getCreateTmpTableString();

  @Override
  public String getTableName() {
    String tableName = getTmpTableName();
    if (sqlManager.getDBType() == DBType.MSSQL) {
      tableName = "#" + tableName;
    }
    return tableName;
  }

  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
  }

  @Override
  protected String getCreateTableString() {
    String command = getCreateTmpTableString();
    Pattern createTablePattern = Pattern.compile("CREATE TABLE", Pattern.CASE_INSENSITIVE);
    Matcher matcher = createTablePattern.matcher(command);
    String tmpCommand = matcher.replaceFirst("CREATE TEMPORARY TABLE");
    return tmpCommand;
  }

}
