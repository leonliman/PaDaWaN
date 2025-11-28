package de.uniwue.misc.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLParamsAdapter extends DatabaseManager implements IParamsAdapter {

  private String tableName;

  public SQLParamsAdapter(SQLManager aSqlManager, String aTableName) throws SQLException {
    super(aSqlManager);
    tableName = aTableName;
    createSQLTables();
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
  }

  @Override
  public void lock() throws SQLException {
    lockTable();
  }

  @Override
  // @formatter:off
  protected String getCreateTableString() {
    String sql = getCreateTableStub()
            + "id INT " + SQLTypes.incrementFlagStartingWith1(sqlManager.config) + " PRIMARY KEY NOT NULL,"
            + "param VARCHAR(255) NULL, \n"
            + "value VARCHAR(255) NULL \n"
            + ")";
    return sql;
  } // @formatter:on

  @Override
  public void deleteParam(String paramName) throws SQLException {
    Statement stmt = getSQLManager().createStatement();
    String sql = "DELETE FROM " + getTableName() + " WHERE param='" + paramName + "'";
    stmt.execute(sql);
    stmt.close();
    commit();
  }

  @Override
  public String getParam(String paramName) throws SQLException {
    return getParam(paramName, null);
  }

  @Override
  public String getParam(String paramName, String defaultValue) throws SQLException {
    String sql = "SELECT value FROM " + getTableName() + " WHERE param=?";
    PreparedStatement stmt = getSQLManager().createPreparedStatement(sql);
    int paramIndex = 1;
    stmt.setString(paramIndex++, paramName);
    ResultSet rs = stmt.executeQuery();
    String result = defaultValue;
    while (rs.next()) {
      result = rs.getString(1);
    }
    rs.close();
    stmt.close();
    return result;
  }

  @Override
  public void setParam(String paramName, String value) throws SQLException {
    if ((value == null) || value.isEmpty()) {
      return;
    }
    deleteParam(paramName);
    Statement stmt = getSQLManager().createStatement();
    String sql = "INSERT INTO " + getTableName() + " (param, value) VALUES ('" + paramName + "','"
            + value + "')";
    stmt.execute(sql);
    stmt.close();
    commit();
  }

}
