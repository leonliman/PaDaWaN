package de.uniwue.dw.query.model.sql;

import de.uniwue.dw.query.model.data.IndexLogEntry;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.dw.query.model.manager.adapter.IIndexLogAdapter;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;
import de.uniwue.misc.util.TimeUtil;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class SQLIndexLogAdapter extends DatabaseManager implements IIndexLogAdapter {

  private String tableName;

  public SQLIndexLogAdapter(SQLManager aSqlManager, String aTableName) throws SQLException {
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
  // @formatter:off
  protected String getCreateTableString() {
    String sql = "CREATE TABLE " + getTableName() 
            + "(id BIGINT " + SQLTypes.incrementFlagStartingWith1(sqlManager.config) + " PRIMARY KEY NOT NULL, \n"
            + "indexID VARCHAR(250) NULL, \n"
            + "occTime DATETIME NOT NULL DEFAULT " + SQLTypes.getCurrentTimestamp(sqlManager.config) + ", \n"
            + "pid BIGINT NULL, \n"
            + "caseid BIGINT NULL, \n"
            + "message " + SQLTypes.bigTextType(sqlManager.config) + " NULL \n"
            + ")";
    return sql;
  } // @formatter:on

  /* (non-Javadoc)
   * @see de.uniwue.dw.query.model.sql.IIndexLogAdapter#error(java.lang.String)
   */
  @Override
  public void error(String message, String serverID) {
    insert(message, 0, 0, serverID);
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.query.model.sql.IIndexLogAdapter#error(de.uniwue.dw.query.model.index.IndexException)
   */
  @Override
  public void error(IndexException e, String serverID) {
    insert(e.getMessage(), e.pid, e.caseID, serverID);
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.query.model.sql.IIndexLogAdapter#error(java.lang.String, de.uniwue.dw.query.model.index.IndexException)
   */
  @Override
  public void error(String message, IndexException e, String serverID) {
    insert(message + "\n" + e.getMessage(), e.pid, e.caseID, serverID);
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.query.model.sql.IIndexLogAdapter#insert(java.lang.String)
   */
  @Override
  public void insert(String message, String serverID) {
    insert(message, 0, 0, serverID);
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.query.model.sql.IIndexLogAdapter#insert(java.lang.String, long, long)
   */
  @Override
  public void insert(String message, long pid, long caseID, String serverID) {
    if ((pid != 0) || (caseID != 0)) {
      System.out.println("IndexLog: " + message + "; pid: " + pid + "; caseID: " + caseID
              + " + Current Time: " + TimeUtil.currentTime());
    } else {
      System.out.println("IndexLog: " + message + "; Current Time: " + TimeUtil.currentTime());
    }
    try {
      String sql = "INSERT INTO " + getTableName() + " (pid, caseid, message, indexid) values (?, ?, ?, ?)";
      PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
      int paramOffset = 1;
      stmt.setLong(paramOffset++, pid);
      stmt.setLong(paramOffset++, caseID);
      stmt.setString(paramOffset++, message);
      stmt.setString(paramOffset++, serverID);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public List<IndexLogEntry> getLogEntriesByServerIDSinceTime(String serverID, Timestamp time) {
    try {
      String sql = "SELECT id, occTime, pid, caseid, message FROM " + getTableName() + " WHERE indexID ";
      if (serverID == null) {
        sql += "IS NULL";
      } else {
        sql += "= ?";
      }
      if (time != null) {
        sql += " AND occTime >= ?";
      }
      PreparedStatement statement = sqlManager.createPreparedStatement(sql);
      int paramOffset = 1;
      if (serverID != null) {
        statement.setString(paramOffset++, serverID);
      }
      if (time != null) {
        statement.setTimestamp(paramOffset, time);
      }
      ResultSet result = statement.executeQuery();
      List<IndexLogEntry> resultList = new ArrayList<>();
      while (result.next()) {
        String message = result.getString("message");
        long id = result.getLong("id");
        long pid = result.getLong("pid");
        long caseID = result.getLong("caseid");
        Timestamp occTime = result.getTimestamp("occTime");
        IndexLogEntry indexLogEntry = new IndexLogEntry(serverID, message, id, pid, caseID, occTime);
        resultList.add(indexLogEntry);
      }
      statement.close();
      return resultList;
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }
  }
}
