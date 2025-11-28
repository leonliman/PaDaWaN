package de.uniwue.dw.query.model.sql;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.sql.IDwSqlSchemaConstant;
import de.uniwue.dw.query.model.data.LoggedQuery;
import de.uniwue.dw.query.model.manager.adapter.IQueryLogAdapter;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;

public class SQLQueryLogAdapter extends DatabaseManager
        implements IDwSqlSchemaConstant, IQueryLogAdapter {

  public SQLQueryLogAdapter(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
    createSQLTables();
  }

  @Override
  public String getTableName() {
    return T_QUERY_LOG;
  }

  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
  }

  @Override
  // @formatter:off
  protected String getCreateTableString() {
    String command = getCreateTableStub();
    command += "LogID INT " + SQLTypes.incrementFlagStartingWith1(sqlManager.config) + " NOT NULL PRIMARY KEY, \n" 
            + "XML " + SQLTypes.bigTextType(sqlManager.config) + ", \n" 
            + "machineName VARCHAR(500), \n"
            + "userName VARCHAR(500), \n" 
            + "queryTime " + SQLTypes.timestampType(sqlManager.config) + ", \n"
            + "queryID BIGINT, \n"
            + "resultCount BIGINT, \n"
            + "duration BIGINT, \n"
            + "exportType VARCHAR(100), \n"
            + "engineType VARCHAR(100), \n"
            + "engineVersion VARCHAR(100)"
            + ")";
    return command;
  } // @formatter:on

  public void insert(String queryXML, String user, int queryID, String exportType,
          String engineType, String engineVersion) throws SQLException {
    PreparedStatement st;
    String command = "";

    command += "INSERT INTO " + getTableName() + "\n" + "(userName, machineName, xml, queryTime";
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      command += ", queryID, resultCount, duration, exportType, engineType, engineVersion";
    }
    command += ") \n" + "VALUES (?, ?, ?, ?";
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      command += ", ?, ?, ?, ?, ?, ?";
    }
    command += ")";
    st = sqlManager.createPreparedStatement(command);
    int paramOffset = 1;
    // String user = DwClientConfiguration.getInstance().user;
    String machineName;
    try {
      machineName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new SQLException(e);
    }
    Timestamp queryTime = new Timestamp(System.currentTimeMillis());
    st.setString(paramOffset++, user);
    st.setString(paramOffset++, machineName);
    st.setString(paramOffset++, queryXML);
    st.setTimestamp(paramOffset++, queryTime);
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      st.setLong(paramOffset++, queryID);
      st.setLong(paramOffset++, -1);
      st.setLong(paramOffset++, -1);
      st.setString(paramOffset++, exportType);
      st.setString(paramOffset++, engineType);
      st.setString(paramOffset++, engineVersion);
    }
    st.execute();
    st.close();
    commit();
  }

  public void updateEntry(int queryID, long resultCount, long duration) throws SQLException {
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      String command = "UPDATE " + getTableName() + " SET "
              + "resultCount=?, duration=? WHERE queryID=?";
      PreparedStatement st = sqlManager.createPreparedStatement(command);
      int paramOffset = 1;
      st.setLong(paramOffset++, resultCount);
      st.setLong(paramOffset++, duration);
      st.setInt(paramOffset++, queryID);
      st.execute();
      st.close();
      commit();
    }
  }

  public List<LoggedQuery> getLatestLoggedQueries(String user, int numberOfQueries)
          throws SQLException {
    List<LoggedQuery> result = new ArrayList<>();
    PreparedStatement st;
    String command = "";

    if (sqlManager.getDBType() == DBType.MSSQL) {
      command += "SELECT TOP (?) * FROM " + getTableName() + "\n" + "WHERE userName=? \n"
              + "ORDER BY queryTime DESC";
    } else {
      command += "SELECT * FROM " + getTableName() + "\n" + "WHERE userName=? \n"
              + "ORDER BY queryTime DESC LIMIT ?";
    }
    st = sqlManager.createPreparedStatement(command);
    int paramOffset = 1;
    if (sqlManager.getDBType() == DBType.MSSQL) {
      st.setInt(paramOffset++, numberOfQueries);
      st.setString(paramOffset++, user);
    } else {
      st.setString(paramOffset++, user);
      st.setInt(paramOffset++, numberOfQueries);
    }
    ResultSet rs = st.executeQuery();
    while (rs.next()) {
      int logID = rs.getInt("LogID");
      String xml = rs.getString("xml");
      String machineName = rs.getString("machineName");
      String queryTime = rs.getString("queryTime");
      LoggedQuery loggedQuery = new LoggedQuery().setLogID(logID).setXml(xml)
              .setMachineName(machineName).setQueryTime(queryTime).setUserName(user);
      result.add(loggedQuery);
    }
    st.close();

    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.query.model.sql.IQueryLogAdapter#getLoggedQueryByID(int)
   */
  public LoggedQuery getLoggedQueryByID(int logID) throws SQLException {
    LoggedQuery result = null;
    PreparedStatement st;
    String command = "";

    command += "SELECT * FROM " + getTableName() + "\n" + "WHERE logID=? ";
    st = sqlManager.createPreparedStatement(command);
    int paramOffset = 1;
    st.setInt(paramOffset++, logID);
    ResultSet rs = st.executeQuery();
    if (rs.next()) {
      String xml = rs.getString("xml");
      String userName = rs.getString("userName");
      String machineName = rs.getString("machineName");
      String queryTime = rs.getString("queryTime");
      result = new LoggedQuery().setLogID(logID).setXml(xml).setMachineName(machineName)
              .setQueryTime(queryTime).setUserName(userName);
    }
    st.close();

    return result;
  }

}
