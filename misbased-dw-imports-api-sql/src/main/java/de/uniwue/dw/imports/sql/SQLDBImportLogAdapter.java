package de.uniwue.dw.imports.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;

import de.uniwue.dw.imports.manager.adapter.IDBImportLogAdapter;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;

public class SQLDBImportLogAdapter extends DatabaseManager implements IDBImportLogAdapter {

  public SQLDBImportLogAdapter(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
    createSQLTables();
  }


  @Override
  public String getTableName() {
    return "DWDBImportLogs";
  }


  @Override
  public HashMap<String, Long> loadRecordIDs() throws SQLException {
    PreparedStatement st;
    String command = "select project, RecordID from " + getTableName();
    st = sqlManager.createPreparedStatement(command);
    ResultSet executeQuery = st.executeQuery();
    HashMap<String, Long> result = new HashMap<String, Long>();
    while (executeQuery.next()) {
      String project = executeQuery.getString("project");
      long seqNr = executeQuery.getLong("RecordID");
      result.put(project, seqNr);
    }
    st.close();
    return result;
  }


  @Override
  // @formatter:off
  protected String getCreateTableString() {
    String command = getCreateTableStub();
    command += "project VARCHAR(200) PRIMARY KEY, \n" + 
               "RecordID BIGINT, \n" + 
               "lastUpdate " + SQLTypes.timestampType(sqlManager.config) + " \n" + 
            ")";
    return command;
  } // @formatter:on


  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
  }


  public void saveRecordID(String project, Long recordID) throws SQLException {
    String command = "UPDATE " + getTableName() + " SET " + "recordID=? WHERE project=?;\n" + "IF @@ROWCOUNT=0 \n "
            + "INSERT INTO " + getTableName() + " (project, recordID) VALUES (?, ?)";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    int offset = 1;
    st.setLong(offset++, recordID);
    st.setString(offset++, project);
    st.setString(offset++, project);
    st.setLong(offset++, recordID);
    try {
      st.execute();
    } catch (Exception e) {
      throw e;
    }
    st.close();
    commit();
  }


  @Override
  public void saveLastUpdateTimestamp(String project, Timestamp timestampTicks) throws SQLException {
    String command = "UPDATE " + getTableName() + " SET " + "lastUpdate=? WHERE project=?;\n" + "IF @@ROWCOUNT=0 \n "
            + "INSERT INTO " + getTableName() + " (project, lastUpdate) VALUES (?, ?)";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    int offset = 1;
    st.setTimestamp(offset++, timestampTicks);
    st.setString(offset++, project);
    st.setString(offset++, project);
    st.setTimestamp(offset++, timestampTicks);
    try {
      st.execute();
    } catch (Exception e) {
      throw e;
    }
    st.close();
    commit();
  }


  @Override
  public HashMap<String, Timestamp> loadLastUpdateTimestamps() throws SQLException {
    PreparedStatement st;
    String command = "select project, lastUpdate from " + getTableName();
    st = sqlManager.createPreparedStatement(command);
    ResultSet executeQuery = st.executeQuery();
    HashMap<String, Timestamp> result = new HashMap<String, Timestamp>();
    while (executeQuery.next()) {
      String project = executeQuery.getString("project");
      Timestamp lastUpdate = executeQuery.getTimestamp("lastUpdate");
      result.put(project, lastUpdate);
    }
    st.close();
    return result;
  }

}
