package de.uniwue.dw.imports.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import de.uniwue.dw.imports.manager.adapter.ISourceTablesAdapter;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;

public class SQLSourceTablesAdapter extends DatabaseManager implements ISourceTablesAdapter {

  public SQLSourceTablesAdapter(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
    createSQLTables();
  }


  @Override
  public String getTableName() {
    return "DWSourceTables";
  }


  @Override
  public void storeMaxSequenceNrs(Map<String, Long> seqNrs) throws SQLException {
    for (String key : seqNrs.keySet()) {
      long seqNr = seqNrs.get(key);
      String command = "UPDATE " + getTableName() + " SET maxSequenceNb=? WHERE targetTable=?;\n"
              + "IF @@ROWCOUNT=0 \n " + "INSERT INTO " + getTableName() + " (targetTable, maxSequenceNb) VALUES (?, ?)";
      PreparedStatement st = sqlManager.createPreparedStatement(command);
      int offset = 1;
      st.setLong(offset++, seqNr);
      st.setString(offset++, key);
      st.setString(offset++, key);
      st.setLong(offset++, seqNr);
      try {
        st.execute();
      } catch (Exception e) {
        throw e;
      }
      st.close();
    }
    commit();
  }


  @Override
  public void storeMaxRecordIDs(Map<String, Long> recIDs) throws SQLException {
    for (String key : recIDs.keySet()) {
      long seqNr = recIDs.get(key);
      String command = "UPDATE " + getTableName() + " SET maxRecordID=? WHERE targetTable=?;\n" + "IF @@ROWCOUNT=0 \n "
              + "INSERT INTO " + getTableName() + " (targetTable, maxRecordID) VALUES (?, ?)";
      PreparedStatement st = sqlManager.createPreparedStatement(command);
      int offset = 1;
      st.setLong(offset++, seqNr);
      st.setString(offset++, key);
      st.setString(offset++, key);
      st.setLong(offset++, seqNr);
      try {
        st.execute();
      } catch (Exception e) {
        throw e;
      }
      st.close();
    }
    commit();
  }


  @Override
  public HashMap<String, Long> loadMaxSequenceNrs() throws SQLException {
    PreparedStatement st;
    String command = "select targetTable, maxSequenceNb from " + getTableName();
    st = sqlManager.createPreparedStatement(command);
    ResultSet executeQuery = st.executeQuery();
    HashMap<String, Long> result = new HashMap<String, Long>();
    while (executeQuery.next()) {
      String tableName = executeQuery.getString("targetTable").toLowerCase();
      long seqNr = executeQuery.getLong("maxSequenceNb");
      ;
      result.put(tableName, seqNr);
    }
    st.close();
    return result;
  }


  @Override
  public HashMap<String, Long> loadMaxRecordIDs() throws SQLException {
    PreparedStatement st;
    String command = "select targetTable, MaxRecordID from " + getTableName();
    st = sqlManager.createPreparedStatement(command);
    ResultSet executeQuery = st.executeQuery();
    HashMap<String, Long> result = new HashMap<String, Long>();
    while (executeQuery.next()) {
      String tableName = executeQuery.getString("targetTable").toLowerCase();
      long seqNr = executeQuery.getLong("MaxRecordID");
      result.put(tableName, seqNr);
    }
    st.close();
    return result;
  }


  @Override
  // @formatter:off
  protected String getCreateTableString() {
    String command = getCreateTableStub();
    command += "targetTable VARCHAR(200) PRIMARY KEY, \n" + 
               "MaxSequenceNb BIGINT, \n" + 
               "MaxRecordID BIGINT)";
    return command;
  } // @formatter:on


  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
  }

}
