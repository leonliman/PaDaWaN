package de.uniwue.dw.imports.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import de.uniwue.dw.imports.api.DWImportsConfig;
import de.uniwue.dw.imports.manager.adapter.IHDP_TableMappingsAdapter;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;

public class SQL_HDP_TableMappingsAdapter extends DatabaseManager implements IHDP_TableMappingsAdapter {

  public SQL_HDP_TableMappingsAdapter(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
  }


  @Override
  public String getTableName() {
    return "HDP_TableMapping";
  }


  @Override
  public HashMap<String, Long> loadSequenceNrs() throws SQLException {
    HashMap<String, Long> result = new HashMap<String, Long>();
    // the sequence-numbers are no longer needed
//    PreparedStatement st;
//    String command = "select targetTable, SequenceNb from [HDP].[dbo]." + getTableName()
//            + " where TargetArea = 'HDP_ForResearch'";
////    String command = "select 1";
//    st = sqlManager.createPreparedStatement(command);
//    ResultSet executeQuery = st.executeQuery();
//    while (executeQuery.next()) {
//      String tableName = executeQuery.getString("targetTable").toLowerCase();
//      long seqNr = executeQuery.getLong("SequenceNb");
//      result.put(tableName, seqNr);
//    }
//    st.close();
    return result;
  }


  @Override
  protected String getCreateTableString() {
    return null;
  }


  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
  }


  public HashMap<String, Long> loadInitializeMaxRecordsFromSourceTables() throws SQLException {
    HashMap<String, Long> result = new HashMap<String, Long>();
    String[] sourceTables = DWImportsConfig.getDBImportLogManager().sourceTables.toArray(new String[0]);
    for (String aTableName : sourceTables) {
      PreparedStatement st;
      String command = "select max(RecordID) MaxRecordID from " + aTableName;
      st = sqlManager.createPreparedStatement(command);
      ResultSet executeQuery = st.executeQuery();
      while (executeQuery.next()) {
        long seqNr = executeQuery.getLong("MaxRecordID");
        result.put(aTableName.toLowerCase(), seqNr);
      }
      st.close();
    }
    commit();
//    result.put("patient", 1567455L);
    return result;
  }

}
