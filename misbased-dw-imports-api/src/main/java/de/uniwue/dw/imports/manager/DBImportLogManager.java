package de.uniwue.dw.imports.manager;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.api.DWImportsConfig;
import de.uniwue.dw.imports.manager.adapter.IDBImportLogAdapter;
import de.uniwue.dw.imports.manager.adapter.IHDP_TableMappingsAdapter;
import de.uniwue.dw.imports.manager.adapter.ISourceTablesAdapter;
import de.uniwue.misc.sql.SQLConfig;

public class DBImportLogManager {

  private IHDP_TableMappingsAdapter sequenceNrInitialReadAdapter;

  private IDBImportLogAdapter dbImportLogAdapter;

  private ISourceTablesAdapter sourceTablesAdapter;

  private Map<String, Timestamp> lastUpdateMap;

//  private Map<String, Long> maxSequenceNrsMap;

  private Map<String, Long> currentRecordIDMap;

  private Map<String, Long> maxRecordIDMap;

  public Set<String> sourceTables = new HashSet<String>();

  public DBImportLogManager(SQLConfig sqlConfig) throws SQLException {
    if (DWImportsConfig.isProperlyHDPConfigured()) {
      sequenceNrInitialReadAdapter = DWImportsConfig.getInstance().getImportsAdapterFactory()
              .createHDP_TableMappingsAdapter(sqlConfig);
      dbImportLogAdapter = DWImportsConfig.getInstance().getImportsAdapterFactory().createRecordIDAdapter();
      sourceTablesAdapter = DWImportsConfig.getInstance().getImportsAdapterFactory().createSourceTablesAdapter();
    }
  }

//  public Long getMaxSequenceNr(String tableName) throws SQLException {
//    return maxSequenceNrsMap.get(tableName);
//  }


  public void addSourceTable(String aTableName) {
    sourceTables.add(aTableName);
  }


  public void truncateTables() throws SQLException {
    if (DWImportsConfig.isProperlyHDPConfigured()) {
      dbImportLogAdapter.truncateTable();
      sourceTablesAdapter.truncateTable();
    }
//    maxSequenceNrsMap = new HashMap<String, Long>();
    currentRecordIDMap = new HashMap<String, Long>();
    maxRecordIDMap = new HashMap<String, Long>();
  }


  public void insertOrUpdateMaxRecordID(long recordID, String table) throws SQLException {
    table = table.toLowerCase();
    maxRecordIDMap.put(table, recordID);
    if (DWImportsConfig.isProperlyHDPConfigured()) {
      dbImportLogAdapter.saveRecordID(table, recordID);
    }
  }


  public void insertOrUpdateLastUpdateTimestamp(Timestamp lastUpdateTimestamp, String table) throws SQLException {
    table = table.toLowerCase();
    lastUpdateMap.put(table, lastUpdateTimestamp);
    if (DWImportsConfig.isProperlyHDPConfigured()) {
      dbImportLogAdapter.saveLastUpdateTimestamp(table, lastUpdateTimestamp);
    }
  }


  public Long getMaxRecordID(String tableName) throws SQLException {
    return maxRecordIDMap.get(tableName.toLowerCase());
  }


  public Long getCurrentRecordID(String tableName) throws SQLException {
    tableName = tableName.toLowerCase();
    if (currentRecordIDMap.containsKey(tableName)) {
      return currentRecordIDMap.get(tableName);
    } else {
      return 0L;
    }
  }


  public Timestamp getLastUpdateTimestamp(String tableName) throws SQLException {
    return lastUpdateMap.get(tableName.toLowerCase());
  }


  public void initialize() throws ImportException {
    try {
      if (DWImportsConfig.isProperlyHDPConfigured()) {
        currentRecordIDMap = dbImportLogAdapter.loadRecordIDs();
        lastUpdateMap = dbImportLogAdapter.loadLastUpdateTimestamps();
        maxRecordIDMap = sequenceNrInitialReadAdapter.loadInitializeMaxRecordsFromSourceTables();
      }
      if (DWImportsConfig.getTreatAsInitialImport()) {
//        maxSequenceNrsMap = sequenceNrInitialReadAdapter.loadSequenceNrs();
//        sourceTablesAdapter.storeMaxSequenceNrs(maxSequenceNrsMap);
//        sourceTablesAdapter.storeMaxRecordIDs(maxRecordIDMap);
      } else {
//        maxSequenceNrsMap = sourceTablesAdapter.loadMaxSequenceNrs();
//        maxRecordIDMap = sourceTablesAdapter.loadMaxRecordIDs();
      }
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, e);
    }
  }

}
