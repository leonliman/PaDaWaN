package de.uniwue.dw.imports.manager.adapter;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;

public interface IDBImportLogAdapter {

  HashMap<String, Long> loadRecordIDs() throws SQLException;

  HashMap<String, Timestamp> loadLastUpdateTimestamps() throws SQLException;

  void saveRecordID(String tableName, Long recordID) throws SQLException;

  void saveLastUpdateTimestamp(String tableName, Timestamp timestamp) throws SQLException;

  boolean truncateTable() throws SQLException;

}
