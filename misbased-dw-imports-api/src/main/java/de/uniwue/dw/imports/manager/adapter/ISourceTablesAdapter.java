package de.uniwue.dw.imports.manager.adapter;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public interface ISourceTablesAdapter {

  HashMap<String, Long> loadMaxSequenceNrs() throws SQLException;

  HashMap<String, Long> loadMaxRecordIDs() throws SQLException;

  void storeMaxSequenceNrs(Map<String, Long> seqNrs) throws SQLException;

  void storeMaxRecordIDs(Map<String, Long> recIDs) throws SQLException;

  boolean truncateTable() throws SQLException;

}
