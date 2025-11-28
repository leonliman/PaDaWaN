package de.uniwue.dw.imports.manager.adapter;

import java.sql.SQLException;
import java.util.HashMap;

public interface IHDP_TableMappingsAdapter {

  HashMap<String, Long> loadSequenceNrs() throws SQLException;

  HashMap<String, Long> loadInitializeMaxRecordsFromSourceTables() throws SQLException;

}
