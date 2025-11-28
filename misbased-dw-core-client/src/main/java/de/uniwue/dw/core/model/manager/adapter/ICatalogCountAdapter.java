package de.uniwue.dw.core.model.manager.adapter;

import java.sql.SQLException;

public interface ICatalogCountAdapter {

  void readTables() throws SQLException;

  void calculateCountsBasedOnCaseIDs() throws SQLException;

  void calculateCountsBasedOnPIDs() throws SQLException;

  void calculateAbsoluteCounts() throws SQLException;

  void commit() throws SQLException;

  void dispose();

  void delete(Integer attrID) throws SQLException;

  void insertOrUpdateCounts(int attrid, long pidCount, long caseIDCount, long absoluteCount)
          throws SQLException;

  boolean dropTable() throws SQLException;

  boolean truncateTable() throws SQLException;

}
