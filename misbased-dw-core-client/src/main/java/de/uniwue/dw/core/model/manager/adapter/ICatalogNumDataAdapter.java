package de.uniwue.dw.core.model.manager.adapter;

import java.io.File;
import java.sql.SQLException;

public interface ICatalogNumDataAdapter {

  void readTables() throws SQLException;

  void setUseBulkInserts(File aBulkFolder, boolean keepIdentity) throws SQLException;

  void insertByBulk(Object... objects) throws SQLException;

  void insert(Integer attrID, String unit, double lowBound, double highBound)
          throws SQLException;

  void commit() throws SQLException;

  void dispose();

  void delete(Integer attrID) throws SQLException;

  boolean dropTable() throws SQLException;

  boolean truncateTable() throws SQLException;

}
