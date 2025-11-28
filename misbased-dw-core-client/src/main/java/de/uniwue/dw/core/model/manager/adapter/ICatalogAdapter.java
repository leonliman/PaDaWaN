package de.uniwue.dw.core.model.manager.adapter;

import java.io.File;
import java.sql.SQLException;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;

public interface ICatalogAdapter {

  void readTables() throws SQLException;

  void setUseBulkInserts(File aBulkFolder, boolean keepIdentity) throws SQLException;

  void insertByBulk(Object... objects) throws SQLException;

  CatalogEntry insertEntry(String name, CatalogEntryType dataType, String extID,
          int parentID, double orderValue, String aProject, String uniqueName, String descripton)
          throws SQLException;

  CatalogEntry insertEntry(int attrID, String name, CatalogEntryType dataType, String extID,
          int parentID, double orderValue, String aProject, String uniqueName, String descripton)
          throws SQLException;

  void updateEntry(CatalogEntry anEntry) throws SQLException;

  CatalogEntry getEntryByRefID(String refID, String aProject,
          boolean throwExceptionWhenNonExists) throws SQLException;

  void deleteEntry(int attrID) throws SQLException;

  void commit() throws SQLException;

  void dispose();

  boolean truncateTable() throws SQLException;

  boolean dropTable() throws SQLException;
  
}
