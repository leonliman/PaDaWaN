package de.uniwue.dw.core.model.manager.adapter;

import java.sql.SQLException;

public interface ICatalogChoiceAdapter {

  void readTables() throws SQLException;

  void insert(Integer attrID, String choice) throws SQLException;

  void delete(Integer attrID) throws SQLException;

  boolean dropTable() throws SQLException;

  boolean truncateTable() throws SQLException;
  
  void commit() throws SQLException;

  void dispose();
  
}
