package de.uniwue.dw.query.model.manager.adapter;

import java.sql.SQLException;

import de.uniwue.dw.query.model.data.RawQuery;

public interface IQueryAdapter {

  RawQuery insert(String name, String xml) throws SQLException;

  void updateEntry(RawQuery aQuery) throws SQLException;

  void deleteEntry(int id) throws SQLException;

  void dispose();
  
  boolean dropTable() throws SQLException;
  
}