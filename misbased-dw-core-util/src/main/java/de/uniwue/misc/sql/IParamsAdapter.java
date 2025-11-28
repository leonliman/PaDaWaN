package de.uniwue.misc.sql;

import java.sql.SQLException;

public interface IParamsAdapter {

  void deleteParam(String paramName) throws SQLException;

  String getParam(String paramName) throws SQLException;

  String getParam(String paramName, String defaultValue) throws SQLException;

  void setParam(String paramName, String value) throws SQLException;

  void commit() throws SQLException;

  void lock() throws SQLException;
  
  
  
}