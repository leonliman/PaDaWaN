package de.uniwue.dw.imports.manager.adapter;

import java.sql.SQLException;
import java.sql.Timestamp;

import de.uniwue.dw.imports.ImportException;

public interface IRefIDAdapter {

  void commit() throws SQLException;

  void dispose();

  boolean dropTable() throws SQLException;

  public Long getUsedOrNewRefID(int attrId, long pid, long caseID, Timestamp startTime, boolean ignoreNanoseconds) throws ImportException;

}