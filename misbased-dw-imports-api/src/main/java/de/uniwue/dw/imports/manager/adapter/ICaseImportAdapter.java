package de.uniwue.dw.imports.manager.adapter;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashSet;

import de.uniwue.dw.imports.data.CaseInfo;
import de.uniwue.misc.sql.SQLInsertMode;

public interface ICaseImportAdapter {

  void insert(long pid, long caseID, boolean storno, Timestamp admissionDate,
          Timestamp dischargeDate, Timestamp firstMovement, Timestamp lastMovement,
          String importFile, Timestamp creationDate, String casetype) throws SQLException;

  CaseInfo readCase(long caseID) throws SQLException;

  HashSet<CaseInfo> readCasesForPID(long PID) throws SQLException;

  void read() throws SQLException;

  void deleteInfosOfStornoCases() throws SQLException;

  void deleteCase(long caseID) throws SQLException;

  void commit() throws SQLException;

  void dispose();

  boolean truncateTable() throws SQLException;

  void setInsertMode(SQLInsertMode insertMode);

  SQLInsertMode getInsertMode();

}