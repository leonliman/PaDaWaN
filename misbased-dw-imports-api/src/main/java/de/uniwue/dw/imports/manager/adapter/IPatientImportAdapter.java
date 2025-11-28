package de.uniwue.dw.imports.manager.adapter;

import java.sql.SQLException;

import de.uniwue.dw.imports.data.PatientInfo;

public interface IPatientImportAdapter {

  void insert(long pid, boolean storno, String sex, int yob, String importFile) throws SQLException;

  void deleteInfosOfStornoPIDs() throws SQLException;

  PatientInfo getPatient(long pid) throws SQLException;

  void commit() throws SQLException;

  void dispose();

  void read() throws SQLException;

  boolean truncateTable() throws SQLException;

}