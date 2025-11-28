package de.uniwue.dw.imports.manager.adapter;

import java.sql.SQLException;
import java.sql.Timestamp;

import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.data.DocInfo;

public interface IDocImportAdapter {

  void insert(long docid, long caseID, Timestamp creationTime, long pid, String type, boolean storno, String importFile)
          throws SQLException;

  void deleteInfosOfStornoDocs() throws SQLException;

  DocInfo getDoc4DocID(long docID) throws SQLException, ImportException;

  void commit() throws SQLException;

  void dispose();

  boolean truncateTable() throws SQLException;

  void readTables() throws SQLException;

}