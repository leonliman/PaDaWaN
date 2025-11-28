package de.uniwue.dw.imports.manager.adapter;

import java.sql.SQLException;
import java.util.ArrayList;

import de.uniwue.dw.imports.IDataElem;
import de.uniwue.dw.imports.data.ImportedFileData;

public interface IImportLogHandlerAdapter {

  void saveEntry(String message, String level, String errorType, String project, IDataElem file,
          long line);

  ArrayList<ImportedFileData> getImportedFiles(String aProject) throws SQLException;

  void commit() throws SQLException;

  void dispose();

  boolean truncateTable() throws SQLException;

  void saveEntryByBulk(String message, String level, String errorType, String project,
          IDataElem file, long line);

}