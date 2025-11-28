package de.uniwue.dw.imports.manager;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.imports.IDataElem;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.api.DWImportsConfig;
import de.uniwue.dw.imports.data.ImportedFileData;
import de.uniwue.dw.imports.manager.adapter.IImportLogHandlerAdapter;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;

public class ImportLogManager {

  private static IImportLogHandlerAdapter logAdapter;

  private static Map<String, ArrayList<ImportedFileData>> importedFiles = new HashMap<String, ArrayList<ImportedFileData>>();

  private static Logger log = LogManager.getLogger("DWImport");

  public static void init() throws SQLException {
    initializeAdapters();
  }


  public static void initializeAdapters() throws SQLException {
    if (SQLPropertiesConfiguration.getInstance().getSQLConfig().isValid()) {
      logAdapter = DWImportsConfig.getInstance().getImportsAdapterFactory().createLogHandlerAdapter();
    } else {
      System.out.println("logging without database");
    }
  }


  private ImportLogManager() {
  }


  public static void truncateTables() throws SQLException {
    if (logAdapter != null) {
      logAdapter.truncateTable();
      clearImportedFiles();
    }
  }


  public static void clearImportedFiles() {
    if (importedFiles != null) {
      importedFiles.clear();
    }
  }


  public static void error(ImportException e) {
    if (!DWImportsConfig.getDoLogErrors()) {
      return;
    }
    String msg = e.getMessage();
    Exception ext = e.getExtException();
    if (ext != null) {
      String stackTrace = ExceptionUtils.getStackTrace(e);
      msg += "; " + stackTrace;
    }
    if (!DWImportsConfig.getMissingMetaInfoIsError() && ((e.getType() == ImportExceptionType.NO_PID)
            || (e.getType() == ImportExceptionType.NO_CASE) || (e.getType() == ImportExceptionType.NO_DOC))) {
      return;
    } else {
      if (logAdapter != null) {
        logAdapter.saveEntry(msg, "error", e.getType().toString(), e.getProject(), e.getFile(), e.getLine());
      }
      log.error(e);
    }
  }


  public static void error(ImportExceptionType type, String message, Exception e) {
    if (!DWImportsConfig.getDoLogErrors()) {
      return;
    }
    String msg = message;
    if (e != null) {
      msg += "; Exception: " + ExceptionUtils.getStackTrace(e) + "(" + e.getMessage() + ")";
    }
    if (logAdapter != null) {
      logAdapter.saveEntry(msg, "error", type.toString(), "", null, 0);
    }
    log.error(e);
  }


  public static void warn(ImportException e) {
    if (!DWImportsConfig.getDoLogInfosAndWarnings()) {
      return;
    }
    String msg = e.getMessage();
    Exception ext = e.getExtException();
    if (ext != null) {
      msg += "; Exception: " + ext.getStackTrace() + "(" + ext.getMessage() + ")";
    }
    if (logAdapter != null) {
      logAdapter.saveEntry(msg, "warning", e.getType().toString(), e.getProject(), e.getFile(), e.getLine());
    }
    log.warn(e);
  }


  public static void warn(String message, ImportException e) {
    if (!DWImportsConfig.getDoLogInfosAndWarnings()) {
      return;
    }
    String msg = "";
    if ((message != null) && !message.isEmpty()) {
      msg = message + "\n";
    }
    msg += e.getMessage();
    Exception ext = e.getExtException();
    if (ext != null) {
      msg += "; Exception: " + ext.getStackTrace() + "(" + ext.getMessage() + ")";
    }
    if (logAdapter != null) {
      logAdapter.saveEntry(msg, "warning", e.getType().toString(), e.getProject(), e.getFile(), e.getLine());
    }
    log.warn(e);
  }


  public static void warn(String msg, ImportExceptionType exType, String project, IDataElem file, long line) {
    if (!DWImportsConfig.getDoLogInfosAndWarnings()) {
      return;
    }
    ImportException ex = new ImportException(exType, msg, project);
    ex.setLine(line);
    ex.setFile(file);
    warn(ex);
  }


  public static void info(List<ImportException> exps) throws ImportException {
    if (DWImportsConfig.getDoLogInfosAndWarnings()) {
      if (exps.size() > 0) {
        for (ImportException e : exps) {
          String msg = e.getMessage();
          Exception ext = e.getExtException();
          if (ext != null) {
            msg += "; Exception: " + ext.getStackTrace() + "(" + ext.getMessage() + ")";
          }
          if (logAdapter != null) {
            if (SQLPropertiesConfiguration.getSQLBulkImportDir() != null) {
              logAdapter.saveEntryByBulk(msg, "info", e.getType().toString(), e.getProject(), e.getFile(), e.getLine());
            } else {
              logAdapter.saveEntry(msg, "info", e.getType().toString(), e.getProject(), e.getFile(), e.getLine());
            }
          }
          log.info(e);
        }
        try {
          if (logAdapter != null) {
            logAdapter.commit();
          }
        } catch (SQLException e1) {
          throw new ImportException(ImportExceptionType.SQL_ERROR, e1);
        }
      }
    }
  }


  public static void info(ImportException e) {
    if (!DWImportsConfig.getDoLogInfosAndWarnings()) {
      return;
    }
    String msg = e.getMessage();
    Exception ext = e.getExtException();
    if (ext != null) {
      msg += "; Exception: " + ext.getStackTrace() + "(" + ext.getMessage() + ")";
    }
    if (logAdapter != null) {
      logAdapter.saveEntry(msg, "info", e.getType().toString(), e.getProject(), e.getFile(), e.getLine());
    }
    log.info(e);
  }


  public static void info(String message) {
    if (!DWImportsConfig.getDoLogInfosAndWarnings()) {
      return;
    }
    if (logAdapter != null) {
      logAdapter.saveEntry(message, "info", "", "", null, 0);
    }
    log.info(message);
//    System.out.println(new java.util.Date().toString() + ": " + message);
  }


  public static ImportException getFileImportSuccessNotification(IDataElem aDataElem, String project)
          throws ImportException {
    ImportException tmp = new ImportException(ImportExceptionType.FILE_IMPORT_SUCCESS,
            "data source imported successfull");
    tmp.setFile(aDataElem);
    tmp.setProject(project);
    return tmp;
  }


  public static void fileImportSuccess(IDataElem aDataElem, String project) throws ImportException {
    ImportException tmp = getFileImportSuccessNotification(aDataElem, project);
    info(tmp);
  }


  public static List<ImportedFileData> getImportedFiles(String aProject) throws ImportException {
    if (!importedFiles.containsKey(aProject)) {
      try {
//        ImportLogManager.info(new ImportException(ImportExceptionType.IMPORT_PROCESS,
//                "Fetching already imported files from DWImportLog for project '" + aProject + "'.", aProject));
        ArrayList<ImportedFileData> result = logAdapter.getImportedFiles(aProject);
        importedFiles.put(aProject, result);
      } catch (SQLException e) {
        throw new ImportException(ImportExceptionType.SQL_ERROR, e);
      }
    }
    return importedFiles.get(aProject);
  }


  public static boolean isDataElemAlreadyImported(IDataElem aDataElem, String project) throws ImportException {
    String filename = aDataElem.getName();
    for (ImportedFileData sf : getImportedFiles(project)) {
      if (sf.getFilename().equals(filename)) {
        long thisLast = 1000 * (aDataElem.getTimestamp() / 1000);
        boolean cmpDat = sf.getLastModTime() >= thisLast;
        Integer cmpProj = sf.getProject().compareTo(project);
        if (cmpDat && cmpProj == 0) {
          return true;
        }
      }
    }
    return false;
  }


  public static void fileImportError(IDataElem file, String project) {
    ImportException tmp = new ImportException(ImportExceptionType.FILE_IMPORT_ERROR, "file imported successfull");
    tmp.setFile(file);
    tmp.setProject(project);
    error(tmp);
  }

}
