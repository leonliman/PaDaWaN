package de.uniwue.dw.imports;

import java.io.IOException;
import java.sql.SQLException;

import de.uniwue.dw.imports.ImportException.ImportExceptionLevel;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.api.DWImportsConfig;
import de.uniwue.dw.imports.configured.data.ConfigCatalogEntry;
import de.uniwue.dw.imports.configured.data.ConfigDataSource;
import de.uniwue.dw.imports.data.CaseInfo;
import de.uniwue.dw.imports.data.DocInfo;
import de.uniwue.dw.imports.data.PatientInfo;
import de.uniwue.dw.imports.manager.ImportLogManager;
import de.uniwue.dw.imports.manager.ImporterManager;
import de.uniwue.misc.util.RegexUtil;

/**
 * 
 * Abstract class that provides architecture for all csv-based importers. Provides further
 * functionality needed for handling csv files.
 *
 */
public abstract class ImporterTable extends Importer {

  protected ITableElem tableElem;

  private String[] requiredHeaders = new String[0];

  public ImporterTable() {
  }


  public ImporterTable(ImporterManager mgr, ConfigCatalogEntry aParentEntry, String projectName,
          ConfigDataSource dataSource) throws ImportException {
    super(mgr, aParentEntry, projectName, dataSource);
  }


  /**
   * To be implemented in a ProjectImporter in case of a CSV file -> process a single CSV line
   * 
   * @throws ImportException
   * @throws SQLException
   * @throws IOException
   */
  protected abstract void processImportInfoFileLine() throws ImportException, SQLException, IOException;


  /**
   * This method is intended to be called for any work done prior to the specific CSV lines, but
   * after the header was read
   */
  protected void runBeforeImportInfoFileLines() throws ImportException {
  }


  @Override
  public void importInfos() throws ImportException {
    runBeforeImport();
    IDataElemIterator filesToImport = getFilesToProcess();
    do {
      tableElem = (ITableElem) filesToImport.next();
      if (tableElem == null) {
        break;
      }
      try {
        if (ImportLogManager.isDataElemAlreadyImported(tableElem, getProject())) {
          continue;
        }
        String logText = getProject();
        if (tableElem.getName() != null) {
          logText += ": starting file '" + tableElem.getName() + "'";
          ImportLogManager.info(new ImportException(ImportExceptionType.IMPORT_PROCESS, logText, getProject()));
        }
        tableElem.checkRequiredHeaders(requiredHeaders);
        runBeforeImportInfoFileLines();
        Boolean fileImportSuccessfull = doIterationOnFileLines();
        tableElem.close();
        commit();
        if ((fileImportSuccessfull) && (tableElem.getName() != null)) {
          ImportLogManager.fileImportSuccess(tableElem, getProject());
        }
      } catch (ImportException e) {
        if (e.getType() == ImportExceptionType.PID_FILTER) {
          continue;
        }
        e.setProject(getProject());
        ImportLogManager.error(e);
        continue;
      }
    } while (true);
    filesToImport.dispose();
    commit();
    runAfterImport();
  }


  protected boolean doIterationOnFileLines() throws ImportException {
    long successfulRows = 0L;
    long failedRows = 0L;
    Boolean fileImportSuccessfull = true;
    while (tableElem.moveToNextLine()) {
      try {
        if (tableElem.isCurrentLineWellFormed()) {
          try {
            processImportInfoFileLine();
          } catch (SQLException e) {
            throw new ImportException(ImportExceptionType.SQL_ERROR, "", e);
          } catch (IOException e) {
            throw new ImportException(ImportExceptionType.IO_ERROR, "", e);
          }
          successfulRows++;
        } else {
          throw new ImportException(ImportExceptionType.CSV_HEADER_ROW_LENGTH_MISMATCH,
                  "not same amount of tokans as its header", getProject());
        }
      } catch (ImportException e) {
        if (!DWImportsConfig.getMissingMetaInfoIsError() && ((e.getType() == ImportExceptionType.NO_PID)
                || (e.getType() == ImportExceptionType.NO_CASE) || (e.getType() == ImportExceptionType.NO_DOC))) {
          continue;
        }
        if (e.getType() == ImportExceptionType.PID_FILTER) {
          continue;
        }
        e.setLine(getRowCounter());
        e.setFile(tableElem);
        e.setProject(getProject());
        if (e.getLevel() == ImportExceptionLevel.error) {
          failedRows++;
          ImportLogManager.error(e);
        } else {
          ImportLogManager.warn(e);
        }
      } catch (NullPointerException e) {
        failedRows++;
        ImportException x = new ImportException(ImportExceptionType.NULLPOINTER, e.getMessage());
        x.setLine(getRowCounter());
        x.setFile(tableElem);
        x.setProject(getProject());
        ImportLogManager.error(x);
      } catch (Exception e) {
        failedRows++;
        ImportException x = new ImportException(ImportExceptionType.UNKNOWN,
                e.getClass().toString() + " : " + e.getMessage());
        x.setLine(getRowCounter());
        x.setFile(tableElem);
        x.setProject(getProject());
        ImportLogManager.error(x);
      }
      if (getRowCounter() % 5000 == 0) {
        System.out.print(".");
        commit();
        tableElem.logLatestRowNumber();
      }
    }
    commit();
    tableElem.logLatestRowNumber();
    ImportLogManager.info(new ImportException(
            ImportExceptionType.IMPORT_PROCESS, getProject() + ": Successfully processed " + successfulRows
                    + " lines, failed " + failedRows + " lines while finishing file '" + tableElem.getName() + "'",
            getProject()));
    double failFraction = DWImportsConfig.getCSVFailRate();
    if (((failedRows * 1.0d) / (getRowCounter() * 1.0d)) > failFraction) {
      fileImportSuccessfull = false;
    }
    return fileImportSuccessfull;
  }


  protected PatientInfo getPatientInfo(String pidColumnName) throws ImportException {
    String PIDString = getItem(pidColumnName);
    if (!PIDString.matches("\\d+")) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED, "PID '" + PIDString + "' is malformed",
              getProject());
    }
    Long PID = Long.parseLong(PIDString);
    PatientInfo pInfo = getPatientInfo(PID);
    return pInfo;
  }


  protected DocInfo getDocInfo(String docIDColumnName) throws ImportException {
    String docIDString = getItem(docIDColumnName);
    if (!docIDString.matches("\\d+")) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED, "docID '" + docIDString + "' is malformed",
              getProject());
    }
    long docID = Long.parseLong(docIDString);
    DocInfo docInfo = getDocInfo(docID);
    return docInfo;
  }


  protected CaseInfo getCaseInfo(String caseIDColumnName) throws ImportException {
    String caseIDString = getItem(caseIDColumnName);
    if (!caseIDString.matches("\\d+")) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED, "caseID '" + caseIDString + "' is malformed",
              getProject());
    }
    long caseID = Long.parseLong(caseIDString);
    CaseInfo caseInfo = getCaseInfo(caseID);
    return caseInfo;
  }


  protected long getDocID(String docIDColumn) throws ImportException {
    String docIDString = getItem(docIDColumn).trim();
    long docID = 0;
    if (RegexUtil.isNumber(docIDString)) {
      docID = Long.parseLong(docIDString);
    } else {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED, "strange docID '" + docIDString + "'");
    }
    return docID;
  }


  protected long getCaseID(String caseIDColumn) throws ImportException {
    String caseIDString = getItem(caseIDColumn);
    if (caseIDString == null) {
      caseIDString = "0";
    }
    caseIDString = caseIDString.trim();
    long caseID = 0L;
    if (RegexUtil.isNumber(caseIDString)) {
      caseID = Long.parseLong(caseIDString);
    } else if (caseIDString.trim().isEmpty()) {
      // non-existing caseID can exist, actually they are quite common.
      // In this case the imported info just has no caseID which can happen
      // and which is not wrong
    } else {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED, "strange caseID '" + caseIDString + "'");
    }
    return caseID;
  }


  protected long getPID(String pidColumn) throws ImportException {
    String pidString = getItem(pidColumn);
    long pid = 0L;
    if (pidString != null) {
      pidString = pidString.trim();
      if (!RegexUtil.isNumber(pidString)) {
        throw new ImportException(ImportExceptionType.DATA_MALFORMED, "PID string '" + pidString + "' is no number");
      }
      pid = Long.parseLong(pidString);
    }
    return pid;
  }


  protected void setRequiredCSVColumnHeaders(String[] headers) {
    requiredHeaders = headers;
  }


  public Integer getColumnIndex(String key) throws ImportException {
    return tableElem.getColumnIndex(key);
  }


  public String getItem(String key) throws ImportException {
    return tableElem.getItem(key);
  }


  public String[] getCurrentLineTokens() {
    return tableElem.getCurrentLineTokens();
  }


  public Long getRowCounter() {
    return tableElem.getRowCounter();
  }

}
