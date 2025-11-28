package de.uniwue.dw.imports.impl.base;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;

import de.uniwue.dw.imports.CatalogImporter;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.ImporterTable;
import de.uniwue.dw.imports.PIDFilterException;
import de.uniwue.dw.imports.configured.data.ConfigMetaDocs;
import de.uniwue.dw.imports.data.CaseInfo;
import de.uniwue.dw.imports.manager.ImportLogManager;
import de.uniwue.dw.imports.manager.ImporterManager;
import de.uniwue.misc.util.TimeUtil;

public class DocImporter extends ImporterTable {

  // TODO: warum und wie viele Docs gibt es mit der gleichen DocID und unterschiedlichem
  // erstellungsdatum ?
  // TODO: löschen von Dokumenten mit Zeit 00:00:00

  public static final String PARAM_IMPORT_DIR = "import.dir.documents";

  public static String projectName = "Documents";

  private ConfigMetaDocs config;

  public DocImporter(ImporterManager mgr, ConfigMetaDocs aConfig) throws ImportException {
    super(mgr, null, projectName, aConfig.getDataSource());
    config = aConfig;
  }


  @Override
  protected CatalogImporter createCatalogImporter() throws ImportException {
    return null; // no catalog entries for docs
  }


  private boolean getStorno() throws ImportException {
    String stornoString = getItem(config.stornoColumn);
    boolean storno = (stornoString.equals("X"));
    return storno;
  }


  private Timestamp getCreationTimestamp() throws ImportException {
    String fullTimeString;
    if (config.timeStampColumn == null) {
      String dateString = getItem(config.dateColumn);
      if (dateString.equals(". .")) {
        dateString = "01.01.1900";
      }
      String timeString = getItem(config.timeColumn);
      if (timeString.equals(": :") || timeString.equals(":00:00")) {
        timeString = "00:00:00";
      }
      fullTimeString = dateString + " " + timeString;
    } else {
      fullTimeString = getItem(config.timeStampColumn);
    }
    Timestamp measureDate;
    try {
      Date opDate = config.getFormat().parse(fullTimeString);
      String timeStampString = TimeUtil.getSdfWithTimeSQLTimestamp().format(opDate);
      measureDate = Timestamp.valueOf(timeStampString);
    } catch (ParseException e) {
      throw new ImportException(ImportExceptionType.DATA_PARSING_ERROR,
              "parsing datetime string '" + fullTimeString + "'", e);
    }
    return measureDate;
  }


  @Override
  protected void processImportInfoFileLine() throws ImportException, SQLException {
    Long docID = getDocID(config.docIDColumn);
    Long caseID = getCaseID(config.caseIDColumn);
    Long pid = 0L;
    if (caseID != 0) {
      CaseInfo caseInfo;
      try {
        caseInfo = getCaseInfo(caseID);
        pid = caseInfo.pid;
        if (config.pidColumn != null) {
          long givenPid = getPID(config.pidColumn);
          if (caseInfo.pid != givenPid) {
            ImportLogManager.warn(
                    "Doc " + docID + " has pid " + givenPid + " but case " + caseID + " has pid " + caseInfo.pid,
                    ImportExceptionType.DATA_MISMATCH, getProject(), tableElem, getRowCounter());
          }
        }
      } catch (PIDFilterException e) {
        // although the data of some PIDs should not be imported, it meta data should for sake of
        // process completeness
      }
    } else {
      pid = getPID(config.pidColumn);
    }
    if (pid == 0) {
      return; // Some PIDs are empty. Just don't import them
    }
    boolean storno = getStorno();
    Timestamp measureDate = getCreationTimestamp();
    String typeString = getItem(config.docTypeColumn);
    String filename = tableElem.getName();
    getDocManager().insert(docID, caseID, measureDate, pid, typeString, storno, filename);
  }


  @Override
  protected void commit() throws ImportException {
    super.commit();
    try {
      getDocManager().commit();
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, "", getProject(), e);
    }
  }

}
