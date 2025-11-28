package de.uniwue.dw.imports.impl.base;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import de.uniwue.dw.imports.CatalogImporter;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.ImporterTable;
import de.uniwue.dw.imports.configured.data.ConfigMetaMovs;
import de.uniwue.dw.imports.manager.DataUtil;
import de.uniwue.dw.imports.manager.ImportLogManager;
import de.uniwue.dw.imports.manager.ImporterManager;
import de.uniwue.misc.util.RegexUtil;
import de.uniwue.misc.util.TimeUtil;

/*
 * As the admission and discharge times from the clinical case files are very accounting related (granularity of year quarters), 
 * the admission and discharge times are calculated based on the patient's movements. All movements are calculated in an earliest 
 * and a latest movement. These are the admission and discharge times.
 */
public class MovImporter extends ImporterTable {

  public static final String PARAM_IMPORT_DIR = "import.dir.movements";

  public static String projectName = "MovMetadata";

  private ConfigMetaMovs config;

  public MovImporter(ImporterManager mgr, ConfigMetaMovs aConfig) throws ImportException {
    super(mgr, null, projectName, aConfig.getDataSource());
    config = aConfig;
    setRequiredCSVColumnHeaders(new String[] { aConfig.caseIDColumn, aConfig.moveTypeColumn });
  }


  @Override
  protected CatalogImporter createCatalogImporter() throws ImportException {
    return null; // no catalog entries for movements-import-metadata
  }


  private Timestamp getTimestamp(String timestampColumn, String dateColumnName, String timeColumnName,
          SimpleDateFormat aFormat) throws ImportException {
    Timestamp ts = null;
    Date date = null;
    try {
      if (dateColumnName != null) {
        String dateString, timeString;
        dateString = getItem(dateColumnName);
        timeString = getItem(timeColumnName);
        if (timeString.equals(": :")) {
          timeString = "00:00:00";
        }
        date = aFormat.parse(dateString + " " + timeString);
      } else {
        String timeString = getItem(timestampColumn);
        if (timeString == null) {
          timeString = "1970-01-01 00:00:00.0";
        }
        if (timeString.contains("::")) {
          timeString = timeString.replaceAll("::", "00:00:00");
        }
        date = aFormat.parse(timeString);
      }
    } catch (ParseException e) {
      ImportLogManager.warn(new ImportException(ImportExceptionType.DATA_PARSING_ERROR,
              "parsing datetime string error " + e.getMessage(), getProject(), getRowCounter()));
      return null;
    }
    if (date != null) {
      if (!DataUtil.isTimeInValidRange(date)) {
        // ImportLogManager.warn(new ImportException(ImportExceptionType.DATA_MALFORMED, "Date '"
        // + fromDate + "' is out of range ", getProject()));
        return null;
      }
      String timeStampString = TimeUtil.getSdfWithTimeSQLTimestamp().format(date);
      ts = Timestamp.valueOf(timeStampString);
      return ts;
    }
    return null;
  }


  @Override
  protected void processImportInfoFileLine() throws ImportException, SQLException {
    String caseIDString = getItem(config.caseIDColumn);
    long caseID = 0;
    if (caseIDString.matches(RegexUtil.numbersRegexString)) {
      caseID = Long.parseLong(caseIDString);
    }
    String bewTyp = getItem(config.moveTypeColumn);
    String stornoString = getItem(config.stornoColumn);
    if (stornoString.equals("X")) {
      return;
    }
    Timestamp fromDateTS = getTimestamp(config.fromTimeStampColumn, config.fromDateColumn, config.fromTimeColumn,
            config.getFromFormat());
    Timestamp toDateTS = getTimestamp(config.endTimeStampColumn, config.endDateColumn, config.endTimeColumn,
            config.getEndFormat());
    if (fromDateTS != null) {
      getCaseManager().updateIfEarlierOrLater(caseID, fromDateTS);
      if (bewTyp.equals("1")) { // Aufnahme
        getCaseManager().updateAdmission(caseID, fromDateTS);
      }
    }
    if (toDateTS != null) {
      getCaseManager().updateIfEarlierOrLater(caseID, toDateTS);
      if (bewTyp.equals("2")) { // Entlassung
        getCaseManager().updateDischarge(caseID, toDateTS);
      }
    }
  }


  protected void commit() throws ImportException {
    super.commit();
    try {
      getCaseManager().commit();
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, e);
    }
  }

}
