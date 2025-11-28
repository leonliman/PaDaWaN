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
import de.uniwue.dw.imports.configured.data.ConfigMetaCases;
import de.uniwue.dw.imports.manager.DataUtil;
import de.uniwue.dw.imports.manager.ImportLogManager;
import de.uniwue.dw.imports.manager.ImporterManager;
import de.uniwue.misc.util.TimeUtil;

public class CaseImporter extends ImporterTable {

  public static final String PARAM_IMPORT_DIR = "import.dir.cases";

  public static String projectName = "Faelle";

  private ConfigMetaCases config;

  public CaseImporter(ImporterManager mgr, ConfigMetaCases aConfig) throws ImportException {
    super(mgr, null, projectName, aConfig.getDataSource());
    config = aConfig;
  }

  @Override
  protected CatalogImporter createCatalogImporter() throws ImportException {
    return null; // no catalog entries for cases
  }

  private boolean getStorno() throws ImportException {
    if (config.stornoColumn != null) {
      String stornoString = getItem(config.stornoColumn);
      if (stornoString.equals("X")) {
        return true;
      }
    }
    return false;
  }

  private long getCaseID() throws ImportException {
    return getCaseID(config.caseIDColumn);
  }

  private long getPID() throws ImportException {
    return getPID(config.pidColumn);
  }

  private Timestamp getTimestamp() throws ImportException {
    String dateString;
    if (config.creationTimeStampColumn != null) {
      dateString = getItem(config.creationTimeStampColumn);
    } else {
      dateString = getItem(config.creationDateColumn);
      if (config.creationTimeColumn != null) {
        dateString += " " + getItem(config.creationTimeColumn);
      } else {
        dateString += " 00:00:00";
      }
    }
    Timestamp date = null;
    try {
      Date dateT = config.getFormat().parse(dateString);
      if (DataUtil.isTimeInValidRange(dateT)) {
        String timeStampString = TimeUtil.getSdfWithTimeSQLTimestamp().format(dateT);
        date = Timestamp.valueOf(timeStampString);
      }
    } catch (ParseException e) {
      ImportLogManager.warn(new ImportException(ImportExceptionType.DATA_PARSING_ERROR,
              "parsing datetime string error " + e.getMessage(), getProject()));
    }
    return date;
  }

  @Override
  protected void processImportInfoFileLine() throws ImportException, SQLException {
    long caseID = getCaseID();
    boolean storno = getStorno();
    long pid = getPID();
    String filename = tableElem.getName();

    // does the patient exist in the DWImportPIDs ?
    try {
      getPatientInfo(pid);
    } catch (PIDFilterException e) {
      // although the data of some PIDs should not be imported, it meta data should for sake of
      // process completeness
    }

    Timestamp creationDate = getTimestamp();
    creationDate = DataUtil.getSpecificTimeForCovariable(creationDate, caseID);

    String casetype = getItem(config.caseTypeColumn);
    switch (casetype) {
      case "1":
        casetype = "stationaer";
        break;
      case "2":
        casetype = "ambulant";
        break;
      case "3":
        casetype = "teilstationaer";
        break;
      default:
        break;
    }

    // if the case has a storno flag it still has to be imported for the completeness
    // of the import process. The case may in this case not have any more infos. If it
    // has they are deleted are the end of the update cycle
    getCaseManager().saveCase(pid, caseID, storno, casetype, filename, creationDate);
  }

  @Override
  protected void commit() throws ImportException {
    super.commit();
    try {
      getCaseManager().commit();
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, "", getProject(), e);
    }
  }

}
