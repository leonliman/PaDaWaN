package de.uniwue.dw.imports.impl.base;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.uniwue.dw.imports.CatalogImporter;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.ImporterTable;
import de.uniwue.dw.imports.configured.data.ConfigMetaPatients;
import de.uniwue.dw.imports.manager.ImporterManager;

/**
 * The PID-metadata stores the age, sex and year of birth of patients. The year of birth is used to
 * determine the age of a patient for the admission time of his cases. The storno flag is used by
 * all importers to check of a new datum which itself is not stonro does not have to be imported
 * because its corresponding patient is storno.
 */
public class PatientImporter extends ImporterTable {

  public static final String PARAM_IMPORT_DIR = "import.dir.patients";

  public static String PROJECT_NAME = "Patienten";

  public ConfigMetaPatients config;

  public Pattern yobPattern;

  public PatientImporter(ImporterManager mgr, ConfigMetaPatients aConfig) throws ImportException {
    super(mgr, null, PROJECT_NAME, aConfig.getDataSource());
    config = aConfig;
    setRequiredCSVColumnHeaders(new String[] { aConfig.pidColumn });
    yobPattern = Pattern.compile(config.YOBRegex);
  }

  @Override
  protected CatalogImporter createCatalogImporter() throws ImportException {
    return null; // no catalog entries for pids
  }

  private long getPID() throws ImportException {
    return getPID(config.pidColumn);
  }

  private String getSex() throws ImportException {
    String sexString;
    sexString = getItem(config.sexColumn);
    if (sexString.equals("1")) {
      sexString = "M";
    } else if (sexString.equals("2")) {
      sexString = "W";
    } else if (sexString.equals("3")) {
      sexString = "T";
    }
    return sexString;
  }

  private int getYOB() throws ImportException {
    int yob = 0;
    String yobString = getItem(config.YOBColumn);
    if (yobString != null) {
      Matcher matcher = yobPattern.matcher(yobString);
      boolean matches = matcher.matches();
      if (matches && (matcher.groupCount() >= 1)) {
        String group = matcher.group(1);
        yob = Integer.valueOf(group);
      }
    }
    return yob;
  }

  private boolean getStorno() throws ImportException {
    boolean storno = false;
    String stornoString = getItem(config.stornoColumn);
    if (stornoString.equals("X")) {
      storno = true;
    }
    return storno;
  }

  @Override
  protected void processImportInfoFileLine() throws ImportException, SQLException {
    long pid = getPID();
    boolean storno = getStorno();
    int yob = getYOB();
    String sexString = getSex();
    String filename = tableElem.getName();
    getPatientManager().insert(pid, storno, sexString, yob, filename);
  }

  @Override
  protected void commit() throws ImportException {
    super.commit();
    try {
      getPatientManager().commit();
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, "", getProject(), e);
    }
  }

}
