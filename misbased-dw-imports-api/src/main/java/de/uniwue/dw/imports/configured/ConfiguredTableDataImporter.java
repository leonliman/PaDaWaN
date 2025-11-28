package de.uniwue.dw.imports.configured;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.exception.ExceptionUtils;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.ImporterTable;
import de.uniwue.dw.imports.configured.data.ConfigDataColumn;
import de.uniwue.dw.imports.configured.data.ConfigDataTable;
import de.uniwue.dw.imports.configured.data.ConfigFilter;
import de.uniwue.dw.imports.data.CaseInfo;
import de.uniwue.dw.imports.manager.ImporterManager;
import de.uniwue.misc.util.RegexUtil;
import de.uniwue.misc.util.TimeUtil;

public class ConfiguredTableDataImporter extends ImporterTable {

  private ConfigDataTable config;

  public SimpleDateFormat timeformat;

  public ConfiguredTableDataImporter(ImporterManager anImportManager, ConfigDataTable aCsvConfig)
          throws ImportException {
    super(anImportManager, aCsvConfig.getParentEntry(), aCsvConfig.getProject(), aCsvConfig.getDataSource());
    config = aCsvConfig;
    checkHeaders();
    checkCatalogEntries();
    initTimes();
    getCatalogImporter().useAbstractNameForRootProject = false;
  }


  private void checkCatalogEntries() throws ImportException {
    for (ConfigDataColumn aColumn : config.dataColumns) {
      try {
        if (!aColumn.isExtIDColumn) {
          getCatalogManager().getEntryByRefID(aColumn.extID, config.getProject());
        }
      } catch (SQLException e) {
        throw new ImportException(ImportExceptionType.SQL_ERROR, "", getProject(), e);
      }
    }
  }


  private void checkHeaders() throws ImportException {
    List<String> requiredHeaders = new ArrayList<String>();
    for (ConfigDataColumn aColumn : config.dataColumns) {
      if (aColumn.valueColumn != null) {
        requiredHeaders.add(aColumn.valueColumn);
      }
    }
    if (config.caseIDColumn != null) {
      requiredHeaders.add(config.caseIDColumn);
    }
    if (config.pidColumn != null) {
      requiredHeaders.add(config.pidColumn);
    }
    if (config.docIDColumn != null) {
      requiredHeaders.add(config.docIDColumn);
    }
    if (config.groupIDColumn != null) {
      requiredHeaders.add(config.groupIDColumn);
    }
    if (config.refIDColumn != null) {
      requiredHeaders.add(config.refIDColumn);
    }
    if (config.measureTimestampColumn != null) {
      requiredHeaders.add(config.measureTimestampColumn);
    }
    if (config.measureDateColumn != null) {
      requiredHeaders.add(config.measureDateColumn);
    }
    if (config.measureTimeColumn != null) {
      requiredHeaders.add(config.measureTimeColumn);
    }
    if (config.extIDColumn != null) {
      if (config.extIDColumn.contains("~+~")) {
        String[] extIDColumns = config.extIDColumn.split("\\~\\+\\~");
        for (String anExtIDColumn : extIDColumns) {
          if (!requiredHeaders.contains(anExtIDColumn)) {
            requiredHeaders.add(anExtIDColumn);
          }
        }
      } else if (config.extIDColumn.contains("~-~")) {
        String[] extIDColumns = config.extIDColumn.split("\\~\\-\\~");
        for (String anExtIDColumn : extIDColumns) {
          if (!requiredHeaders.contains(anExtIDColumn)) {
            requiredHeaders.add(anExtIDColumn);
          }
        }
      } else {
        requiredHeaders.add(config.extIDColumn);
      }
    }
    if (config.valueColumn != null) {
      requiredHeaders.add(config.valueColumn);
    }
    setRequiredCSVColumnHeaders(requiredHeaders.toArray(new String[0]));
  }


  private void initTimes() throws ImportException {
    if (config.timestampFormat != null) {
      timeformat = new SimpleDateFormat(config.timestampFormat);
    }
    if ((config.dateFormat != null) && (config.timeFormat != null)) {
      timeformat = new SimpleDateFormat(config.dateFormat + " " + config.timeFormat);
    }
    if ((config.dateFormat != null) && (config.timeFormat == null)) {
      timeformat = new SimpleDateFormat(config.dateFormat + " hhmmss");
    }
  }

  private Pattern extIDPattern;

  private String getExtID() throws ImportException {
    String result = "";
    if (config.extIDColumn.contains("~+~")) {
      // This branch exists for the Lab-Domain
      String[] split = config.extIDColumn.split("\\~\\+\\~");
      result = getItem(split[0]);
      String resultName = getItem(split[0]);
      String extID = getItem(split[0]);
      if (!split[1].trim().isEmpty()) {
        result += " " + getItem(split[1]);
        resultName += " (" + getItem(split[1]) + ")";
        extID += "_" + getItem(split[1]);
      }
      try {
        getCatalogManager().getOrCreateEntry(resultName, CatalogEntryType.Number, extID, getDomainRoot(), getProject());
      } catch (SQLException e) {
        throw new ImportException(ImportExceptionType.SQL_ERROR, "", getProject(), e);
      }
    } else if (config.extIDColumn.contains("~-~")) {
      // This branch exists for the Diag-Domain
      String[] split = config.extIDColumn.split("\\~\\-\\~");
      String[] ids = new String[split.length];
      for (int i = 0; i < split.length; i++) {
        ids[i] = getItem(split[i]);
      }
      result = chooseFirstNonEmpty(ids);
    } else {
      result = getItem(config.extIDColumn);
      if (config.extIDRegex != null) {
        if (extIDPattern == null) {
          extIDPattern = Pattern.compile(config.extIDRegex);
        }
        Matcher matcher = extIDPattern.matcher(result);
        if (matcher.find()) {
          result = matcher.group(1);
        } else {
          result = "";
        }
      }
    }
    return result;
  }


  private String chooseFirstNonEmpty(String[] codes) {
    for (int i = 0; i < codes.length; i++) {
      if (!codes[i].trim().isEmpty()) {
        return codes[i];
      }
    }
    return "";
  }


  private void getMeasureTimestamp(Identifier ident) throws ImportException {
    String timestampString = null;
    String sqlTimestampString = null;
    if (config.measureTimestampColumn != null) {
      timestampString = getItem(config.measureTimestampColumn);
    } else if (config.measureDateColumn != null) {
      String measureDateString = getItem(config.measureDateColumn);
      String measureTimeString = "000000";
      if (config.measureTimeColumn != null) {
        measureTimeString = getItem(config.measureTimeColumn);
      }
      timestampString = measureDateString + " " + measureTimeString;
    }
    if (timestampString != null) {
      Date opDate;
      try {
        opDate = timeformat.parse(timestampString);
      } catch (ParseException e) {
        throw new ImportException(ImportExceptionType.DATA_PARSING_ERROR, "", getProject(), e);
      }
      // there should not be a correction of obviously wrong timestamps !
      sqlTimestampString = TimeUtil.getSdfWithTimeSQLTimestamp().format(opDate);
      ident.measureTime = Timestamp.valueOf(sqlTimestampString);
    }
    if (ident.measureTime == null) {
      throw new ImportException(ImportExceptionType.DATA_MISMATCH, "No timestamp given.");
    }
  }


  private boolean getAcceptRow() throws ImportException {
    boolean acceptRow = true;
    for (ConfigFilter aFilter : config.filter) {
      String filterValue = getItem(aFilter.filterColumn);
      if (aFilter.isRegex) {
        acceptRow = filterValue.matches(aFilter.filterValue);
      } else {
        acceptRow = filterValue.equals(aFilter.filterValue);
      }
      if (!acceptRow) {
        return false;
      }
    }
    return acceptRow;
  }


  private Identifier getIdent() throws ImportException {
    long pid = 0, caseID = 0, docID = 0, groupID = 0, refID = 0;
    if (config.pidColumn != null) {
      String pidString = getItem(config.pidColumn);
      if (!pidString.trim().isEmpty() && RegexUtil.isNumber(pidString)) {
        pid = Long.valueOf(pidString);
      }
    }
    if (config.caseIDColumn != null) {
      String caseIDString = getItem(config.caseIDColumn);
      if (!caseIDString.trim().isEmpty() && RegexUtil.isNumber(caseIDString)) {
        caseID = Long.valueOf(caseIDString);
      }
    }
    if (config.docIDColumn != null) {
      String docIDString = getItem(config.docIDColumn);
      if (!docIDString.trim().isEmpty() && RegexUtil.isNumber(docIDString)) {
        docID = Long.valueOf(docIDString);
      }
    }
    if (config.groupIDColumn != null) {
      String groupIDString = getItem(config.groupIDColumn);
      if (!groupIDString.trim().isEmpty()) {
        groupID = Long.valueOf(groupIDString);
      }
    }
    if (config.refIDColumn != null) {
      String refIDString = getItem(config.refIDColumn);
      if (!refIDString.trim().isEmpty()) {
        docID = Long.valueOf(refIDString);
      }
    }
    Identifier ident = new Identifier(pid, caseID, docID, groupID, refID, this, config);
    getMeasureTimestamp(ident);
    return ident;
  }


  @Override
  protected void processImportInfoFileLine() throws ImportException {
    try {
      boolean acceptRow = getAcceptRow();
      if (!acceptRow) {
        return;
      }
      Identifier ident = getIdent();
      if (ident.isStorno(this)) {
        return;
      }
      processExtIDValue(ident);
      processColumns(ident);
      processSpecial(ident);
      processAllColumns(ident);
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, e);
    } catch (ParseException e) {
      throw new ImportException(ImportExceptionType.DATA_PARSING_ERROR, e);
    }
  }


  private void processAllColumns(Identifier ident) throws SQLException, ImportException, ParseException {
    if (config.allColumns != null) {
      for (String aColumnName : tableElem.getHeaderColumns()) {
        CatalogEntry anEntry = getCatalogManager().getEntryByRefID(aColumnName, getProject(), false);
        if (anEntry != null) {
          String value = getItem(aColumnName);
          if ((value != null) && !value.isEmpty()) {
            if (anEntry.getDataType() == CatalogEntryType.DateTime) {
              Timestamp timestamp = TimeUtil.parseTimestamp(config.allColumns.timestampFormat, value);
              processInfo(anEntry, timestamp, ident);
            } else {
              processInfo(anEntry, value, ident);
            }
          }
        }
      }
    }
  }


  private void processSpecial(Identifier ident) throws ImportException, SQLException {
    if (config.special != null) {
      if (config.special.equals("Alter")) {
        processAge(ident);
      }
      if (config.special.equals("Geschlecht")) {
        processSpecialString(ident, "Geschlecht", ident.patientInfo.sex);
      }
      if (config.special.equals("Aufnahme")) {
        processSpecialTimestamp(ident, "Aufnahme", ident.caseInfo.admission);
      }
      if (config.special.equals("Entlassung")) {
        processSpecialTimestamp(ident, "Entlassung", ident.caseInfo.discharge);
      }
      if (config.special.equals("Erste Bewegung")) {
        processSpecialTimestamp(ident, "Erste Bewegung", ident.caseInfo.firstMovement);
      }
      if (config.special.equals("Letzte Bewegung")) {
        processSpecialTimestamp(ident, "Letzte Bewegung", ident.caseInfo.lastMovement);
      }
      if (config.special.equals("PID")) {
        processSpecialString(ident, "PID", Long.toString(ident.pid));
      }
    }
  }


  private void processSpecialString(Identifier ident, String extID, String value) throws ImportException, SQLException {
    CatalogEntry alterEntry = getCatalogManager().getEntryByRefID(extID, config.getProject());
    if (value != null) {
      insert(alterEntry, ident.pid, value, ident.measureTime, ident.caseID, ident.docID);
    }
  }


  private void processSpecialTimestamp(Identifier ident, String extID, Timestamp value)
          throws SQLException, ImportException {
    if (value == null) {
      CaseInfo caseInfo = getCaseManager().getCase(ident.caseID);
      value = caseInfo.measureTime;
    }
    CatalogEntry timestampEntry = getCatalogManager().getEntryByRefID(extID, config.getProject());
    if (value != null) {
      insert(timestampEntry, ident.pid, value, ident.measureTime, ident.caseID, ident.docID, ident.groupID);
    }
  }

  private Calendar cal;

  private Calendar getCal() {
    if (cal == null) {
      cal = Calendar.getInstance();
    }
    return cal;
  }


  private void processAge(Identifier ident) throws ImportException, SQLException {
    CatalogEntry alterEntry = getCatalogManager().getEntryByRefID("Alter", config.getProject());
    if (ident.patientInfo.yob == 0) {
      // don't throw an exception. Some patient either have no YOB stored, some have explicitely
      // stored the year 0000.
      return;
    }
    getCal().setTimeInMillis(ident.measureTime.getTime());
    int alter = getCal().get(Calendar.YEAR) - ident.patientInfo.yob;
    insert(alterEntry, ident.pid, Integer.toString(alter), ident.measureTime, ident.caseID, ident.docID);
  }


  private void processExtIDValue(Identifier ident) throws ImportException, SQLException {
    if (config.extIDColumn != null) { // the valueColumn may be null
      String value;
      String extID = getExtID();
      if (extID.isEmpty()) {
        return;
      }
      String usedProject = getProject();
      if (config.projectColumn != null) {
        usedProject = getItem(config.projectColumn);
      }
      CatalogEntry anEntry = getCatalogManager().getEntryByRefID(extID, usedProject, false);
      if (anEntry == null) {
        if (getProject().equals("Diagnose")) {
          anEntry = getFixedICD10Code(extID);
        }
        if (getProject().equals("Procedures")) {
          anEntry = getFixedOPSCode(extID);
        }
        if (anEntry == null) {
          if (config.unknownExtIDEntryExtID != null) {
            anEntry = getCatalogManager().getEntryByRefID(config.unknownExtIDEntryExtID, usedProject);
          } else {
            throw new ImportException(ImportExceptionType.NO_CATALOG_ENTRY,
                    "Entry '" + extID + "' does not exists in project '" + usedProject + "'");
          }
        }
      }
      if (config.valueColumn != null) {
        value = getItem(config.valueColumn);
      } else if (anEntry.getDataType() == CatalogEntryType.DateTime) {
        value = ident.measureTime.toString();
      } else {
        value = "";
      }
      processInfo(anEntry, value, ident);
    }
  }


  private CatalogEntry getFixedOPSCode(String extID) throws SQLException {
    String adjustedCode = getHigherOPSLevel(extID);
    CatalogEntry anEntry = getCatalogManager().getEntryByRefID(adjustedCode, getProject(), false);
    if (anEntry == null) {
      adjustedCode = getHigherOPSLevel(adjustedCode);
      anEntry = getCatalogManager().getEntryByRefID(adjustedCode, getProject(), false);
    }
    return anEntry;
  }


  private CatalogEntry getFixedICD10Code(String extID) throws SQLException {
    String adjustedCode = getHigherICDLevel(extID);
    CatalogEntry anEntry = getCatalogManager().getEntryByRefID(adjustedCode, getProject(), false);
    if (anEntry == null) {
      adjustedCode = getHigherICDLevel(adjustedCode);
      anEntry = getCatalogManager().getEntryByRefID(adjustedCode, getProject(), false);
    }
    return anEntry;
  }


  private String getHigherOPSLevel(String code) {
    Pattern checkLevel5 = Pattern.compile("^(\\w-\\w{3}\\.\\w)\\w$");
    Matcher matchLevel5 = checkLevel5.matcher(code.trim());
    if (matchLevel5.find()) {
      return matchLevel5.group(1);
    }

    Pattern checkLevel4 = Pattern.compile("^(\\w-\\w{3})\\.\\w{0,2}$");
    Matcher matchLevel4 = checkLevel4.matcher(code.trim());
    if (matchLevel4.find()) {
      return matchLevel4.group(1);
    }

    Pattern checkLevel3 = Pattern.compile("^(\\w-\\w{2})\\w");
    Matcher matchLevel3 = checkLevel3.matcher(code.trim());
    if (matchLevel3.find()) {
      return matchLevel3.group(1);
    }

    return code;
  }


  private String getHigherICDLevel(String icd) {
    Pattern checkLevel5 = Pattern.compile("^(\\p{Alpha}\\d\\d\\.\\d)\\d$");
    Matcher matchLevel5 = checkLevel5.matcher(icd.trim());
    if (matchLevel5.find()) {
      return matchLevel5.group(1);
    }

    Pattern checkLevel4 = Pattern.compile("^(\\p{Alpha}\\d\\d\\.\\d)\\d$");
    Matcher matchLevel4 = checkLevel4.matcher(icd.trim());
    if (matchLevel4.find()) {
      return matchLevel4.group(1);
    }

    Pattern checkLevel3 = Pattern.compile("^(\\p{Alpha}\\d\\d)\\.\\d-{0,1}$");
    Matcher matchLevel3 = checkLevel3.matcher(icd.trim());
    if (matchLevel3.find()) {
      return matchLevel3.group(1);
    }

    Pattern checkBasic = Pattern.compile("^(\\p{Alpha}\\d\\d).*");
    Matcher matchBasic = checkBasic.matcher(icd.trim());
    if (matchBasic.find()) {
      return matchBasic.group(1);
    }
    return icd;
  }


  private void processColumns(Identifier ident) throws ImportException, SQLException, ParseException {
    for (ConfigDataColumn aColumn : config.dataColumns) {
      String value = "";
      if (aColumn.valueColumn != null) {
        value = getItem(aColumn.valueColumn);
      }
      if (aColumn.replacements.containsKey(value)) {
        value = aColumn.replacements.get(value);
      }
      CatalogEntry anEntry;
      if (aColumn.isExtIDColumn) {
        if ((value == null) || (value.isEmpty())) {
          continue;
        }
        anEntry = getCatalogManager().getEntryByRefID(value, config.getProject(), false);
      } else {
        anEntry = getCatalogManager().getEntryByRefID(aColumn.extID, config.getProject(), false);
      }
      if (anEntry == null) {
        throw new ImportException(ImportExceptionType.NO_CATALOG_ENTRY,
                "Entry '" + aColumn.extID + "' does not exists in project '" + config.getProject() + "'");
      }
      if (anEntry != null) {
        if (anEntry.getDataType() == CatalogEntryType.DateTime) {
          if (value != null) {
            Timestamp timestamp = TimeUtil.parseTimestamp(aColumn.timestampFormat, value);
            processInfo(anEntry, timestamp, ident);
          }
        } else {
          if (aColumn.isExtIDColumn) {
            processInfo(anEntry, "x", ident);
          } else {
            processInfo(anEntry, value, ident);
          }
        }
      }
    }
  }


  private void processInfo(CatalogEntry anEntry, Object value, Identifier ident) throws ImportException {
    if (config.stornoColumn != null) {
      String stornoString = getItem(config.stornoColumn);
      if (stornoString.equals("X")) {
        delete(anEntry, ident.pid, ident.caseID, ident.measureTime);
        return;
      }
    }
    try {
      if (value instanceof String) {
        insert(anEntry, ident.pid, (String) value, ident.measureTime, ident.caseID, ident.docID, ident.groupID);
      } else if (value instanceof Timestamp) {
        insert(anEntry, ident.pid, (Timestamp) value, ident.measureTime, ident.caseID, ident.docID, ident.groupID);
      } else if (value == null) {
        insert(anEntry, ident.pid, ident.measureTime, ident.caseID, ident.docID, ident.groupID);
      }
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, ExceptionUtils.getStackTrace(e), getProject(), e);
    }
  }

}
