package de.uniwue.dw.imports.configured.data;

import java.text.SimpleDateFormat;

import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;

public class ConfigMetaMovs extends ConfigDataTable {

  public String moveTypeColumn;

  public String fromTimeStampColumn;

  public String fromTimeStampFormat;

  public String fromDateColumn;

  public String fromDateFormat;

  public String fromTimeColumn;

  public String fromTimeFormat;

  public String endTimeStampColumn;

  public String endTimeStampFormat;

  public String endDateColumn;

  public String endDateFormat;

  public String endTimeColumn;

  public String endTimeFormat;

  private SimpleDateFormat fromFormat, endFormat;

  public ConfigMetaMovs(ConfigStructureElem aParent) {
    super(aParent);
  }


  public ConfigMetaMovs(ConfigDataSource aDataSource, String aCaseIDColumn, String aMoveTypecolumn,
          String aFromTimeStampColumn, String aFromTimeStampFormat, String aFromDateColumn, String aFromDateFormat,
          String aFromTimeColumn, String aFromTimeFormat, String anEndTimeStampColumn, String anEndTimeStampFormat,
          String anEndDateColumn, String anEndDateFormat, String anEndTimeColumn, String anEndTimeFormat,
          String aStornoColumn) {
    this(null);
    dataSource = aDataSource;
    caseIDColumn = aCaseIDColumn;
    moveTypeColumn = aMoveTypecolumn;
    fromTimeStampColumn = aFromTimeStampColumn;
    fromTimeStampFormat = aFromTimeStampFormat;
    fromDateColumn = aFromDateColumn;
    fromDateFormat = aFromDateFormat;
    fromTimeColumn = aFromTimeColumn;
    fromTimeFormat = aFromTimeFormat;
    endTimeStampColumn = anEndTimeStampColumn;
    endTimeStampFormat = anEndTimeStampFormat;
    endTimeColumn = anEndTimeColumn;
    endTimeFormat = anEndTimeFormat;
    endDateColumn = anEndDateColumn;
    endDateFormat = anEndDateFormat;
    stornoColumn = aStornoColumn;
  }


  public SimpleDateFormat getFromFormat() throws ImportException {
    if ((fromTimeStampFormat != null) && ((fromDateFormat != null) || (fromTimeFormat != null))) {
      throw new ImportException(ImportExceptionType.DATA_MISMATCH,
              "Either a 'from' timestamp or a combination of date and time has to be given. Not both !");
    }
    if (fromTimeStampFormat != null) {
      fromFormat = new SimpleDateFormat(fromTimeStampFormat);
    } else {
      fromFormat = new SimpleDateFormat(fromDateFormat + " " + fromTimeFormat);
    }
    return fromFormat;
  }


  public SimpleDateFormat getEndFormat() throws ImportException {
    if ((endTimeStampFormat != null) && ((endDateFormat != null) || (endTimeFormat != null))) {
      throw new ImportException(ImportExceptionType.DATA_MISMATCH,
              "Either a 'end' timestamp or a combination of date and time has to be given. Not both !");
    }
    if (endTimeStampFormat != null) {
      endFormat = new SimpleDateFormat(endTimeStampFormat);
    } else {
      endFormat = new SimpleDateFormat(endDateFormat + " " + endTimeFormat);
    }
    return endFormat;
  }
}
