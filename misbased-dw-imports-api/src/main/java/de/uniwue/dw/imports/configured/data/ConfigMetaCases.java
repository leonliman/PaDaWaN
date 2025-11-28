package de.uniwue.dw.imports.configured.data;

import java.text.SimpleDateFormat;

import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;

public class ConfigMetaCases extends ConfigDataTable {

  public String creationTimeStampColumn;

  public String creationTimeStampFormat;

  public String creationTimeColumn;

  public String creationTimeFormat;

  public String creationDateColumn;

  public String creationDateFormat;

  public String caseTypeColumn;

  private SimpleDateFormat format;

  public ConfigMetaCases(ConfigStructureElem aParent) {
    super(aParent);
  }


  public ConfigMetaCases(ConfigDataSource aDataSource, String aPIDColum, String aCaseIDColumn,
          String aCreationTimestampColumn, String aCreationTimestampFormat, String aCreationTimeColumn,
          String aCreationTimeFormat, String aCreationDateColumn, String aCreationDateFormat, String aStornoColumn,
          String aCaseTypeColumn) throws ImportException {
    this(null);
    dataSource = aDataSource;
    pidColumn = aPIDColum;
    caseIDColumn = aCaseIDColumn;
    creationTimeStampColumn = aCreationTimestampColumn;
    creationTimeStampFormat = aCreationTimestampFormat;
    creationTimeColumn = aCreationTimeColumn;
    creationTimeFormat = aCreationTimeFormat;
    creationDateColumn = aCreationDateColumn;
    creationDateFormat = aCreationDateFormat;
    stornoColumn = aStornoColumn;
    caseTypeColumn = aCaseTypeColumn;
    getFormat();
  }


  public SimpleDateFormat getFormat() throws ImportException {
    if ((creationTimeStampFormat != null) && ((creationTimeFormat != null) || (creationDateFormat != null))) {
      throw new ImportException(ImportExceptionType.DATA_MISMATCH,
              "Either a creation timestamp or a combination of date and time has to be given. Not both !");
    }
    if (creationTimeStampFormat != null) {
      format = new SimpleDateFormat(creationTimeStampFormat);
    } else {
      if (creationTimeFormat != null) {
        format = new SimpleDateFormat(creationDateFormat + " " + creationTimeFormat);
      } else {
        format = new SimpleDateFormat(creationDateFormat + " hh:mm:ss");
      }
    }
    return format;
  }

}
