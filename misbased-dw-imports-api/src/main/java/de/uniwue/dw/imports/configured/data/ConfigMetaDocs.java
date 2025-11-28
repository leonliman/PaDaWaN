package de.uniwue.dw.imports.configured.data;

import java.text.SimpleDateFormat;

import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;

public class ConfigMetaDocs extends ConfigDataTable {

  public String timeStampColumn;

  public String timeStampFormat;

  public String timeColumn;

  public String dateColumn;

  public String docTypeColumn;

  private SimpleDateFormat format;

  public ConfigMetaDocs(ConfigStructureElem aParent) {
    super(aParent);
  }


  public ConfigMetaDocs(ConfigDataSource aDataSource, String aPIDColumn, String aCaseIDColumn, String aDocIDColumn,
          String aTimeStampColumn, String aTimeStampFormat, String aTimeColumn, String aTimeFormat, String aDateColumn,
          String aDateFormat, String aStornoColumn, String aDocTypeColumn) throws ImportException {
    this(null);
    dataSource = aDataSource;
    pidColumn = aPIDColumn;
    caseIDColumn = aCaseIDColumn;
    docIDColumn = aDocIDColumn;
    timeStampColumn = aTimeStampColumn;
    timeStampFormat = aTimeStampFormat;
    timeColumn = aTimeColumn;
    timeFormat = aTimeFormat;
    dateColumn = aDateColumn;
    dateFormat = aDateFormat;
    stornoColumn = aStornoColumn;
    docTypeColumn = aDocTypeColumn;
    getFormat();
  }


  public SimpleDateFormat getFormat() throws ImportException {
    if (format == null) {
      if ((timeStampFormat != null) && ((dateFormat != null) || (timeFormat != null))) {
        throw new ImportException(ImportExceptionType.DATA_MISMATCH,
                "Either a creation timestamp or a combination of date and time has to be given. Not both !");
      }
      if (timeStampFormat != null) {
        format = new SimpleDateFormat(timeStampFormat);
      } else {
        format = new SimpleDateFormat(dateFormat + " " + timeFormat);
      }
    }
    return format;
  }
}
