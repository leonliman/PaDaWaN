package de.uniwue.dw.imports.configured.data;

public class ConfigMetaPatients extends ConfigDataTable {

  public String YOBColumn;

  public String YOBRegex;

  public String sexColumn;

  public ConfigMetaPatients(ConfigStructureElem aParent) {
    super(aParent);
  }


  public ConfigMetaPatients(ConfigDataSource aDataSource, String aPIDColumn, String aYOBColumn, String aYOBRegex,
          String aSexColumn, String aStornoColumn) {
    this(null);
    dataSource = aDataSource;
    pidColumn = aPIDColumn;
    YOBColumn = aYOBColumn;
    YOBRegex = aYOBRegex;
    sexColumn = aSexColumn;
    stornoColumn = aStornoColumn;
  }

}
