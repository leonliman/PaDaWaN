package de.uniwue.dw.imports.configured.data;

import java.util.ArrayList;
import java.util.List;

public class ConfigDataTable extends ConfigData {

  public String pidColumn;

  public String caseIDColumn;

  public String docIDColumn;

  public String groupIDColumn;

  public String refIDColumn;

  public String measureTimestampColumn;

  public String timestampFormat;

  public String measureDateColumn;

  public String dateFormat;

  public String measureTimeColumn;

  public String timeFormat;

  public String stornoColumn;

  public List<ConfigFilter> filter = new ArrayList<ConfigFilter>();

  public String unknownExtIDEntryExtID;

  // this is for configuration 1
  public String extIDColumn;

  public String extIDRegex;

  public String projectColumn;

  public String valueColumn;

  // this is for configuration 2
  public List<ConfigDataColumn> dataColumns = new ArrayList<ConfigDataColumn>();

  public ConfigAllColumns allColumns;

  // this is for configuration 3
  public String special;

  public ConfigDataTable(ConfigStructureElem aParent) {
    super(aParent);
  }

}
