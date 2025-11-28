package de.uniwue.dw.imports.configured.data;

import java.text.SimpleDateFormat;
import java.util.HashMap;

public class ConfigDataColumn extends ConfigStructureElem {

  public String valueColumn;

  public String extID;

  public Boolean isExtIDColumn;

  public SimpleDateFormat timestampFormat;

  public HashMap<String, String> replacements = new HashMap<String, String>();

  public ConfigDataColumn(ConfigStructureElem aParent) {
    super(aParent);
  }

}
