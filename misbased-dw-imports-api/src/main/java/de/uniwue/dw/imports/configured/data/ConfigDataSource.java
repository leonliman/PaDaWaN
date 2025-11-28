package de.uniwue.dw.imports.configured.data;

import java.io.File;

import de.uniwue.dw.imports.IDataElemIterator;
import de.uniwue.dw.imports.ImportException;

public abstract class ConfigDataSource extends ConfigStructureElem {

  private static String default_encoding = "UTF-8";

  public String encoding;

  public ConfigDataSource(ConfigStructureElem aParent) {
    super(aParent);
  }

  public ConfigDataSource(ConfigStructureElem aParent, String anEncoding) {
    this(aParent);
    if (anEncoding == null) {
      encoding = default_encoding;
    } else {
      encoding = anEncoding;
    }
  }

  public abstract IDataElemIterator getDataElemsToProcess(String project, boolean doSort) throws ImportException;

  public abstract void addDataElemsToProcess(String project, File afile) throws ImportException;

}
