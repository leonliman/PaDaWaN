package de.uniwue.dw.imports.configured.data;

import java.util.Iterator;

import de.uniwue.dw.imports.DataElem;
import de.uniwue.dw.imports.ImportException;

public class ConfigCatalogTable extends ConfigCatalog {

  public String extIDColumn;

  public String projectColumn;

  public String attrIDColumn;

  public String parentExtIDColumn;

  public String parentProjectColumn;

  public String parentAttrIDColumn;

  public String nameColumn;

  public String descriptionColumn;

  public String orderValueColumn;

  public String uniqueNameColumn;

  public String dataTypeColumn;

  public String stornoColumn;

  public String upperBoundColumn;

  public String lowerBoundColumn;

  public String unitColumn;

  public ConfigCatalogTable(ConfigStructureElem aParent) {
    super(aParent);
  }

  public Iterator<DataElem> getTable() throws ImportException {
    Iterator<DataElem> dataElemsToProcess = getDataSource().getDataElemsToProcess(getProject(),
            false);
    return dataElemsToProcess;
  }

}
