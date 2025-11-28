package de.uniwue.dw.imports.configured.data;

import java.io.File;

import de.uniwue.dw.imports.DBDataElemIterator;
import de.uniwue.dw.imports.IDataElemIterator;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.WrappedDBDataElemIterator;

public class ConfigDataSourceDatabase extends ConfigDataSource {

  public String selectString;

  public String sourceTable;

  public String alternativeMaxRecordIDTable;

  public String dbName;

  public String project;

  public String rowIDColumn;

  public String timestampColumnName;

  public String nameColumnName;

  public String contentColumnName;

  public ConfigDataSourceDatabase(ConfigStructureElem aParent) {
    super(aParent);
  }


  @Override
  public IDataElemIterator getDataElemsToProcess(String project, boolean doSort) throws ImportException {
    IDataElemIterator iter;
    if (getParent() instanceof ConfigDataXML || getParent() instanceof ConfigDataText) {
      iter = new DBDataElemIterator(this);
    } else {
      iter = new WrappedDBDataElemIterator(this);
    }
    return iter;
  }


  @Override
  public void addDataElemsToProcess(String project, File afile) throws ImportException {
    // TODO Auto-generated method stub
  }


  public String getProject() {
    if (project == null) {
      return ((ConfigData) getParent()).getProject();
    } else {
      return project;
    }
  }

}
