package de.uniwue.dw.imports.configured.data;

import java.io.File;

import de.uniwue.dw.imports.DummyElemIterator;
import de.uniwue.dw.imports.IDataElemIterator;
import de.uniwue.dw.imports.ImportException;

public class ConfigDummyDataSource extends ConfigDataSource {

  public ConfigDummyDataSource(ConfigStructureElem aParent) {
    super(aParent);
  }


  public IDataElemIterator getDataElemsToProcess(String project, boolean doSort) throws ImportException {
    return new DummyElemIterator();
  }


  public void addDataElemsToProcess(String project, File afile) throws ImportException {
  }

}
