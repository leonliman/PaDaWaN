package de.uniwue.dw.imports;

import de.uniwue.dw.imports.configured.data.ConfigDataSource;

public abstract class DataElem implements IDataElem {

  protected ConfigDataSource dataSource;

  public DataElem(ConfigDataSource aDataSource) {
    dataSource = aDataSource;
  }
  
  protected ConfigDataSource getDataSource() {
    return dataSource;
  }
  
}
