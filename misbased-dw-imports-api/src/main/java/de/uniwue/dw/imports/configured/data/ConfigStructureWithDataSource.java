package de.uniwue.dw.imports.configured.data;

public class ConfigStructureWithDataSource extends ConfigStructureElem {

  protected ConfigDataSource dataSource;

  public ConfigStructureWithDataSource(ConfigStructureElem aParent) {
    super(aParent);
  }

  public void setDataSource(ConfigDataSource aDataSource) {
    dataSource = aDataSource;
  }
  
  public ConfigDataSource getDataSource() {
    if (dataSource != null) {
      return dataSource;
    } else {
      return getParent().getDataSource();
    }
  }


}
