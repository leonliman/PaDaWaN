package de.uniwue.dw.imports.configured.data;

public class ConfigStructureElem {

  private ConfigStructureElem parent;

  public ConfigStructureElem(ConfigStructureElem aParent) {
    parent = aParent;
  }


  public ConfigStructureElem getParent() {
    return parent;
  }


  public ImportsConfig getRoot() {
    return parent.getRoot();
  }


  public ConfigDataSource getDataSource() {
    return parent.getDataSource();
  }

}
