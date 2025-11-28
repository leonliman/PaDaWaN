package de.uniwue.dw.imports.configured.data;

public class ConfigAcceptedID extends ConfigStructureElem {

  public String ID;
  
  public boolean isRoot = false;
  
  public boolean importText = false;
  
  public ConfigAcceptedID(ConfigStructureElem aParent) {
    super(aParent);
  }
}
