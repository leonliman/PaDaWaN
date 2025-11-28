package de.uniwue.dw.imports.configured.data;


public class ConfigFilter extends ConfigStructureElem {

	public String filterColumn;

	public String filterValue;

  public boolean isRegex;

  public ConfigFilter(ConfigStructureElem aParent) {
    super(aParent);
  }


}
