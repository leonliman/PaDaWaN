package de.uniwue.dw.imports.configured.data;

import de.uniwue.dw.core.model.data.CatalogEntryType;

public class ConfigCatalogEntry extends ConfigCatalog {

  public String extID;

  public String name;

  public CatalogEntryType dataType = CatalogEntryType.Structure;

  public ConfigCatalogEntry(ConfigStructureElem aParent) {
    super(aParent);
  }

  // this constructor is for programmed importers that define their parent entry
  public ConfigCatalogEntry(String anExtID, String aName, String aProject) {
    super(null);
    setProject(aProject);
    name = aName;
    extID = anExtID;
  }

}
