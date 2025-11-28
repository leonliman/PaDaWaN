package de.uniwue.dw.imports.configured.data;

public abstract class ConfigData extends ConfigStructureWithDataSource {

  private String project;

  private ConfigCatalogEntry parentEntry = new ConfigCatalogEntry(this);

  public boolean useMetaData = true;

  public int priority;

  public ConfigData(ConfigStructureElem aParent) {
    super(aParent);
  }

  public String getProject() {
    return project;
  }

  public void setProject(String project) {
    this.project = project;
  }

  public ConfigCatalogEntry getParentEntry() {
    return parentEntry;
  }

  public void setParentEntry(ConfigCatalogEntry anEntry) {
    parentEntry = anEntry;
  }

}
