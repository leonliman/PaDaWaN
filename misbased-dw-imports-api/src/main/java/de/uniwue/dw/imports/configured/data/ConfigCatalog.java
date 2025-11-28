package de.uniwue.dw.imports.configured.data;

public abstract class ConfigCatalog extends ConfigStructureWithDataSource {

	private String project;
	
	public String projectName;

	private String parentProject;

	private String parentExtID;

  public int priority = 0;
  
  public ConfigCatalog(ConfigStructureElem aParent) {
    super(aParent);
  }

  public String getProject() {
    return project;
  }

  public void setProject(String project) {
    this.project = project;
  }

  public String getParentExtID() {
    return parentExtID;
  }

  public void setParentExtID(String parentExtID) {
    this.parentExtID = parentExtID;
  }

  public String getParentProject() {
    return parentProject;
  }

  public void setParentProject(String parentProject) {
    this.parentProject = parentProject;
  }

  
}
