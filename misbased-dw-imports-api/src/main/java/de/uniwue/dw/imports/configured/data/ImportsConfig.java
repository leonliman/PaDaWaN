package de.uniwue.dw.imports.configured.data;

import java.util.ArrayList;
import java.util.List;

public class ImportsConfig extends ConfigStructureWithDataSource {

  public List<ConfigData> datas = new ArrayList<ConfigData>();

  public List<ConfigCatalog> catalogs = new ArrayList<ConfigCatalog>();

  public List<ConfigMetaPatients> metaPatients = new ArrayList<ConfigMetaPatients>();

  public List<ConfigMetaCases> metaCases = new ArrayList<ConfigMetaCases>();

  public List<ConfigMetaDocs> metaDocs = new ArrayList<ConfigMetaDocs>();

  public List<ConfigMetaMovs> metaMovs = new ArrayList<ConfigMetaMovs>();

  public List<ConfigMetaGroupCase> metaGroupCase = new ArrayList<ConfigMetaGroupCase>();

  public String version;
  
  public ImportsConfig(ConfigStructureElem aParent) {
    super(aParent);
  }

  public ImportsConfig() {
    super(null);
  }

  public void merge(ImportsConfig anotherConfig) {
    datas.addAll(anotherConfig.datas);
    catalogs.addAll(anotherConfig.catalogs);
    metaPatients.addAll(anotherConfig.metaPatients);
    metaCases.addAll(anotherConfig.metaCases);
    metaDocs.addAll(anotherConfig.metaDocs);
    metaMovs.addAll(anotherConfig.metaMovs);
    metaGroupCase.addAll(anotherConfig.metaGroupCase);
  }

  public ImportsConfig getRoot() {
    return this;
  }
  
  public ConfigDataSource getDataSource() {
    return dataSource;
  }
  

}
