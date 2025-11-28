package de.uniwue.dw.imports.configured;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.api.DWImportsConfig;
import de.uniwue.dw.imports.configured.data.ConfigCatalog;
import de.uniwue.dw.imports.configured.data.ConfigCatalogEntry;
import de.uniwue.dw.imports.configured.data.ConfigCatalogTable;
import de.uniwue.dw.imports.configured.data.ConfigData;
import de.uniwue.dw.imports.configured.data.ConfigDataTable;
import de.uniwue.dw.imports.configured.data.ConfigDataText;
import de.uniwue.dw.imports.configured.data.ConfigDataXML;
import de.uniwue.dw.imports.configured.data.ConfigMetaCases;
import de.uniwue.dw.imports.configured.data.ConfigMetaDocs;
import de.uniwue.dw.imports.configured.data.ConfigMetaGroupCase;
import de.uniwue.dw.imports.configured.data.ConfigMetaMovs;
import de.uniwue.dw.imports.configured.data.ConfigMetaPatients;
import de.uniwue.dw.imports.configured.data.ImportsConfig;
import de.uniwue.dw.imports.configured.xml.XMLAnalyzer;
import de.uniwue.dw.imports.configured.xml.XMLData;
import de.uniwue.dw.imports.impl.base.CaseImporter;
import de.uniwue.dw.imports.impl.base.DocImporter;
import de.uniwue.dw.imports.impl.base.MovImporter;
import de.uniwue.dw.imports.impl.base.PatientImporter;
import de.uniwue.dw.imports.manager.ImportLogManager;
import de.uniwue.dw.imports.manager.ImporterManager;

public class ImporterConfigured {

  private ImporterManager importManager;

  public ImporterConfigured(ImporterManager anImportManager) {
    importManager = anImportManager;
  }


  public void doImport() throws ImportException {
    ImportsConfig config = getMergedConfig();
    try {
      DWImportsConfig.getDBImportLogManager().initialize();
      processImportConfig(config);
    } catch (ImportException e) {
      ImportLogManager.error(e);
    }
  }


  private ImportsConfig getMergedConfig() throws ImportException {
    ImportsConfig mergedConfig = new ImportsConfig();
    List<ImportsConfig> configs = new ArrayList<ImportsConfig>();
    File importConfigsDir = DWImportsConfig.getImportConfigsDir();
    if (importConfigsDir == null) {
      // so nothing is done at all
      return mergedConfig;
    }
    if (!importConfigsDir.exists()) {
      throw new ImportException(ImportExceptionType.DATA_MISMATCH, "ImportConfigsDir does not exist");
    }
    if (!importConfigsDir.isDirectory()) {
      throw new ImportException(ImportExceptionType.DATA_MISMATCH, "ImportConfigsDir is no directory");
    }
    File[] configfiles = importConfigsDir.listFiles();
    if (configfiles == null) {
      throw new ImportException(ImportExceptionType.DATA_MISMATCH, "ImportConfigsDir is not accessible");
    }
    ImportConfigReader reader = new ImportConfigReader();
    for (File aConfig : configfiles) {
      if (aConfig.getName().matches(".*\\.xml")) {
        ImportsConfig config;
        try {
          config = reader.readImportConfig(aConfig);
          configs.add(config);
        } catch (IOException e) {
          throw new ImportException(ImportExceptionType.IO_ERROR, e);
        } catch (ParserConfigurationException e) {
          throw new ImportException(ImportExceptionType.DATA_PARSING_ERROR, e);
        } catch (SAXException e) {
          throw new ImportException(ImportExceptionType.DATA_PARSING_ERROR, e);
        }
      }
    }
    for (ImportsConfig aconfig : configs) {
      mergedConfig.merge(aconfig);
    }
    mergedConfig.catalogs.sort(new Comparator<ConfigCatalog>() {
      @Override
      public int compare(ConfigCatalog o1, ConfigCatalog o2) {
        return Integer.compare(o2.priority, o1.priority);
      }
    });
    mergedConfig.datas.sort(new Comparator<ConfigData>() {
      @Override
      public int compare(ConfigData o1, ConfigData o2) {
        return Integer.compare(o2.priority, o1.priority);
      }
    });
    return mergedConfig;
  }


  public void doChecks() throws ImportException {
    getMergedConfig();
  }


  private void processImportConfig(ImportsConfig config) throws ImportException {
    for (ConfigMetaPatients metaPatientConfig : config.metaPatients) {
      processMetaPatients(metaPatientConfig);
    }
    for (ConfigMetaCases metaCaseConfig : config.metaCases) {
      processMetaCases(metaCaseConfig);
    }
    for (ConfigMetaDocs metaDocConfig : config.metaDocs) {
      processMetaDoc(metaDocConfig);
    }
    for (ConfigMetaMovs metaMovConfig : config.metaMovs) {
      processMetaMovs(metaMovConfig);
    }
    for (ConfigMetaGroupCase metaGroupConfig : config.metaGroupCase) {
      processMetaGroupCase(metaGroupConfig);
    }
    for (ConfigCatalog catalogConfig : config.catalogs) {
      processCatalogConfig(catalogConfig);
    }
    for (ConfigData dataConfig : config.datas) {
      processDataConfig(dataConfig);
    }
  }


  private void processMetaGroupCase(ConfigMetaGroupCase metaGroupCase) {
    // TODO Auto-generated method stub

  }


  private void processMetaPatients(ConfigMetaPatients config) throws ImportException {
    PatientImporter importer = new PatientImporter(importManager, config);
    importer.doImportInfo();
    // why should the manager be forced to reread his info are the import ?
    // if (!DWImportsConfig.getLoadMetaDataLazy()) {
    // try {
    // importManager.patientManager.clear();
    // importManager.patientManager.read();
    // } catch (SQLException e) {
    // throw new ImportException(ImportExceptionType.SQL_ERROR, "", "MetaPIDs", e);
    // }
    // }
  }


  private void processMetaCases(ConfigMetaCases config) throws ImportException {
    CaseImporter importer = new CaseImporter(importManager, config);
    importer.doImportInfo();
    // why should the manager be forced to reread his info are the import ?
    // try {
    // importManager.caseManager.read();
    // } catch (SQLException e) {
    // throw new ImportException(ImportExceptionType.SQL_ERROR, "", "MetaCases", e);
    // }
  }


  private void processMetaDoc(ConfigMetaDocs config) throws ImportException {
    DocImporter importer = new DocImporter(importManager, config);
    importer.doImportInfo();
    // why should the manager be forced to reread his info are the import ?
    // try {
    // importManager.docManager.read();
    // } catch (SQLException e) {
    // throw new ImportException(ImportExceptionType.SQL_ERROR, "", "MetaDocs", e);
    // }
  }


  private void processMetaMovs(ConfigMetaMovs config) throws ImportException {
    MovImporter importer = new MovImporter(importManager, config);
    importer.doImportInfo();
    // why should the manager be forced to reread his info are the import ?
    // try {
    // importManager.caseManager.read();
    // } catch (SQLException e) {
    // throw new ImportException(ImportExceptionType.SQL_ERROR, "", "MetaMovs", e);
    // }
  }


  private void processDataConfig(ConfigData config) throws ImportException {
    if (config instanceof ConfigDataTable) {
      ConfiguredTableDataImporter importer = new ConfiguredTableDataImporter(importManager, (ConfigDataTable) config);
      importer.doImportInfo();
    }
    if (config instanceof ConfigDataText) {
      ConfiguredTextDataImporter importer = new ConfiguredTextDataImporter(importManager, (ConfigDataText) config);
      importer.doImportInfo();
    }
    if (config instanceof ConfigDataXML) {
      importDataXML((ConfigDataXML) config);
    }
  }


  private void importDataXML(ConfigDataXML config) throws ImportException {
    if (config.getRoot().version.equals("0.3")) {
      ConfiguredXMLDataImporter2 importer = new ConfiguredXMLDataImporter2(importManager, config);
      importer.doImportInfo();
    } else {
      ConfiguredXMLDataImporter importer = new ConfiguredXMLDataImporter(importManager, config);
      if (config.idCSVFile != null) {
        XMLAnalyzer analyzer = new XMLAnalyzer();
        try {
          XMLData xmlData = analyzer.read(new File(DWImportsConfig.getImportConfigsDir(), config.idCSVFile));
          xmlData.createCatalog(importManager.catalogManager, config, importer.getDomainRoot());
          importer.supportingCatalog = xmlData;
        } catch (IOException e) {
          throw new ImportException(ImportExceptionType.IO_ERROR, config.getProject(), e);
        } catch (SQLException e) {
          throw new ImportException(ImportExceptionType.SQL_ERROR, config.getProject(), e);
        }
      }
      importer.doImportInfo();
    }
  }


  private void processCatalogConfig(ConfigCatalog config) throws ImportException {
    if (config instanceof ConfigCatalogTable) {
      ConfiguredTableCatalogImporter importer = new ConfiguredTableCatalogImporter((ConfigCatalogTable) config,
              importManager);
      importer.doImport();
    }
    if (config instanceof ConfigCatalogEntry) {
      processCreationOfSingleCatalogEntries((ConfigCatalogEntry) config);
    }
  }


  private void processCreationOfSingleCatalogEntries(ConfigCatalogEntry config) throws ImportException {
    try {
      String parentProject = config.getParentProject();
      if (parentProject == null) {
        parentProject = config.getProject();
      }
      if (!config.getParentProject().isEmpty()) {
        parentProject = config.getParentProject();
      }
      String parentExtID = config.getParentExtID();
      if (!config.getParentExtID().isEmpty()) {
        parentExtID = config.getParentExtID();
      }
      CatalogEntry parentEntry = importManager.catalogManager.getEntryByRefID(parentExtID, parentProject);
      if (parentEntry == null) {
        throw new ImportException(ImportExceptionType.NO_CATALOG_ENTRY, "parent entry with exitID '"
                + config.getParentExtID() + "' and project '" + config.getParentProject() + "' does not exist.");
      }
      importManager.catalogManager.getOrCreateEntry(config.name, config.dataType, config.extID, parentEntry,
              config.getProject());
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, "", config.getProject(), e);
    }
  }

}
