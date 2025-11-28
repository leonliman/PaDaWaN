package de.uniwue.dw.exchangeFormat;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.group.Group;
import de.uniwue.dw.query.model.DWQueryConfig;
import de.uniwue.dw.query.model.IQueryKeys;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.dw.query.solr.preprocess.NestedDocumentIndexer;
import net.lingala.zip4j.ZipFile;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

public class EFImport {

  private EFImport() {
  }

  public static void doImport(ZipFile zipFile, boolean withAuthManagerReset, boolean withSolrIndexRebuild)
          throws ParseException, SQLException, IOException, IndexException {
    zipFile.extractAll(zipFile.getFile().getAbsoluteFile().getParent());
    File catalogFile = new File(zipFile.getFile().getAbsoluteFile().getParent(), EFExport.catalogFileName);
    File factsFile = new File(zipFile.getFile().getAbsoluteFile().getParent(), EFExport.factFileName);
    doImport(catalogFile, factsFile,
            new File(zipFile.getFile().getAbsoluteFile().getParent(), EFExport.factMetaDataFileName), null, false);
    if (!catalogFile.delete()) {
      System.err.println(catalogFile.getAbsolutePath() + " was extracted but could not be deleted automatically");
    }
    if (!factsFile.delete()) {
      System.err.println(factsFile.getAbsolutePath() + " was extracted but could not be deleted automatically");
    }
    if (withAuthManagerReset) {
      Group tempGroup = DwClientConfiguration.getInstance().getAuthManager().addGroup("tempGroup", 0, false, false);
      DwClientConfiguration.getInstance().getAuthManager().deleteGroup(tempGroup.getId());
    }

    if (withSolrIndexRebuild) {
      rebuildSolrIndex();
    }
  }

  public static void rebuildSolrIndex() throws IndexException {
    boolean priorDeleteIndexFlag = DWQueryConfig.hasToDeleteIndex();
    boolean priorIndexCatalogFlag = DWQueryConfig.hasToIndexCatalog();
    boolean priorIndexEntireCatalogFlag = DWQueryConfig.hasToIndexEntireCatalog();
    boolean priorCalculateCatalogCountsFlag = DWQueryConfig.hasToCalculateCatalogCount();
    boolean priorCleanCatalogIndexFlag = DWQueryConfig.cleanCatalogIndex();

    setIndexConfiguration(true, true, true, true, true);

    buildSolrIndex();

    setIndexConfiguration(priorDeleteIndexFlag, priorIndexCatalogFlag, priorIndexEntireCatalogFlag,
            priorCalculateCatalogCountsFlag, priorCleanCatalogIndexFlag);
  }

  private static void setIndexConfiguration(boolean deleteIndexFlag, boolean indexCatalogFlag,
          boolean indexEntireCatalogFlag, boolean calculateCatalogCountsFlag, boolean cleanCatalogIndexFlag) {
    DwClientConfiguration.getInstance().setProperty(IQueryKeys.PARAM_INDEXER_DELETE_INDEX,
            Boolean.toString(deleteIndexFlag));
    DwClientConfiguration.getInstance().setProperty(IQueryKeys.PARAM_INDEXER_INDEX_CATALOG,
            Boolean.toString(indexCatalogFlag));
    DwClientConfiguration.getInstance().setProperty(IQueryKeys.PARAM_INDEXER_INDEX_ENTIRE_CATALOG,
            Boolean.toString(indexEntireCatalogFlag));
    DwClientConfiguration.getInstance().setProperty(IQueryKeys.PARAM_INDEXER_CALCULATE_CATALOG_COUNT,
            Boolean.toString(calculateCatalogCountsFlag));
    DwClientConfiguration.getInstance().setProperty(IQueryKeys.PARAM_INDEXER_CLEAN_CATALOG_INDEX,
            Boolean.toString(cleanCatalogIndexFlag));
  }

  public static void buildSolrIndex() throws IndexException {
    NestedDocumentIndexer indexer = new NestedDocumentIndexer();
    indexer.update();
  }

  public static void doImport(String catalogFilePath, String factFilePath, String metaFilePath,
          String defaultProjectName)
          throws NumberFormatException, SQLException, IOException, ParseException {
    doImport(catalogFilePath, factFilePath, metaFilePath, defaultProjectName, false);
  }

  public static void doImport(String catalogFilePath, String factFilePath, String metaFilePath,
          String defaultProjectName, boolean withResultPrintedToConsole)
          throws NumberFormatException, SQLException, IOException, ParseException {
    File catalogFile = null;
    if (catalogFilePath != null) {
      catalogFile = new File(catalogFilePath);
    }
    File factFile = null;
    if (factFilePath != null) {
      factFile = new File(factFilePath);
    }
    File metaFile = null;
    if (metaFilePath != null) {
      metaFile = new File(metaFilePath);
    }
    doImport(catalogFile, factFile, metaFile, defaultProjectName, withResultPrintedToConsole);
  }

  public static void doImport(File catalogFile, File factFile, File metaFile,
          String defaultProjectName, boolean withResultPrintedToConsole)
          throws NumberFormatException, SQLException, IOException, ParseException {
    EFCatalogCSVProcessor catalogProcessor = null;
    if (catalogFile != null && catalogFile.exists()) {
      catalogProcessor = new EFCatalogCSVProcessor(catalogFile, defaultProjectName);
    }
    EFFactCSVProcessor factProcessor = null;
    if (factFile != null && factFile.exists()) {
      factProcessor = new EFFactCSVProcessor(factFile);
    }
    EFFactMetaDataCSVProcessor factMetaProcessor = new EFFactMetaDataCSVProcessor(metaFile);
    doImport(catalogProcessor, factProcessor, factMetaProcessor, defaultProjectName,
            withResultPrintedToConsole);
  }

  private static void doImport(EFCatalogCSVProcessor catalogProcessor,
          EFFactCSVProcessor factProcessor, EFFactMetaDataCSVProcessor factMetaProcessor,
          String defaultProjectName, boolean withResultPrintedToConsole)
          throws NumberFormatException, SQLException, IOException, ParseException {
    EFCatalogCSVProcessor curCatalogProcessor = catalogProcessor;
    if (curCatalogProcessor != null) {
      curCatalogProcessor.importCatalogFromCSV(withResultPrintedToConsole);
    } else {
      curCatalogProcessor = new EFCatalogCSVProcessor("", defaultProjectName);
    }
    if (factProcessor != null) {
      factProcessor.importFactsFromCSV(withResultPrintedToConsole, curCatalogProcessor, factMetaProcessor);
    }

    DwClientConfiguration.getInstance().getCatalogManager().commitAll();
    DwClientConfiguration.getInstance().getInfoManager().commit();

  }

}
