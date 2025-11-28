package de.uniwue.dw.imports.manager;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.model.manager.InfoManager;
import de.uniwue.dw.imports.DwImportsConfigValidator;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.Importer;
import de.uniwue.dw.imports.api.DWImportsConfig;
import de.uniwue.dw.imports.configured.ImporterConfigured;
import de.uniwue.dw.imports.configured.data.ConfigDataSourceCSVDir;
import de.uniwue.dw.imports.configured.data.ConfigMetaCases;
import de.uniwue.dw.imports.configured.data.ConfigMetaDocs;
import de.uniwue.dw.imports.configured.data.ConfigMetaMovs;
import de.uniwue.dw.imports.configured.data.ConfigMetaPatients;
import de.uniwue.dw.imports.impl.base.CaseImporter;
import de.uniwue.dw.imports.impl.base.DocImporter;
import de.uniwue.dw.imports.impl.base.MovImporter;
import de.uniwue.dw.imports.impl.base.PatientImporter;
import de.uniwue.dw.imports.mail.ImportMail;
import de.uniwue.dw.imports.manager.adapter.IRefIDAdapter;
import de.uniwue.misc.util.TimeUtil;

/**
 * The ImportManager manages big parts of the import process. It holds a list of all importers to be
 * run (allImporters). It holds references to several metadata-managers (caseManager, docManager,
 * patientManager) which can be accessed by the importers to access potentially needed information.
 */
public class ImporterManager {

  // Provides information about all cases
  public CaseManager caseManager;

  // Provides information about all docs
  public DocManager docManager;

  // Provides information about all patients
  public PatientManager patientManager;

  // with this manager data can be written an read to and from the fact table of the database
  public InfoManager infoManager;

  // with this manager data can be written an read to and from the catalog table of the database
  public CatalogManager catalogManager;

  private static String LOGNAME_IMPORTER_MANAGER = "DW-Import-Manager";

  public static Logger log = LogManager.getLogger(LOGNAME_IMPORTER_MANAGER);

  /**
   * allows to get new RefID (DocIDs) to link multiple attributes that belong together
   */
  public IRefIDAdapter refAdapter;

  /**
   * this is the list of all importers which have to be processed in an import run
   */
  private List<Importer> allImporters = new ArrayList<Importer>();

  private static String updateRunningParamName = "TheDwImperatorApp";

  public ImporterManager() throws SQLException, ParseException, IOException, ImportException {
    ImportLogManager.init();
    ImportLogManager.info(" - init importManager");
    ImportLogManager.init(); // because the log table could have been deleted
    infoManager = DwClientConfiguration.getInstance().getInfoManager();
    catalogManager = DwClientConfiguration.getInstance().getCatalogManager();
    patientManager = new PatientManager(this);
    caseManager = new CaseManager(this);
    docManager = new DocManager(this);
    refAdapter = DWImportsConfig.getInstance().getImportsAdapterFactory().createRefIDImportAdapter(infoManager);
    if (DWImportsConfig.getImportMetaInfos()) {
      instanciateMetaDataImporter();
    }
    instanciateImporters();
  }


  private void lockDW() throws Exception {
    DwClientConfiguration.getInstance().getSystemManager().lock(updateRunningParamName);
    String message = updateRunningParamName + " started by user: " + System.getProperty("user.name") + " on host: "
            + InetAddress.getLocalHost().getHostName();
    ImportLogManager.info(message);
  }


  private void unlockDW() throws SQLException {
    DwClientConfiguration.getInstance().getSystemManager().deleteParam(updateRunningParamName);
  }


  public List<Class<? extends Importer>> getActiveImporters() throws IOException {
    List<Class<? extends Importer>> activeImporters = new ArrayList<Class<? extends Importer>>();
    String regexFilter = DWImportsConfig.getImporterRegexFilter();
    String regexFilterExclude = DWImportsConfig.getImporterRegexFilterExclude();
    List<Class<? extends Importer>> importers = Importer.getAllImporters();
    for (Class<? extends Importer> clazz : importers) {
      if (clazz.getName().toLowerCase().matches(regexFilter.toLowerCase())
              && !clazz.getName().toLowerCase().matches(regexFilterExclude.toLowerCase())) {
        activeImporters.add(clazz);
      }
    }
    return activeImporters;
  }


  private void instanciateImporters() throws IOException {
    List<Class<? extends Importer>> importers = getActiveImporters();
    ImportLogManager.info("TheDwImperator...: importer plug-ins = " + Importer.getAllImporters().size() + "; active = "
            + importers.size() + " (filter: " + DWImportsConfig.getImporterRegexFilter() + ", exclude: "
            + DWImportsConfig.getImporterRegexFilterExclude() + ")");
    ArrayList<Importer> someImporter = new ArrayList<Importer>();
    for (Class<? extends Importer> clz : importers) {
      Constructor<? extends Importer> mth;
      try {
        mth = clz.getDeclaredConstructor(ImporterManager.class);
        if (mth == null) {
          throw new IllegalArgumentException();
        }
        Importer newInstance = mth.newInstance(this);
        someImporter.add(newInstance);
      } catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException
              | IllegalArgumentException | InvocationTargetException e) {
        ImportLogManager.warn(new ImportException(ImportExceptionType.IMPORTER_LOAD_ERROR,
                "Importer '" + clz.getName() + "' could not be loaded: " + e.getMessage()));
      }
    }
    someImporter.sort(new Comparator<Importer>() {
      @Override
      public int compare(Importer o1, Importer o2) {
        return o1.compareTo(o2);
      }
    });
    allImporters.addAll(someImporter);
  }


  private void instanciateMetaDataImporter() throws ImportException {
    if (DWImportsConfig.getImportPIDMetaInfos()
            && (DwClientConfiguration.getInstance().getParameter(PatientImporter.PARAM_IMPORT_DIR) != null)) {
      ConfigDataSourceCSVDir dataSource = new ConfigDataSourceCSVDir(
              new File(DWImportsConfig.getSAPImportDir(),
                      DwClientConfiguration.getInstance().getParameter(PatientImporter.PARAM_IMPORT_DIR)),
              "Windows-1252");
      ConfigMetaPatients config = new ConfigMetaPatients(dataSource, "ppatnr", "YOB", "(.*)", "gschl", "Storn");
      Importer patientImporter = new PatientImporter(this, config);
      allImporters.add(patientImporter);
    }
    if (DWImportsConfig.getImportCaseMetaInfos()
            && (DwClientConfiguration.getInstance().getParameter(CaseImporter.PARAM_IMPORT_DIR) != null)) {
      ConfigDataSourceCSVDir dataSource = new ConfigDataSourceCSVDir(new File(DWImportsConfig.getSAPImportDir(),
              DwClientConfiguration.getInstance().getParameter(CaseImporter.PARAM_IMPORT_DIR)), "Windows-1252");
      ConfigMetaCases config = new ConfigMetaCases(dataSource, "ppatnr", "pfalnr", null, null, null, null, "ERDAT",
              TimeUtil.sdf_withoutTimeString, "storn", "FALAR");
      Importer caseImporter = new CaseImporter(this, config);
      allImporters.add(caseImporter);
    }
    if (DWImportsConfig.getImportMovMetaInfos()
            && (DwClientConfiguration.getInstance().getParameter(MovImporter.PARAM_IMPORT_DIR) != null)) {
      ConfigDataSourceCSVDir dataSource = new ConfigDataSourceCSVDir(new File(DWImportsConfig.getSAPImportDir(),
              DwClientConfiguration.getInstance().getParameter(MovImporter.PARAM_IMPORT_DIR)), "Windows-1252");
      ConfigMetaMovs config = new ConfigMetaMovs(dataSource, "FALNR", "BEWTY", null, null, "BWIDT",
              TimeUtil.sdf_withoutTimeString, "BWIZT", TimeUtil.sdf_onlyTimeString, null, null, "BWEDT",
              TimeUtil.sdf_withoutTimeString, "BWEZT", TimeUtil.sdf_onlyTimeString, "STORN");
      Importer movImporter = new MovImporter(this, config);
      allImporters.add(movImporter);
    }
    if (DWImportsConfig.getImportDocMetaInfos()
            && (DwClientConfiguration.getInstance().getParameter(DocImporter.PARAM_IMPORT_DIR) != null)) {
      ConfigDataSourceCSVDir dataSource = new ConfigDataSourceCSVDir(new File(DWImportsConfig.getSAPImportDir(),
              DwClientConfiguration.getInstance().getParameter(DocImporter.PARAM_IMPORT_DIR)), "Windows-1252");
      ConfigMetaDocs config = new ConfigMetaDocs(dataSource, "PPATNR", "PFALNR", "pdoknr", null, null, "erstelltUm",
              TimeUtil.sdf_onlyTimeString, "erstelltAm", TimeUtil.sdf_withoutTimeString, "invalid?", "Dokumenttyp");
      Importer docImporter = new DocImporter(this, config);
      allImporters.add(docImporter);
    }
  }


  public void dropTablesIfNecessary() throws SQLException {
    try {
      if (DWImportsConfig.getDropAllOldTables() || DWImportsConfig.getDropFactTables()) {
        infoManager.truncateInfoTables();
        if (DWImportsConfig.getDropMetaDataTables()) {
          patientManager.truncatePatientTables();
          caseManager.truncateCaseTables();
          docManager.truncateDocTables();
        }
        ImportLogManager.truncateTables();
        DwClientConfiguration.getInstance().getAuthManager().truncateTables();
//        DWQueryConfig.getInstance().getQueryAdapterFactory().dropDataTables();
        DWImportsConfig.getDBImportLogManager().truncateTables();
      }
      if (DWImportsConfig.getDropAllOldTables()) {
        catalogManager.truncateCatalogTables();
      }
    } catch (SQLException e) {
      ImportLogManager.error(ImportExceptionType.SQL_ERROR, "error while dropping old tables", e);
      throw e;
    }
  }


  public void importCatalogsIfNecessary() {
    if (DWImportsConfig.getImportCatalogIfNecessary() && (allImporters.size() > 0)) {
      ImportLogManager.info("begin - importCatalogs");
      Set<String> importedImporter = catalogManager.getProjects();
      for (Importer anImporter : allImporters) {
        if (!importedImporter.contains(anImporter.getProject()) || anImporter.getReImportCatalogEveryTime()) {
          try {
            anImporter.doImportCatalog();
          } catch (ImportException e) {
            ImportLogManager.error(e);
          }
        }
      }
      if (DWImportsConfig.getSortCatalogRoot()) {
        try {
          catalogManager.sortChildrenAlphabeticallyNonRecursive(catalogManager.getRoot());
        } catch (SQLException e) {
          ImportLogManager.error(new ImportException(ImportExceptionType.SQL_ERROR, e));
        }
      }
      ImportLogManager.info("end - importCatalogs");
    }
  }


  public void initializeMetaDataManager() throws ImportException {
    try {
      patientManager.read();
      caseManager.read();
      if (DWImportsConfig.getLoadDocMetaData()) {
        docManager.read();
      }
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR,
              "error during non-lazy loading of meta data: " + e.getMessage());
    }
  }


  private void importInfos() {
    for (Importer anImporter : allImporters) {
      try {
        anImporter.doImportInfo();
      } catch (ImportException e) {
        ImportLogManager.error(e);
      } catch (NullPointerException e) {
        ImportException x = new ImportException(ImportExceptionType.NULLPOINTER, e.getMessage());
        x.setProject(anImporter.getProject());
        ImportLogManager.error(x);
      } catch (Exception e) {
        ImportException x = new ImportException(ImportExceptionType.UNKNOWN, e.getMessage());
        x.setProject(anImporter.getProject());
        ImportLogManager.error(x);
      }
    }
    if (DWImportsConfig.getImportConfigsDir() != null) {
      ImporterConfigured configuredImporter = new ImporterConfigured(this);
      try {
        configuredImporter.doImport();
      } catch (ImportException e) {
        ImportLogManager.error(e);
      }
    }
  }


  public void doImport() {
    try {
      try {
        dropTablesIfNecessary();
        if (DWImportsConfig.getLockForUpdate()) {
          lockDW();
        }
        validateImportIfNecessary();
        importCatalogsIfNecessary();
        importInfosIfNecessary();
        deleteStornoInfosIfNecessary();
        deleteInfosWOCatalogEntriesIfNecessary();
        if (DWImportsConfig.getSendMailOnSuccess() || DWImportsConfig.getSendMailOnFail()) {
          ImportMail.sendNotification();
        }
      } finally {
        if (DWImportsConfig.getLockForUpdate()) {
          unlockDW();
        }
      }
      ImportLogManager.info("TheDwImperatorApp finished.");
      dispose();
    } catch (Exception e) {
      // we should never end up here. If it still happens correct the error handling code below !
      ImportLogManager.error(ImportExceptionType.UNKNOWN, "unexpected error", e);
    }
  }


  public void dispose() {
    docManager.dispose();
    caseManager.dispose();
    patientManager.dispose();
    infoManager.dispose();
    catalogManager.dispose();
    refAdapter.dispose();
  }


  private void validateImportIfNecessary() throws ImportException {
    if (DWImportsConfig.getDoChecksBeforeStart()) {
      DwImportsConfigValidator validator = new DwImportsConfigValidator(this);
      validator.doCheck(allImporters);
    }
  }


  private void importInfosIfNecessary() {
    try {
      if (DWImportsConfig.getImportInfos()) {
        ImportLogManager.info("begin - importData");
        if (!DWImportsConfig.getLoadMetaDataLazy() && !(DWImportsConfig.getInstance().getDropFactTables()
                || DWImportsConfig.getInstance().getDropAllOldTables()
                || DWImportsConfig.getInstance().getDropMetaDataTables())) {
          initializeMetaDataManager();
        }
        importInfos();
        ImportLogManager.info("end - importData");
      }
    } catch (ImportException e) {
      ImportLogManager.error(e);
      return;
    }
  }


  private void deleteInfosWOCatalogEntriesIfNecessary() {
    if (DWImportsConfig.getDeleteInfosWOCatalogEntries()) {
      try {
        infoManager.deleteInfosWithoutCatalogEntry();
      } catch (SQLException e) {
        ImportLogManager.error(
                new ImportException(ImportExceptionType.SQL_ERROR, "error during get delete infos: " + e.getMessage()));
      }
    }
  }


  private void deleteStornoInfosIfNecessary() throws ImportException {
    try {
      if (DWImportsConfig.getDeleteStornosOfMetaData()) {
        ImportLogManager.info("Deleting stornos of patients");
        patientManager.deleteInfosOfStornoPIDs();
        ImportLogManager.info("Deleting stornos of cases");
        caseManager.deleteInfosOfStornoCases();
        ImportLogManager.info("Deleting stornos of docs");
        docManager.deleteInfosOfStornoDocs();
        ImportLogManager.info("Finished deleting stornos of metadata");
      }
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, "while delete storno info", e);
    }
  }
}
