package de.uniwue.dw.imports;

import java.io.File;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ServiceLoader;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.model.manager.InfoIterator;
import de.uniwue.dw.core.model.manager.InfoManager;
import de.uniwue.dw.core.model.manager.UniqueNameUtil;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.api.DWImportsConfig;
import de.uniwue.dw.imports.configured.data.ConfigCatalogEntry;
import de.uniwue.dw.imports.configured.data.ConfigDataSource;
import de.uniwue.dw.imports.configured.data.ConfigDataSourceDatabase;
import de.uniwue.dw.imports.data.CaseInfo;
import de.uniwue.dw.imports.data.DocInfo;
import de.uniwue.dw.imports.data.PatientInfo;
import de.uniwue.dw.imports.manager.CaseManager;
import de.uniwue.dw.imports.manager.DocManager;
import de.uniwue.dw.imports.manager.ImportLogManager;
import de.uniwue.dw.imports.manager.ImporterManager;
import de.uniwue.dw.imports.manager.PatientManager;
import de.uniwue.dw.imports.manager.adapter.IRefIDAdapter;

/*
 * Abstract class that describes the general architecture of each importer. Adds useful basic
 * functionality for each importer.
 */
public abstract class Importer implements Comparable<Importer> {

  private ImporterManager importerManager;

  protected CatalogImporter catalogImporter;

  protected ConfigCatalogEntry parentEntry;

  private String project;

  private int priority;

  private boolean reImportCatalogEveryTime = false;

  protected ConfigDataSource dataSource;

  private HashMap<String, Long> refsOfCurrentImport = null;

  public Importer(ImporterManager mgr, ConfigCatalogEntry aParentEntry, String projectName,
          ConfigDataSource aDataSource) throws ImportException {
    dataSource = aDataSource;
    priority = 10;
    importerManager = mgr;
    project = projectName;
    parentEntry = aParentEntry;
    try {
      catalogImporter = createCatalogImporter();
    } catch (ImportException e) {
      e.setProject(getProject());
      throw e;
    }
  }


  public boolean isFirstFactDataImport() {
    if (DWImportsConfig.getTreatAsInitialImport() || DWImportsConfig.getDropAllOldTables()
            || DWImportsConfig.getDropFactTables()) {
      return true;
    } else {
      return false;
    }
  }


  public ConfigCatalogEntry getParentEntry() {
    return parentEntry;
  }


  public static List<Class<? extends Importer>> getAllImporters() {
    ServiceLoader<Importer> loader = ServiceLoader.load(Importer.class);
    List<Class<? extends Importer>> ls = new ArrayList<Class<? extends Importer>>();
    for (Importer i : loader) {
      Class<? extends Importer> clazz = i.getClass();
      ls.add(clazz);
    }
    return ls;
  }


  public boolean getReImportCatalogEveryTime() {
    return reImportCatalogEveryTime;
  }


  protected void setReImportCatalogEveryTime(boolean val) {
    reImportCatalogEveryTime = val;
  }


  public void setPriority(int newPriority) {
    priority = newPriority;
  }


  @Override
  public int compareTo(final Importer o) {
    int result = Integer.compare(this.priority, o.priority);
    if (result == 0) {
      result = this.getClass().getName().compareTo(o.getClass().getName());
    }
    return result;
  }


  /**
   * zero arguments constructor needed for plugin functionality
   */
  public Importer() {
  }


  /**
   * This method is called for all importers at once as no importer should be started if there
   * exists a problem with another importer. It would only cause mischief if the whole import would
   * stop during the night. Problems should be detected at the very beginning.
   */
  public void checkForImport() throws ImportException {
    // if (getMainImportDir() != null && !getMainImportDir().exists()) {
    // throw new
    // ImportException(ImportExceptionType.IMPORT_DIR_NONE_EXISTANT,
    // "dir " + getMainImportDir() + " not found", getProject());
    // }
  }


  protected IDataElemIterator getFilesToProcess() throws ImportException {
    return dataSource.getDataElemsToProcess(getProject(), true);
  }


  protected void addFileToProcess(File afile) throws ImportException {
    dataSource.addDataElemsToProcess(getProject(), afile);
  }


  public void doImportInfo() throws ImportException {
    String sourceProject = project;
    if (dataSource instanceof ConfigDataSourceDatabase) {
      sourceProject += "(" + ((ConfigDataSourceDatabase) dataSource).project + ")";
    }
    ImportLogManager.info(new ImportException(ImportExceptionType.IMPORT_PROCESS,
            "started data import of project '" + sourceProject + "'.", project));
    Date d1 = new Date();
    try {
      importInfos();
    } catch (ImportException e) {
      // the project should always be already set but it doesn't hurt to do it again
      e.setProject(getProject());
      throw e;
    }
    Date d2 = new Date();
    double diff = d2.getTime() - d1.getTime();
    DecimalFormat df = new DecimalFormat("###########0.00");
    String timeTakenString = " in seconds: " + df.format(diff / 1000d) + ", in minutes: "
            + df.format(diff / (60d * 1000d)) + ", in hours: " + df.format(diff / (60d * 60d * 1000d));
    ImportLogManager.info(new ImportException(ImportExceptionType.IMPORT_PROCESS,
            "finished data import of project '" + project + "'." + timeTakenString, project));
  }


  /**
   * This method is intended to be called after the import of this domain is finished
   */
  protected void runAfterImport() throws ImportException {
    if (DWImportsConfig.getParallelizeImport()) {
      getInfoManager().infoAdapter.setParallelizeInsert(false);
    }
  }


  /**
   * This method is intended to be called before the import of this domain is started
   */
  protected void runBeforeImport() throws ImportException {
    if (DWImportsConfig.getParallelizeImport()) {
      getInfoManager().infoAdapter.setParallelizeInsert(true);
    }
  }


  /**
   * This abstract method is intended to be implemented in the derived importer class (e.g. CSV,
   * FullFile). The main import work is done in this method.
   */
  public abstract void importInfos() throws ImportException;


  /**
   * creates the specific catalog importer for this type of importer
   */
  protected CatalogImporter createCatalogImporter() throws ImportException {
    return new CatalogImporter(this);
  }


  public Long addUsedRefID(int attrId, long pid, long caseid, Timestamp measureDate, Long refid)
          throws ImportException {
    Long toRet = null;
    if (refsOfCurrentImport == null) {
      refsOfCurrentImport = new HashMap<String, Long>();
    }
    String key = Integer.toString(attrId) + Long.toString(pid) + Long.toString(caseid) + measureDate.toString();

    refsOfCurrentImport.put(key, refid);

    return toRet;
  }


  public CatalogEntry getEntryByRefID(String refID, String aProject) throws ImportException {
    try {
      return getCatalogManager().getEntryByRefID(refID, aProject);
    } catch (SQLException e) {
      ImportException tmp = new ImportException(ImportExceptionType.SQL_ERROR,
              "error while getting catalog entry by ref id '" + refID + "'", e);
      tmp.setProject(getProject());
      throw tmp;
    }
  }


  protected CaseManager getCaseManager() {
    return importerManager.caseManager;
  }


  protected PatientManager getPatientManager() {
    return importerManager.patientManager;
  }


  protected CatalogManager getCatalogManager() {
    return importerManager.catalogManager;
  }


  // the InfoManager is no langer accessible from the outside because the PIDs are now filtered with
  // the PIDFilter and all access to the InfoManager is now channeled via forwarding methods here
  // within the importer to do the filtering
  private InfoManager getInfoManager() {
    return importerManager.infoManager;
  }


  protected DocManager getDocManager() {
    return importerManager.docManager;
  }


  public CatalogEntry getDomainRoot() throws ImportException {
    return catalogImporter.getDomainRoot();
  }


  public String getProject() {
    return project;
  }


  protected void commit() throws ImportException {
    try {
      getInfoManager().commit();
    } catch (SQLException e) {
      ImportException tmp = new ImportException(ImportExceptionType.SQL_ERROR, "error during commit", e);
      tmp.setProject(getProject());
      throw tmp;
    }
  }


  public void doImportCatalog() throws ImportException {
    ImportLogManager.info(new ImportException(ImportExceptionType.IMPORT_PROCESS,
            "started catalog import of project " + project + "'.", project));
    Date d1 = new Date();
    try {
      importCatalog();
      commit();
    } catch (Exception e) {
      ImportException ie;
      if (e instanceof ImportException) {
        ie = (ImportException) e;
      } else {
        ie = new ImportException(ImportExceptionType.IMPORT_PROCESS, e);
      }
      ie.setProject(getProject());
      throw ie;
    }
    Date d2 = new Date();
    double diff = d2.getTime() - d1.getTime();
    DecimalFormat df = new DecimalFormat("###########0.00");
    ImportLogManager.info(new ImportException(ImportExceptionType.IMPORT_PROCESS,
            "finished catalog import of project '" + project + "'.", project));
    ImportLogManager.info(new ImportException(ImportExceptionType.IMPORT_PROCESS,
            "imported catalog of project '" + project + "' in " + df.format(diff / 1000d) + " seconds.", project));
  }


  /**
   * This method runs a defined external Catalog importer class defined by createCatalogImporter or,
   * alternatively, can be overwritten by the specific importer
   * 
   * @throws ImportException
   */
  protected void importCatalog() throws ImportException {
    if (catalogImporter != null) {
      catalogImporter.doImport();
    }
  }


  public PatientInfo getPatientInfo(long PID) throws ImportException {
    return getPatientManager().getPatient(PID);
  }


  public DocInfo getDocInfo(long docID) throws ImportException {
    return getDocManager().getDoc(docID);
  }


  public CaseInfo getCaseInfo(long caseID) throws ImportException {
    return getCaseManager().getCase(caseID);
  }


  protected CatalogEntry getOrCreateEntry(String name, CatalogEntryType type, String extID) throws ImportException {
    return getOrCreateEntry(name, type, extID, getCatalogImporter().getDomainRoot(), 0,
            UniqueNameUtil.getUniqueName(name, getProject()), "");
  }


  protected CatalogEntry getOrCreateEntry(String name, CatalogEntryType type, String extID, double orderValue)
          throws ImportException {
    return getOrCreateEntry(name, type, extID, getCatalogImporter().getDomainRoot(), orderValue,
            UniqueNameUtil.getUniqueName(name, getProject()), "");
  }


  protected CatalogEntry getOrCreateEntry(String name, CatalogEntryType type, String extID, String uniqueName)
          throws ImportException {
    return getOrCreateEntry(name, type, extID, getCatalogImporter().getDomainRoot(), 0, uniqueName, "");
  }


  protected CatalogEntry getOrCreateEntry(String name, CatalogEntryType type, String extID, CatalogEntry aParentEntry,
          String uniqueName) throws ImportException {
    return getOrCreateEntry(name, type, extID, aParentEntry, 0, uniqueName, "");
  }


  protected CatalogEntry getOrCreateEntry(String name, CatalogEntryType type, String extID, CatalogEntry aParentEntry)
          throws ImportException {
    return getOrCreateEntry(name, type, extID, aParentEntry, 0, UniqueNameUtil.getUniqueName(name, project), "");
  }


  protected CatalogEntry getOrCreateEntry(String name, CatalogEntryType type, String extID, CatalogEntry aParentEntry,
          double orderValue) throws ImportException {
    return getOrCreateEntry(name, type, extID, aParentEntry, orderValue, UniqueNameUtil.getUniqueName(name, project),
            "");
  }


  protected CatalogEntry getOrCreateEntry(String name, CatalogEntryType type, String extID, CatalogEntry aParentEntry,
          String uniqueName, String aDescription) throws ImportException {
    return getOrCreateEntry(name, type, extID, aParentEntry, 0, uniqueName, aDescription);
  }


  /*
   * Get or create the create and do a commit
   */
  protected CatalogEntry getOrCreateEntry(String name, CatalogEntryType type, String extID, CatalogEntry aParentEntry,
          double orderValue, String uniqueName, String aDescription) throws ImportException {
    try {
      CatalogEntry entry = getCatalogManager().getOrCreateEntry(name, type, extID, aParentEntry, orderValue,
              getProject(), uniqueName, aDescription);
      return entry;
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, "", getProject(), e);
    }
  }


  public IRefIDAdapter getRefAdapter() {
    return importerManager.refAdapter;
  }


  public static File calculateMainImportDir(String configParamForPossiblyRelativeImportDir) throws ImportException {
    if (configParamForPossiblyRelativeImportDir == null) {
      throw new ImportException(ImportExceptionType.IMPORTER_LOAD_ERROR, "ImportDir parameter not given.");
    }
    String possiblyRelativeImportDir = DwClientConfiguration.getInstance()
            .getParameter(configParamForPossiblyRelativeImportDir);
    if (possiblyRelativeImportDir == null) {
      throw new ImportException(ImportExceptionType.IMPORTER_LOAD_ERROR,
              "ImportDir parameter not contained in params.");
    }
    if (new File(possiblyRelativeImportDir).isAbsolute()) {
      return new File(possiblyRelativeImportDir);
    }
    return new File(DWImportsConfig.getSAPImportDir(), possiblyRelativeImportDir);
  }


  protected CatalogImporter getCatalogImporter() {
    return catalogImporter;
  }

  // InfoManager access


  public long insert(CatalogEntry anEntry, long pid, Date value, Timestamp measureTime, long caseID)
          throws SQLException {
    return insert(anEntry, pid, value, measureTime, caseID, 0);
  }


  public long insert(CatalogEntry anEntry, long pid, Date value, Timestamp measureTime, long caseID, long docID,
          long groupID) throws SQLException {
    if (PIDImportFilter.getInstance().checkPID(pid)) {
      return getInfoManager().insert(anEntry, pid, value, measureTime, caseID, docID, groupID);
    } else {
      return 0;
    }
  }


  public long insert(CatalogEntry anEntry, long pid, Date value, Timestamp measureTime, long caseID, long docID)
          throws SQLException {
    if (PIDImportFilter.getInstance().checkPID(pid)) {
      return getInfoManager().insert(anEntry, pid, value, measureTime, caseID, docID, 0);
    } else {
      return 0;
    }
  }


  public long insert(CatalogEntry anEntry, long pid, String value, Timestamp measureTime, long caseID)
          throws SQLException {
    return insert(anEntry, pid, value, measureTime, caseID, 0);
  }


  public long insert(CatalogEntry anEntry, long pid, String value, Timestamp measureTime, long caseID, long docID)
          throws SQLException {
    if (PIDImportFilter.getInstance().checkPID(pid)) {
      return getInfoManager().insert(anEntry, pid, value, measureTime, caseID, docID, 0);
    } else {
      return 0;
    }
  }


  public long insert(CatalogEntry anEntry, long pid, String value, Timestamp measureTime, long caseID, long docID,
          long groupID) throws SQLException {
    if (PIDImportFilter.getInstance().checkPID(pid)) {
      return getInfoManager().insert(anEntry, pid, value, measureTime, caseID, docID, groupID);
    } else {
      return 0;
    }
  }


  public long insert(CatalogEntry anEntry, long pid, double value, String valueStr, Timestamp measureTime, long caseID)
          throws SQLException {
    return insert(anEntry, pid, value, valueStr, measureTime, caseID, 0);
  }


  public long insert(CatalogEntry anEntry, long pid, double value, String valueStr, Timestamp measureTime, long caseID,
          long docID) throws SQLException {
    if (PIDImportFilter.getInstance().checkPID(pid)) {
      return getInfoManager().insert(anEntry, pid, value, valueStr, measureTime, caseID, docID);
    } else {
      return 0;
    }
  }


  public long insert(CatalogEntry anEntry, long pid, double value, String valueStr, Timestamp measureTime, long caseID,
          long docID, long groupID) throws SQLException {
    if (PIDImportFilter.getInstance().checkPID(pid)) {
      return getInfoManager().insert(anEntry, pid, value, valueStr, measureTime, caseID, docID, groupID);
    } else {
      return 0;
    }
  }


  public long insert(CatalogEntry anEntry, long pid, Timestamp measureTime, long caseID, long docID)
          throws SQLException {
    return insert(anEntry, pid, measureTime, caseID, docID, 0);
  }


  public long insert(CatalogEntry anEntry, long pid, Timestamp measureTime, long caseID) throws SQLException {
    return insert(anEntry, pid, measureTime, caseID, 0);
  }


  public long insert(CatalogEntry anEntry, long pid, Timestamp measureTime, long caseID, long docID, long groupID)
          throws SQLException {
    if (PIDImportFilter.getInstance().checkPID(pid)) {
      return getInfoManager().insert(anEntry, pid, measureTime, caseID, docID, groupID);
    } else {
      return 0;
    }
  }


  public InfoIterator getInfosByAttrID(int attrID, boolean getValue) throws SQLException {
    return getInfoManager().getInfosByAttrID(attrID, getValue);
  }


  public Information getInfoById(long infoid) throws SQLException {
    return getInfoManager().getInfoById(infoid);
  }


  public InfoIterator getInfosByCaseIDAndEntry(long caseID, CatalogEntry anEntry, boolean getValue)
          throws SQLException {
    return getInfoManager().getInfosByCaseIDAndEntry(caseID, anEntry, getValue);
  }


  public void delete(CatalogEntry anEntry, long pid, long caseid, Timestamp measureDate) throws ImportException {
    try {
      if (DWImportsConfig.getDeleteStornos()) {
        getInfoManager().deleteInfo(anEntry, pid, caseid, measureDate);
      }
    } catch (SQLException e) {
      ImportException tmp = new ImportException(ImportExceptionType.SQL_ERROR, "error while deleting data entry", e);
      tmp.setProject(getProject());
      throw tmp;
    }
  }


  public void delete(long docID) throws ImportException {
    try {
      getInfoManager().deleteInfosForDocID(docID);
    } catch (SQLException e) {
      ImportException tmp = new ImportException(ImportExceptionType.SQL_ERROR, "error while deleting data entry", e);
      tmp.setProject(getProject());
      throw tmp;
    }
  }

}
