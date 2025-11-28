package de.uniwue.dw.tests;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.api.configuration.IDWClientKeys;
import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.client.authentication.group.AuthManager;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.model.manager.CompleteCatalogClientManager;
import de.uniwue.dw.core.model.manager.InfoManager;
import de.uniwue.dw.core.model.util.ImportExportDump;
import de.uniwue.dw.exchangeFormat.EFImport;
import de.uniwue.dw.imports.PIDImportFilter;
import de.uniwue.dw.imports.app.TheDwImperatorApp;
import de.uniwue.dw.imports.manager.ImportLogManager;
import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.client.IGUIClient;
import de.uniwue.dw.query.model.client.QueryCache;
import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.exception.QueryStructureException;
import de.uniwue.dw.query.model.exception.QueryStructureException.QueryStructureExceptionType;
import de.uniwue.dw.query.model.result.Cell;
import de.uniwue.dw.query.model.result.Result;
import de.uniwue.dw.query.model.result.export.ExportConfiguration;
import de.uniwue.dw.query.model.result.export.ExportType;
import de.uniwue.dw.query.model.result.export.MemoryOutputHandler;
import de.uniwue.dw.query.model.tests.QueryTest;
import de.uniwue.dw.query.model.tests.QueryTestLoader;
import de.uniwue.dw.query.solr.model.manager.EmbeddedSolrManager;
import de.uniwue.dw.query.solr.preprocess.TheDWIndexingApp;
import de.uniwue.dw.solr.api.DWSolrConfig;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.sql.ISqlConfigKeys;
import de.uniwue.misc.util.ConfigException;
import de.uniwue.misc.util.Configuration;
import de.uniwue.misc.util.ResourceUtil;
import org.junit.AfterClass;

import javax.security.auth.login.AccountException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class AbstractTest {

  private static final String testuserName = "testuser";

  private static final String testuserPassword = "testuser";

  private static final String testGroupName = "testGroup";

  protected static TheDwImperatorApp imperator;

  // this variable is an additional flag that can be set by test instances during initialize, so
  // that individual tests should not be executed
  protected static boolean canPerformTestFlag = true;

  protected static IGUIClient queryClient;

  private static QueryTestLoader testLoader;

  private static CompleteCatalogClientManager completeCatalogClientManager;

  protected DBType dbType;

  protected QueryEngineType engineType;

  protected AbstractTest(DBType aType, QueryEngineType engineType) {
    dbType = aType;
    this.engineType = engineType;
  }

  private static CompleteCatalogClientManager getCompleteCatalogManager() throws SQLException {
    if (completeCatalogClientManager == null) {
      completeCatalogClientManager = new CompleteCatalogClientManager();
    }
    return completeCatalogClientManager;
  }

  private static QueryTestLoader getTestLoader() throws SQLException {
    if (testLoader == null) {
      testLoader = new QueryTestLoader(getCompleteCatalogManager());
    }
    return testLoader;
  }

  public static ArrayList<QueryTest> getTests(String path)
          throws IOException, QueryException, SQLException {
    ArrayList<QueryTest> result = new ArrayList<>();
    File queryDir = ResourceUtil.getResource("classpath:" + path).getFile();
    for (File aQueryFile : queryDir.listFiles()) {
      if (aQueryFile.isFile()) {
        String relativeName = new File(path, aQueryFile.getName()).getPath();
        QueryTest test = getTestLoader().readTest(relativeName);
        result.add(test);
      }
    }
    return result;
  }

  @AfterClass
  public static void finish() throws Exception {
    if (queryClient != null) {
      queryClient.dispose();
      queryClient = null;
    }
  }

  protected static void createGUIClient(DBType dbType, QueryEngineType engineType)
          throws GUIClientException {
    if (queryClient != null) {
      throw new GUIClientException("GUIClient already created");
    }
    if (canPerformTest(dbType, engineType)) {
      createGuiClient(engineType);
    }
  }

  private static void createGuiClient(QueryEngineType engineType) throws GUIClientException {
    // so that the GUIClients do not have to be directly referenced they are instantiated by string
    try {
      String classToLoad;
      if (engineType == QueryEngineType.Solr) {
        classToLoad = "de.uniwue.dw.query.solr.SolrGUIClient";
      } else if (engineType == QueryEngineType.I2B2) {
        classToLoad = "de.uniwue.dw.query.i2b2.search.I2B2GUIClient";
      } else if (engineType == QueryEngineType.SQL) {
        classToLoad = "de.uniwue.dw.query.sql.util.SQLGUIClient";
      } else if (engineType == QueryEngineType.Hana) {
        classToLoad = "";
      } else {
        throw new GUIClientException("Unknown query engine: " + engineType);
      }
      queryClient = (IGUIClient) AbstractTest.class.getClassLoader().loadClass(classToLoad)
              .getConstructor().newInstance();
    } catch (Exception e) {
      throw new GUIClientException(e);
    }
  }

  public static void initialize(DBType dbType, QueryEngineType engineType, File testPropertiesFile,
          ImportDumpMode importDumpMode, Class<?> aClass) throws Exception {
    canPerformTestFlag = testPropertiesFile.exists();
    initAbstractTest(testPropertiesFile, dbType, engineType, importDumpMode, aClass);
    createGUIClient(dbType, engineType);
  }

  protected static void importDump() throws IOException, SQLException {
    ImportExportDump ieDump = new ImportExportDump();
    String basePath = "src/main/resources/Dumps/";
    if (!(new File(basePath).exists())) {
      basePath = "src/test/resources/Dumps/";
    }
    if (!(new File(basePath).exists()))
      basePath = AbstractTest.class.getClassLoader().getResource("Dumps/").getFile();
    ieDump.importDump(basePath + "catalogDump.csv", basePath + "infoDump.csv");
  }

  protected static void initAbstractTest(File configFile, DBType aDBType,
          QueryEngineType engineType, ImportDumpMode importDumpMode, Class<?> aClass)
          throws Exception {
    initAbstractTest(configFile, aDBType, engineType, importDumpMode, aClass, false);
  }

  protected static void initAbstractTest(File configFile, DBType aDBType,
          QueryEngineType engineType, ImportDumpMode importDumpMode, Class<?> aClass,
          boolean doSQLUpdatesIfBulkImportNotUsed)
          throws Exception {
    System.out.println("Starting Test: " + aClass.toString());
    Configuration.clearAllConfigurations();
    PIDImportFilter.getInstance().clear();
    configFile = createMergedConfigFile(configFile, aDBType, engineType, doSQLUpdatesIfBulkImportNotUsed);
    if (configFile == null) {
      System.out.println(
              "This test can't be executed. Maybe some configuration proerties are not correctly set.");
      canPerformTestFlag = false;
      return;
    }
    ImportLogManager.clearImportedFiles();
    imperator = new TheDwImperatorApp();
    if (importDumpMode == ImportDumpMode.ExchangeFormat) {
      String basePath = "src/main/resources/NewData/";
      if (!(new File(basePath).exists())) {
        basePath = "src/test/resources/NewData/";
      }
      if (!(new File(basePath).exists()))
        basePath = AbstractTest.class.getClassLoader().getResource("NewData/").getFile();
      EFImport.doImport(basePath + "catalog.csv", basePath + "facts.csv", null, null, true);

      // Add group information to the imported data, as this is not currently contained in the EF-Files imported above
      CatalogManager cm = DwClientConfiguration.getInstance().getCatalogManager();
      InfoManager im = DwClientConfiguration.getInstance().getInfoManager();

      CatalogEntry i50_1 = cm.getEntryByRefID("i50_1", "diagnose");
      CatalogEntry entlassDiagnose = cm.getEntryByRefID("entlass_diagnose", "diagnose");

      Calendar calendar = Calendar.getInstance();
      calendar.set(2010, Calendar.JANUARY, 1, 0, 0, 0);
      calendar.set(Calendar.MILLISECOND, 0);
      Timestamp measureTime = new Timestamp(calendar.getTimeInMillis());

      im.deleteInfo(i50_1, 1002, 1102, measureTime);
      im.deleteInfo(entlassDiagnose, 1002, 1102, measureTime);
      im.insert(i50_1, 1002, measureTime, 1102, 1, 1);
      im.insert(entlassDiagnose, 1002, measureTime, 1102, 1, 1);
      im.commit();
    } else {
      imperator.doImport(configFile);
      if (importDumpMode == ImportDumpMode.ClassicFormat) {
        importDump();
      }
    }
    addPermissions();
    String[] args = { configFile.getAbsolutePath() };
    if (engineType == QueryEngineType.Solr) {
      try {
        DWSolrConfig.getInstance().getSolrManager();
      } catch (IllegalStateException e) {
        DWSolrConfig.getInstance().setSolrManager(new EmbeddedSolrManager(), true);
      }
      TheDWIndexingApp.main(args);
    }
    QueryCache.getInstance().clear();
  }

  public static AuthManager getAuthManager() throws SQLException {
    return DwClientConfiguration.getInstance().getAuthManager();
  }

  public static User getTestUser() throws AccountException, SQLException {
    return getAuthManager().authenticate(testuserName, testuserPassword, null);
  }

  public static void addPermissions() throws SQLException {
    AuthManager authService = getAuthManager();
    Collection<String> users = authService.getUsernames();
    if (!users.contains(testuserName)) {
      try {
        authService.addUser(testuserName, testuserName, testuserPassword, "user", "test@test.com");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    }
    if (authService.getGroup(testGroupName) == null) {
      authService.addGroup(testGroupName, 0, true, true);
    }
    authService.addUser2Group(testuserName, testGroupName);
  }

  protected static boolean canPerformTest(DBType aDBType, QueryEngineType engineType) {
    return canPerformTest(aDBType, engineType, canPerformTestFlag);
  }

  private static boolean systemTestFileParamInValid(File aParm) {
    return (aParm == null) || !aParm.exists();
  }

  protected static boolean canPerformTest(DBType aDBType, QueryEngineType engineType,
          boolean aCanPerformTestFlag) {
    // it is assumed that the config has definitely been loaded
    boolean result = aCanPerformTestFlag;
    if (engineType == QueryEngineType.Solr) {
      if (systemTestFileParamInValid(
              SystemTestParameters.getInstance().getSOLR_ConnectionConfigFile())) {
        result = false;
      }
    }
    if (engineType == QueryEngineType.I2B2) {
      if (systemTestFileParamInValid(
              SystemTestParameters.getInstance().getI2B2__AS_INDEX_CONNECTION_CONFIG_FILE())) {
        result = false;
      }
    }
    if (aDBType == DBType.MSSQL) {
      if (!SystemTestParameters.doMSSQLTests() || systemTestFileParamInValid(
              SystemTestParameters.getInstance().getMSSQL_ConnectionConfigFile())) {
        result = false;
      }
    } else if (aDBType == DBType.MySQL) {
      if (!SystemTestParameters.doMySQLTests() || systemTestFileParamInValid(
              SystemTestParameters.getInstance().getMYSQL_ConnectionConfigFile())) {
        result = false;
      }
    } else {
      throw new RuntimeException("unknown DBType");
    }
    return result;
  }

  protected static File createMergedConfigFile(File configFile, DBType aDBType,
          QueryEngineType engineType, boolean doSQLUpdatesIfBulkImportNotUsed)
          throws IOException, ConfigException {
    if (!canPerformTest(aDBType, engineType)) {
      return null;
    }
    if (aDBType == DBType.MSSQL) {
      DwClientConfiguration
              .loadProperties(SystemTestParameters.getInstance().getMSSQL_ConnectionConfigFile());
    } else if (aDBType == DBType.MySQL) {
      DwClientConfiguration
              .loadProperties(SystemTestParameters.getInstance().getMYSQL_ConnectionConfigFile());
    } else {
      throw new ConfigException("unknown DBType");
    }
    String outputFileName = aDBType + "_";
    if (engineType == QueryEngineType.Solr) {
      DwClientConfiguration
              .loadProperties(SystemTestParameters.getInstance().getSOLR_ConnectionConfigFile());
      outputFileName += "Solr_";
    } else if (engineType == QueryEngineType.I2B2) {
      DwClientConfiguration.loadProperties(
              SystemTestParameters.getInstance().getI2B2__AS_INDEX_CONNECTION_CONFIG_FILE());
      outputFileName += "I2B2_";
    }
    DwClientConfiguration.loadProperties(configFile);
    Properties props = DwClientConfiguration.getInstance().getProps();
    for (String aPropertyName : props.stringPropertyNames()) {
      String propertyValue = props.getProperty(aPropertyName);
      String resourcesPathRegex = ".*[\\\\/]?src[\\\\/](main)?(test)?[\\\\/]resources[\\\\/]?.*";
      String replacePathRegex = resourcesPathRegex.replace(".*", "");
      if (propertyValue.matches(resourcesPathRegex) && !(new File(propertyValue).exists())) {
        propertyValue = propertyValue.replaceFirst(replacePathRegex, "");
        propertyValue = AbstractTest.class.getClassLoader().getResource(propertyValue).getFile();
        props.setProperty(aPropertyName, propertyValue);
      }
    }
    if (doSQLUpdatesIfBulkImportNotUsed && !props.containsKey(IDWClientKeys.PARAM_DO_SQL_UPDATES)) {
      if (!props.containsKey(ISqlConfigKeys.PARAM_SQL_BULK_IMPORT_DIR)) {
        props.setProperty(IDWClientKeys.PARAM_DO_SQL_UPDATES, "true");
      }
    }
    outputFileName += configFile.getName();
    File outputDir = new File(SystemTestParameters.getInstance().getRootConfigPath(),
            "mergedConfigs");
    if (!outputDir.exists()) {
      outputDir.mkdir();
    }
    File outputFile = new File(outputDir, outputFileName);
    FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
    props.store(fileOutputStream, "");
    fileOutputStream.close();
    return outputFile;
  }

  protected QueryTest getTest(String fileName, QueryEngineType engine)
          throws IOException, QueryException, SQLException {
    File file = new File(fileName);
    String dir = file.getParent();
    File possibleOverrideDir = new File(dir, engine.toString());
    File possibleOverrideFile = new File(possibleOverrideDir, file.getName());
    if (ResourceUtil.getResource("classpath:" + possibleOverrideFile.getPath()).exists()) {
      fileName = possibleOverrideFile.getPath();
    }
    return getTestLoader().readTest(fileName);
  }

  protected void startTest(String queryFilename, IGUIClient client)
          throws QueryException, AccountException {
    try {
      if (!canPerformTest(dbType, engineType)) {
        return;
      }
      QueryCache.getInstance().clear();
      QueryTest test = getTest(queryFilename, engineType);
      Set<QueryStructureException> structureErrors = client.getQueryRunner()
              .getStructureErrors(test.query);
      if (!structureErrors.isEmpty()) {
        for (QueryStructureException e : structureErrors) {
          if ((e.structureType != QueryStructureExceptionType.ENGINE_CANNOT_PERFORM_OPERATION)) {
            throw new QueryException("Errors in query structure:\n" + structureErrors);
          } else {
            if (!client.getQueryRunner().canDoPostProcessing()) {
              throw new QueryException("Engine cannot process query:\n" + structureErrors);
            }
          }
        }
      }
      ExportConfiguration eConf = new ExportConfiguration(ExportType.RESULT_TABLE);
      int runningQueryID = client.getQueryRunner().createQuery(test.query, getTestUser(), eConf);
      Result result = client.getQueryRunner().runQueryBlocking(runningQueryID);
      checkResult(test.desiredResult, result);
    } catch (IOException | GUIClientException | SQLException e) {
      throw new QueryException(e);
    }
  }

  protected void checkResult(Result desiredResults, Result results) {
    String sortedResults = serializeAndSort2DArray(to2DStringArray(results));
    String sDesiredResults = serializeAndSort2DArray(to2DStringArray(desiredResults));
    // check number of rows
    assertEquals("number of rows:", desiredResults.getRows().size(), results.getRows().size());

    for (int i = 0; i < desiredResults.getRows().size(); i++) {
      // check number of columns in row i
      assertEquals("columns in row " + i + ":", desiredResults.getRow(i).getCells().size(),
              results.getRows().get(i).getCells().size());
    }
    // check results
    assertEquals(sDesiredResults, sortedResults);
  }

  protected String[][] to2DStringArray(Result list) {
    if (list.getRows().size() < 1) {
      return new String[0][0];
    }
    String[][] res = new String[list.getRows().size()][list.getRows().get(0).getCells().size()];
    for (int i = 0; i < res.length; i++) {
      for (int j = 0; j < res[i].length; j++) {
        Cell cell = list.getCell(i, j);
        if (cell.getValueAsString() == null) {
          res[i][j] = "";
        } else {
          res[i][j] = MemoryOutputHandler.defaultFormatter.getFormattedValue(cell);
        }
      }
    }
    return res;
  }

  protected String serializeAndSort2DArray(String[][] array) {
    StringBuilder res = new StringBuilder();
    for (String[] strings : array) {
      Arrays.sort(strings);
      for (int j = 0; j < strings.length - 1; j++) {
        res.append(strings[j]).append("|");
      }
      if (strings.length == 0) {
        continue;
      }
      res.append(strings[strings.length - 1]);
      res.append("===");
    }
    String[] temp = res.toString().split("===");
    Arrays.sort(temp);
    res = new StringBuilder();
    for (String s : temp) {
      res.append(s).append("===");
    }
    return res.toString();
  }

  public enum ImportDumpMode {
    None, ClassicFormat, ExchangeFormat
  }

}
