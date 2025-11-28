package de.uniwue.dw.tests;

import de.uniwue.misc.util.ConfigException;
import de.uniwue.misc.util.Configuration;

import java.io.File;
import java.io.IOException;

public class SystemTestParameters extends Configuration {

  private static SystemTestParameters instance = null;

  public static synchronized SystemTestParameters getInstance() {
    if (instance == null) {
      instance = new SystemTestParameters();
      singletons.add(instance);
    }
    return instance;
  }

  @Override
  public void clear() {
    instance = null;
  }

  public SystemTestParameters() {
    try {
      if (getMetaConfigsPath().exists()) {
        loadProperties(getMetaConfigsPath());
      }
    } catch (IOException | ConfigException e) {
      throw new RuntimeException(e);
    }
  }

  private File getConfigFolder() {
    File resourceFolder = new File("src/main/resources");
    File configFileFolder = new File(resourceFolder, "Configs");
    if (!configFileFolder.exists()) {
      resourceFolder = new File("src/test/resources");
      configFileFolder = new File(resourceFolder, "Configs");
    }
    if (!configFileFolder.exists())
      configFileFolder = new File(getClass().getClassLoader().getResource("Configs").getFile());
    return configFileFolder;
  }

  public static boolean doMSSQLTests() {
    return getInstance().getBooleanParameter("doMSSQLTests", true);
  }

  public static boolean doMySQLTests() {
    return getInstance().getBooleanParameter("doMySQLTests", true);
  }

  protected File getPropFileFolder() {
    File resourceFolder = new File("src/main/resources");
    File propFileFolder = new File(resourceFolder, "PropertyFiles");
    if (!propFileFolder.exists()) {
      resourceFolder = new File("src/test/resources");
      propFileFolder = new File(resourceFolder, "PropertyFiles");
    }
    if (!propFileFolder.exists())
      propFileFolder = new File(getClass().getClassLoader().getResource("PropertyFiles").getFile());

    return propFileFolder;
  }

  public File getRootConfigPath() {
    String testConfigsPath = System.getenv("PADAWAN_TEST_CONFIG");
    File result;
    if (testConfigsPath == null) {
      result = new File("/import");
    } else {
      result = new File(testConfigsPath);
    }
    if (!result.exists()) {
      throw new RuntimeException(
              "Root test properties dir does not exist: " + result.getAbsolutePath());
    }
    return result;
  }

  public File getMetaConfigsPath() {
    return getPropFile(getRootConfigPath(), "testMeta.properties");
  }

  public File getMSSQL_ConnectionConfigFile() {
    return getPropFile(getRootConfigPath(), getConfigFolder(), "ConnectionMSSQL.properties");
  }

  public File getMYSQL_ConnectionConfigFile() {
    return getPropFile(getRootConfigPath(), getConfigFolder(), "ConnectionMySQL.properties");
  }

  public File getSOLR_ConnectionConfigFile() {
    return getPropFile(getRootConfigPath(), getConfigFolder(), "ConnectionSolr.properties");
  }

  // I2B2 tests should only be run when set up locally
  public File getI2B2_CONNECTION_CONFIG_FILE() {
    return getPropFile(getRootConfigPath(), null, "ConnectionI2B2.properties");
  }

  public File getI2B2__AS_INDEX_CONNECTION_CONFIG_FILE() {
    return getPropFile(getRootConfigPath(), null, "ConnectionI2B2_asIndex.properties");
  }

  public File getCONNECTION_TEST_CONFIG_FILE() {
    return getPropFile(getPropFileFolder(), "Test_Connection.properties");
  }

  public File getINDEX_TEST_CONFIG_FILE() {
    return getPropFile(getPropFileFolder(), "Test_Index.properties");
  }

  public File getIMPORT_SQLUPDATES_TEST_CONFIG_FILE1() {
    return getPropFile(getPropFileFolder(), "Test_ImportSQLUpdates1.properties");
  }

  public File getIMPORT_SQLUPDATES_TEST_CONFIG_FILE2() {
    return getPropFile(getPropFileFolder(), "Test_ImportSQLUpdates2.properties");
  }

  public File getIMPORT_DUMP_TEST_CONFIG_FILE() {
    return getPropFile(getPropFileFolder(), "Test_ImportDump.properties");
  }

  public File getIMPORT_SQL_DUMP_TEST_CONFIG_FILE() {
    return getPropFile(getPropFileFolder(), "Test_Import_SQLDump.properties");
  }

  public File getCREATE_TABLES_TEST_CONFIG_FILE() {
    return getPropFile(getPropFileFolder(), "Test_CreateTables.properties");
  }

  public File getSTORNO_TEST_CONFIG_FILE1() {
    return getPropFile(getPropFileFolder(), "Test_Storno_First.properties");
  }

  public File getSTORNO_TEST_CONFIG_FILE2() {
    return getPropFile(getPropFileFolder(), "Test_Storno_Second.properties");
  }

  public File getCONFIGURED_DBSOURCE_IMPORT_TEST_CONFIG_FILE() {
    return getPropFile(getRootConfigPath(), getPropFileFolder(),
            "Test_ConfiguredImporterDBSource.properties");
  }

  public File getCONFIGURED_IMPORT_TEST_CONFIG_FILE() {
    return getPropFile(getPropFileFolder(), "Test_ConfiguredImporter.properties");
  }

  public File getCONFIGURED_IMPORT_PARALLEL_TEST_CONFIG_FILE() {
    return getPropFile(getPropFileFolder(), "Test_ConfiguredImporter_ParallelImport.properties");
  }

  public File getPID_FILTER_TEST_CONFIG_FILE() {
    return getPropFile(getPropFileFolder(), "Test_PIDFilter.properties");
  }

  public File getCONFIGURED_V0_1_IMPORT_TEST_CONFIG_FILE() {
    return getPropFile(getPropFileFolder(), "Test_ConfiguredImporterv0_1.properties");
  }

  public File getQUERY_LOGIC_TEST_FILE() {
    return getPropFile(getPropFileFolder(), "Test_QueryLogicTest.properties");
  }

  public File getI2B2_QUERY_LOGIC_TEST_FILE() {
    return getPropFile(getPropFileFolder(), "Test_I2B2QueryLogicTest.properties");
  }

  protected File getPropFile(File givenRootFolder, String propFileName) {
    return getPropFile(givenRootFolder, givenRootFolder, propFileName);
  }

  protected File getPropFile(File givenRootFolder, File defaultFolder, String propFileName) {
    File propFile = new File(givenRootFolder, propFileName);
    if (!propFile.exists()) {
      if (defaultFolder != null) {
        propFile = new File(defaultFolder, propFileName);
      } else {
        propFile = null;
      }
    }
    return propFile;
  }

}
