package de.uniwue.misc.sql;

import de.uniwue.misc.util.ConfigException;
import de.uniwue.misc.util.Configuration;

import java.io.File;
import java.sql.SQLException;

public class SQLPropertiesConfiguration extends Configuration {

  protected static SQLPropertiesConfiguration instance = null;

  private SQLManager sqlManager;

  private SQLFactory sqlFactory;

  public static synchronized SQLPropertiesConfiguration getInstance() {
    if (instance == null) {
      instance = new SQLPropertiesConfiguration();
      singletons.add(instance);
    }
    return instance;
  }

  protected SQLPropertiesConfiguration() {
  }

  public SQLFactory getSQLFactory() throws SQLException {
    if (sqlFactory == null) {
      String factoryClassDefaultName = getInstance()
              .getParameter(ISqlConfigKeys.PARAM_SQL_FACTORY_CLASS);
      sqlFactory = SQLFactory.getSQLFactory(getSQLConfig(), factoryClassDefaultName);
    }
    return sqlFactory;
  }

  public static String getSQLUserName() {
    return getInstance().getParameter(ISqlConfigKeys.PARAM_SQL_USER_NAME);
  }

  public static String getSQLPassword() {
    return getInstance().getParameter(ISqlConfigKeys.PARAM_SQL_PASSWORD);
  }

  public static String getSQLDBName() {
    return getInstance().getParameter(ISqlConfigKeys.PARAM_SQL_DB_NAME);
  }

  public static String getSQLServerURL() {
    return getInstance().getParameter(ISqlConfigKeys.PARAM_SQL_SERVER);
  }

  public static boolean getSQLBulkInsertMerge() {
    return getInstance().getBooleanParameter(ISqlConfigKeys.PARAM_SQL_BULK_INSERT_MERGE, false);
  }

  public static File getSQLBulkImportDir() {
    String dir = getInstance().getParameter(ISqlConfigKeys.PARAM_SQL_BULK_IMPORT_DIR);
    if ((dir != null) && (!dir.isEmpty())) {
      return getInstance().getFileParameter(ISqlConfigKeys.PARAM_SQL_BULK_IMPORT_DIR);
    } else {
      return null;
    }
  }

  public static String getSQLBulkImportDirDestinationHostPerspective() {
    return getInstance().getParameter(ISqlConfigKeys.PARAM_SQL_BULK_IMPORT_DIR_DESTINATION_HOST_PERSPECTIVE);
  }

  @Override
  public void clear() throws ConfigException {
    sqlManager = null;
    sqlFactory = null;
  }

  /**
   * Erzwingt Erzeugung des SQLManagers anhand der aktuellen SQLConfig
   *
   * @param isNew
   * @return
   * @throws SQLException
   */
  public SQLManager getSQLManager(boolean isNew) throws SQLException {
    if (isNew) {
      SQLConfig sqlConfig = getInstance().getSQLConfig();
      getInstance().sqlManager = getSQLFactory().getSQLManager(sqlConfig);
    }
    return getInstance().sqlManager;
  }

  public SQLConfig getSQLConfig() throws SQLException {
    String sqlUsername = getParameter(ISqlConfigKeys.PARAM_SQL_USER_NAME);
    String sqlPassword = getParameter(ISqlConfigKeys.PARAM_SQL_PASSWORD);
    String sqlDatabaseName = getParameter(ISqlConfigKeys.PARAM_SQL_DB_NAME);
    String sqlServer = getParameter(ISqlConfigKeys.PARAM_SQL_SERVER);
    boolean sqlUseTrustedConnection = getBooleanParameter(
            ISqlConfigKeys.PARAM_SQL_TRUSTED_CONNECTION, false);
    DBType sqlDatabaseType = DBType
            .valueOf(getParameter(ISqlConfigKeys.PARAM_SQL_DB_TYPE, DBType.MSSQL.toString()));
    SQLConfig sqlConfig = new SQLConfig(sqlUsername, sqlDatabaseName, sqlPassword, sqlServer,
            sqlDatabaseType, sqlUseTrustedConnection);
    return sqlConfig;
  }

  public SQLManager getSQLManager(SQLConfig aConfig) throws SQLException {
    return getSQLFactory().getSQLManager(aConfig);
  }

  public SQLManager getSQLManager() throws SQLException {
    if (getInstance().sqlManager == null) {
      getSQLManager(true);
    }
    return getInstance().sqlManager;
  }

}
