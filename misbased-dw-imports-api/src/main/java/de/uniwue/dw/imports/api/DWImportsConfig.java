package de.uniwue.dw.imports.api;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.imports.manager.DBImportLogManager;
import de.uniwue.dw.imports.manager.IImportsAdapterFactory;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.sql.SQLConfig;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;
import de.uniwue.misc.util.ConfigException;

public class DWImportsConfig extends DwClientConfiguration implements IDwImportsConfigurationKeys {

  private static DWImportsConfig instance = null;

  private IImportsAdapterFactory importsAdapterFactory;

  private DBImportLogManager dbImportLogManager;

  public static DBImportLogManager getDBImportLogManager() {
    if (getInstance().dbImportLogManager == null) {
      SQLConfig hdp_SQLConfig = getHDP_SQLConfig();
      try {
        getInstance().dbImportLogManager = new DBImportLogManager(hdp_SQLConfig);
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    return getInstance().dbImportLogManager;
  }


  public static boolean isProperlyHDPConfigured() {
    return (getInstance().getParameter(PARAM_HDP_DB_NAME) != null);
  }


  public static SQLConfig getHDP_SQLConfig() {
    if (isProperlyHDPConfigured()) {
      String dbName = getInstance().getParameter(PARAM_HDP_DB_NAME);
      String dbTypeString = getInstance().getParameter(PARAM_HDP_DB_TYPE);
      DBType dbType = DBType.valueOf(dbTypeString);
      String server = getInstance().getParameter(PARAM_HDP_SERVER);
      boolean useTrustedCon = getInstance().getBooleanParameter(PARAM_HDP_TRUSTED_CONNECTION);
      SQLConfig config = new SQLConfig("", dbName, "", server, dbType, useTrustedCon);
      return config;
    } else {
      return null;
    }
  }


  public static synchronized DWImportsConfig getInstance() {
    if (instance == null) {
      instance = new DWImportsConfig();
      singletons.add(instance);
    }
    return instance;
  }


  @Override
  public void clear() throws ConfigException {
    super.clear();
    importsAdapterFactory = null;
  }


  public IImportsAdapterFactory getImportsAdapterFactory() throws SQLException {
    if (importsAdapterFactory == null) {
      String adapterFactoryClass = getImportAdapterFactoryClass();
      try {
        importsAdapterFactory = (IImportsAdapterFactory) getClass().getClassLoader().loadClass(adapterFactoryClass)
                .getConstructor().newInstance();
      } catch (InstantiationException e) {
        throw new SQLException(e);
      } catch (IllegalAccessException e) {
        throw new SQLException(e);
      } catch (IllegalArgumentException e) {
        throw new SQLException(e);
      } catch (InvocationTargetException e) {
        throw new SQLException(e);
      } catch (NoSuchMethodException e) {
        throw new SQLException(e);
      } catch (SecurityException e) {
        throw new SQLException(e);
      } catch (ClassNotFoundException e) {
        throw new SQLException(e);
      }
    }
    return importsAdapterFactory;
  }


  public String getImportAdapterFactoryClass() throws SQLException {
    String defaultValue = null;
    if (SQLPropertiesConfiguration.getInstance().getSQLConfig().dbType == DBType.MSSQL) {
      defaultValue = "de.uniwue.dw.imports.sql.mssql.MSSQLImportsAdapterFactory";
    } else if (SQLPropertiesConfiguration.getInstance().getSQLConfig().dbType == DBType.MySQL) {
      defaultValue = "de.uniwue.dw.imports.sql.mysql.MySQLImportsAdapterFactory";
    }
    return getParameter(IDwImportsConfigurationKeys.PARAM_IMPORT_ADAPTER_FACTORY_CLASS, defaultValue);
  }


  public static boolean getDoChecksBeforeStart() {
    return getInstance().getBooleanParameter(PARAM_DO_CHECKS_BEFORE_START, true);
  }


  public static boolean getSortCatalogRoot() {
    return getInstance().getBooleanParameter(PARAM_IMPORT_SORT_CATALOG_ROOT, false);
  }


  public static File getSAPImportDir() {
    return getInstance().getFileParameter(PARAM_SAP_EXPORT_DIR);
  }


  public static File getTermDir() {
    return getInstance().getFileParameter(PARAM_TERMINOLOGIES_DIR);
  }


  public static File getImportConfigsDir() {
    return getInstance().getFileParameter(PARAM_IMPORT_CONFIGS);
  }


  public static File getBackupDir() {
    return getInstance().getFileParameter(PARAM_BACKUP_DIR);
  }


  public static File getPIDFilterFile() {
    return getInstance().getFileParameter(PARAM_PID_FILTER_FILE);
  }


  public static String getImporterRegexFilter() {
    return getInstance().getParameter(PARAM_IMPORTER_REGEX_FILTER, ".*");
  }


  public static double getCSVFailRate() {
    return getInstance().getDoubleParameter(PARAM_CSV_FAIL_RATE, 0.0);
  }


  public static String getImporterRegexFilterExclude() {
    return getInstance().getParameter(PARAM_IMPORTER_REGEX_FILTER_EXCLUDE, "^$");
  }


  public static Boolean getImportCatalogIfNecessary() {
    return getInstance().getBooleanParameter(PARAM_IMPORT_CATALOG, true);
  }


  public static Boolean getDropAllOldTables() {
    return getInstance().getBooleanParameter(PARAM_DROP_ALL_TABLES, false);
  }


  public static Boolean getTreatAsInitialImport() {
    return getInstance().getBooleanParameter(PARAM_TREAT_ASINITIAL_IMPORT, false);
  }


  public static Boolean getDropMetaDataTables() {
    return getInstance().getBooleanParameter(PARAM_DROP_METADATA_TABLES, false);
  }


  public static Boolean getDropFactTables() {
    return getInstance().getBooleanParameter(PARAM_DROP_FACT_TABLES, false);
  }


  public static Boolean getLoadMetaDataLazy() {
    return getInstance().getBooleanParameter(PARAM_LOAD_IMPORT_METADATA_LAZY, true);
  }


  public static Boolean getLoadDocMetaData() {
    return getInstance().getBooleanParameter(PARAM_LOAD_IMPORT_DOC_METADATA, false);
  }


  public static Boolean getMoveFilesAfterUpdate() {
    return DwClientConfiguration.getInstance().getBooleanParameter(PARAM_MOVE_FILE_AFTER_UPDATE, false);
  }


  public static Boolean getDeleteStornos() {
    return getInstance().getBooleanParameter(PARAM_DELETE_STORNOS, true);
  }


  public static Boolean getSendMailOnSuccess() {
    return getInstance().getBooleanParameter(PARAM_REPORT_SEND_MAIL_ON_SUCCESS, false);
  }


  public static Boolean getSendMailOnFail() {
    return getInstance().getBooleanParameter(PARAM_REPORT_SEND_MAIL_ON_FAIL, false);
  }


  public static String getOnlyAfter() {
    return getInstance().getParameter(PARAM_ONLY_AFTER);
  }


  public static String getOnlyBefore() {
    return getInstance().getParameter(PARAM_ONLY_BEFORE);
  }


  public static Boolean getImportPIDMetaInfos() {
    return getInstance().getBooleanParameter(PARAM_IMPORT_PATIENT_METAINFOS, true);
  }


  public static Boolean getImportCaseMetaInfos() {
    return getInstance().getBooleanParameter(PARAM_IMPORT_CASE_METAINFOS, true);
  }


  public static Boolean getImportDocMetaInfos() {
    return getInstance().getBooleanParameter(PARAM_IMPORT_DOC_METAINFOS, true);
  }


  public static Boolean getImportMovMetaInfos() {
    return getInstance().getBooleanParameter(PARAM_IMPORT_MOV_METAINFOS, true);
  }


  public static Boolean getImportMetaInfos() {
    return getInstance().getBooleanParameter(PARAM_IMPORT_METAINFOS, true);
  }


  public static Boolean getMissingMetaInfoIsError() {
    return getInstance().getBooleanParameter(PARAM_IMPORT_MISSING_METAINFO_IS_ERROR, false);
  }


  public static Boolean getImportInfos() {
    return getInstance().getBooleanParameter(PARAM_IMPORT_INFOS, true);
  }


  public static Boolean getDeleteStornosOfMetaData() {
    return getInstance().getBooleanParameter(PARAM_DELETE_STORNOS_OF_META_DATA, true);
  }


  public static Boolean getDeleteInfosWOCatalogEntries() {
    return getInstance().getBooleanParameter(PARAM_DELETE_INFOS_WO_CATALOG_ENTRIES, false);
  }


  public static Boolean getLockForUpdate() {
    return getInstance().getBooleanParameter(PARAM_LOCK_DW_FOR_UPDATE, false);
  }


  public static Boolean getDoLogErrors() {
    return getInstance().getBooleanParameter(PARAM_DO_LOG_ERRORS, true);
  }


  public static Boolean getDoLogInfosAndWarnings() {
    return getInstance().getBooleanParameter(PARAM_DO_LOG_INFOS_AND_WARNINGS, true);
  }


  public static Boolean getParallelizeImport() {
    return getInstance().getBooleanParameter(PARAM_PARALLELIZE_IMPORT, false);
  }

}
