package de.uniwue.dw.query.model;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.api.configuration.ICatalogClientManagerFactory;
import de.uniwue.dw.core.client.api.configuration.IDWClientKeys;
import de.uniwue.dw.core.model.manager.ICatalogClientManager;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;
import de.uniwue.misc.util.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DWQueryConfig extends Configuration {

  private static final Logger logger = LogManager.getLogger(DWQueryConfig.class);

  private static final int COMMIT_AFTER_DOCS_DEFAULT = 1000;

  private static DWQueryConfig instance = null;

  private IQueryAdapterFactory adapterFactory;

  private ICatalogClientManagerFactory catalogClientFactory;

  private ICatalogClientManager catalogClientManager;

  public static synchronized DWQueryConfig getInstance() {
    if (instance == null) {
      instance = new DWQueryConfig();
      singletons.add(instance);
    }
    return instance;
  }

  public static List<String[]> getQuickSearchExamples() {
    List<String[]> result = new ArrayList<>();
    int i = 1;
    String line = null;
    while ((line = DwClientConfiguration.getInstance()
            .getParameter(IQueryKeys.PARAM_QUICK_SEARCH_EXAMPLE + i)) != null) {
      i++;
      String[] parts = line.split(",", -1);
      if (parts.length == 4) {
        result.add(parts);
      } else {
        logger.warn("input must have 4 parts seperated with ','. wrong input: " + line);
      }

    }
    return result;
  }

  public static boolean getFilterDocumentsByGroup() {
    return DwClientConfiguration.getInstance()
            .getBooleanParameter(IQueryKeys.PARAM_FILTER_DOCUMENTS_BY_GROUPS, false);
  }

  public static boolean getDoCatalogCountPersonalisation() {
    return DwClientConfiguration.getInstance()
            .getBooleanParameter(IQueryKeys.PARAM_DO_CATALOG_COUNT_PERSONALISATION, false);
  }

  public static String getProjectToIndex() {
    String projectsString = DwClientConfiguration.getInstance()
            .getParameter(IQueryKeys.PARAM_INDEXER_PROJECT_TO_INDEX, "");
    return projectsString;
  }

  public static int getIndexerCommitAfterDocs() {
    return DwClientConfiguration.getInstance().getIntegerParameter(
            IQueryKeys.PARAM_INDEXER_COMMIT_AFTER_DOCS, COMMIT_AFTER_DOCS_DEFAULT);
  }

  public static boolean hasToIndexCatalog() {
    return DwClientConfiguration.getInstance()
            .getBooleanParameter(IQueryKeys.PARAM_INDEXER_INDEX_CATALOG, true);
  }

  // public String getQueryAdapterFactoryClass() throws SQLException {
  // String defaultValue = "de.uniwue.dw.query.model.sql.SQLQueryAdapterFactory";
  // return getParameter(IQueryKeys.PARAM_QUERY_ADAPTER_FACTORY_CLASS, defaultValue);
  // }

  public static boolean hasToIndexEntireCatalog() {
    return DwClientConfiguration.getInstance()
            .getBooleanParameter(IQueryKeys.PARAM_INDEXER_INDEX_ENTIRE_CATALOG, false);
  }

  public static boolean hasToCalculateCatalogCount() {
    return DwClientConfiguration.getInstance()
            .getBooleanParameter(IQueryKeys.PARAM_INDEXER_CALCULATE_CATALOG_COUNT, true);
  }

  public static boolean hasToIndexData() {
    return DwClientConfiguration.getInstance()
            .getBooleanParameter(IQueryKeys.PARAM_INDEXER_INDEX_DATA, true);
  }

  public static boolean hasToDeleteIndex() {
    return DwClientConfiguration.getInstance()
            .getBooleanParameter(IQueryKeys.PARAM_INDEXER_DELETE_INDEX, false);
  }

  public static boolean hasToIndexAllData() {
    return DwClientConfiguration.getInstance()
            .getBooleanParameter(IQueryKeys.PARAM_INDEXER_INDEX_ALL_DATA, false);
  }

  public static boolean processDeletedInfos() {
    return DwClientConfiguration.getInstance()
            .getBooleanParameter(IQueryKeys.PARAM_INDEXER_PROCESS_DELETED_INFOS, true);
  }

  public static boolean doIncrementalUpdate() {
    return DwClientConfiguration.getInstance()
            .getBooleanParameter(IQueryKeys.PARAM_INDEXER_DO_INCREMENTAL_UPDATE, false);
  }

  public static int queryHighlightFragSize() {
    return DwClientConfiguration.getInstance()
            .getIntegerParameter(IQueryKeys.PARAM_HIT_HIGHLIGHT_FRAGSIZE, 100);
  }

  public static String queryUserIgnoreLogging() {
    return DwClientConfiguration.getInstance()
            .getParameter(IQueryKeys.PARAM_IGNORE_USER_FOR_QUERY_LOGGING, "ignoreUser");
  }

  public static String queryHighlightPre() {
    return DwClientConfiguration.getInstance().getParameter(IQueryKeys.PARAM_HIT_HIGHLIGHT_PRE,
            "<match>");
  }

  public static String queryHighlightStyle() {
    return DwClientConfiguration.getInstance().getParameter(IQueryKeys.PARAM_HIT_HIGHLIGHT_STYLE,
            "background-color: yellow;");
  }

  public static String getIndexPrefix() {
    return DwClientConfiguration.getInstance().getParameter(IQueryKeys.PARAM_INDEXER_INDEX_PREFIX,
            "");
  }

  public static boolean queryUseCache() {
    return DwClientConfiguration.getInstance().getBooleanParameter(IQueryKeys.PARAM_USE_CACHE,
            true);
  }

  public static String queryHighlightPost() {
    return DwClientConfiguration.getInstance().getParameter(IQueryKeys.PARAM_HIT_HIGHLIGHT_POST,
            "</match>");
  }

  public static boolean lockDWForUpdate() {
    return DwClientConfiguration.getInstance()
            .getBooleanParameter(IQueryKeys.PARAM_LOCK_DW_FOR_UPDATE, false);
  }

  public static String getCatalogClientProviderClass() {
    return DwClientConfiguration.getInstance().getParameter(
            IQueryKeys.PARAM_CATALOG_CLIENT_PROVIDER_CLASS, "CombinedCatalogClientManager");
  }

  public static Integer getAttrIdToIndex() {
    int projectsString = DwClientConfiguration.getInstance()
            .getIntegerParameter(IQueryKeys.PARAM_INDEXER_ATTRID_TO_INDEX, 0);
    return projectsString;
  }

  public static Long getPIDToIndex() {
    long projectsString = DwClientConfiguration.getInstance()
            .getLongParameter(IQueryKeys.PARAM_INDEXER_PID_TO_INDEX, 0L);
    return projectsString;
  }

  public static boolean cleanCatalogIndex() {
    return DwClientConfiguration.getInstance()
            .getBooleanParameter(IQueryKeys.PARAM_INDEXER_CLEAN_CATALOG_INDEX, true);
  }

  public static boolean doParallelIndexing() {
    return DwClientConfiguration.getInstance()
            .getBooleanParameter(IQueryKeys.PARAM_INDEXER_DO_PARALLEL_INDEXING, true);
  }

  public static boolean queryAlwaysGroupDistinctQueriesOnDocLevel() {
    return DwClientConfiguration.getInstance()
            .getBooleanParameter(IQueryKeys.PARAM_QUERY_ALWAYS_GROUP_DISTINCT_QUERIES_ON_DOC_LEVEL, false);
  }

  @Override
  public void clear() {
    adapterFactory = null;
    catalogClientFactory = null;
    catalogClientManager = null;
  }

  public ICatalogClientManager getCatalogClientManager() throws SQLException {
    if ((catalogClientManager == null) || (catalogClientManager.isDisposed())) {
      catalogClientManager = getCatalogClientFactory().getCatalogClientManager();
    }
    return catalogClientManager;
  }

  public ICatalogClientManagerFactory getCatalogClientFactory() throws SQLException {
    if (catalogClientFactory == null) {
      String adapterFactoryClass = getCatalogClientFactoryClass();

      try {
        catalogClientFactory = (ICatalogClientManagerFactory) getClass().getClassLoader()
                .loadClass(adapterFactoryClass).getConstructor().newInstance();
      } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
              | InvocationTargetException | NoSuchMethodException | SecurityException
              | ClassNotFoundException e) {
        throw new SQLException(e);
      }

    }
    return catalogClientFactory;
  }

  private String getCatalogClientFactoryClass() {
    // this is a bad hack that sets the combined manager as the default when there is some other
    // solr stuff configured, because it actually is currently our default manager. solr stuff
    // should normally not exists in query.model !
    String defaultValue = "de.uniwue.dw.query.solr.model.manager.CombinedCatalogClientManagerFactory";
    if (getParameter("solr.server_url") == null) {
      defaultValue = "de.uniwue.dw.query.model.client.CompleteCatalogClientManagerFactory";
    }
    return getParameter(IDWClientKeys.PARAM_CATALOG_CLIENT_FACTORY_CLASS, defaultValue);
  }

  public IQueryAdapterFactory getQueryAdapterFactory() throws SQLException {
    if (adapterFactory == null) {
      String adapterFactoryClass = getQueryAdapterFactoryClass();
      try {
        adapterFactory = (IQueryAdapterFactory) getClass().getClassLoader()
                .loadClass(adapterFactoryClass).getConstructor().newInstance();
      } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
              | InvocationTargetException | NoSuchMethodException | SecurityException
              | ClassNotFoundException e) {
        throw new SQLException(e);
      }
    }
    return adapterFactory;
  }

  public String getQueryAdapterFactoryClass() throws SQLException {
    String defaultValue = null;
    if (SQLPropertiesConfiguration.getInstance().getSQLConfig().dbType == DBType.MSSQL) {
      defaultValue = "de.uniwue.dw.query.model.sql.MSSQLQueryAdapterFactory";
    } else if (SQLPropertiesConfiguration.getInstance().getSQLConfig().dbType == DBType.MySQL) {
      defaultValue = "de.uniwue.dw.query.model.sql.MySQLQueryAdapterFactory";
    }
    return getParameter(IQueryKeys.PARAM_QUERY_ADAPTER_FACTORY_CLASS, defaultValue);
  }

}
