package de.uniwue.dw.core.client.api.configuration;

import de.uniwue.dw.core.client.authentication.group.AuthManager;
import de.uniwue.dw.core.model.hooks.IDwCatalogHooks;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.model.manager.ICatalogClientManager;
import de.uniwue.dw.core.model.manager.InfoManager;
import de.uniwue.dw.core.model.manager.SystemManager;
import de.uniwue.dw.core.model.manager.adapter.IDWClientAdapterFactory;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;
import de.uniwue.misc.util.ConfigException;
import de.uniwue.misc.util.Configuration;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

//import de.uniwue.misc.sql.ISqlConfigKeys;
//import de.uniwue.misc.sql.SQLPropertiesConfiguration;

/**
 * <p>
 * This singleton is the central place to configure and access all relevant parameters for a data
 * warehouse client connection which provides SQL and solr managers.
 * </p>
 * <p>
 * When the system environment variable MISBASED_DW_CONFIG_DIR is set to a folder with appropriate
 * *.property files, they can be found and loaded with static methods of this class.
 * </p>
 * Otherwise the porperties can be loaded with loadProperties(File)
 */
public class DwClientConfiguration extends Configuration implements IDWClientKeys {

  private static DwClientConfiguration instance = null;

  private static SpecialCatalogEntries specialCatalogEntries;

  private SystemManager systemManager;

  private CatalogManager catalogManager;

  private InfoManager infoManager;

  private AuthManager authManager;

  private IDWClientAdapterFactory adapterFactory;

  protected DwClientConfiguration() {
  }

  public static synchronized DwClientConfiguration getInstance() {
    if (instance == null) {
      instance = new DwClientConfiguration();
      singletons.add(instance);
    }
    return instance;
  }

  public static String getLDAPServer() {
    return getInstance().getParameter(IDWClientKeys.PARAM_LDAP_SERVER);
  }

  public static String getLDAPou() {
    return getInstance().getParameter(IDWClientKeys.PARAM_LDAP_OU);
  }

  public static String getLDAPGroupOU() {
    return getInstance().getParameter(IDWClientKeys.PARAM_LDAP_GROUP_OU);
  }

  public static String getLDAPdc() {
    return getInstance().getParameter(IDWClientKeys.PARAM_LDAP_DC);
  }

  public boolean shouldFixInvalidUniqueNamesInDatabase() {
    return getBooleanParameter(IDWClientKeys.PARAM_CATALOG_CLIENT_SHOULD_FIX_INVALID_UNIQUE_NAMES_IN_DATABASE, true);
  }

  @Override
  public void clear() throws ConfigException {
    super.clear();
    catalogManager = null;
    infoManager = null;
    authManager = null;
    adapterFactory = null;
  }

  public int getDefaultKAnonymity() {
    return getIntegerParameter(IDWClientKeys.PARAM_K_ANONYMITY, 10);
  }

  public boolean doSQLUpdates() {
    return getBooleanParameter(IDWClientKeys.PARAM_DO_SQL_UPDATES, false);
  }

  public boolean createInfoIndices() {
    return getBooleanParameter(IDWClientKeys.PARAM_CREATE_INFO_INDICES, false);
  }

  public boolean createInfoIndexOnPID() {
    return getBooleanParameter(IDWClientKeys.PARAM_CREATE_INFO_INDEX_ON_PID, true);
  }

  public boolean createMSSQLFulltextCatalog() {
    return getBooleanParameter(IDWClientKeys.PARAM_CREATE_MSSQL_FULLTEXT_CATALOG, true);
  }

  public boolean useFulltextIndex() {
    return getBooleanParameter(IDWClientKeys.PARAM_USE_FULLTEXT_INDEX, false);
  }

  public IDWClientAdapterFactory getClientAdapterFactory() throws SQLException {
    if (adapterFactory == null) {
      String adapterFactoryClass = getClientAdapterFactoryClass();
      try {
        adapterFactory = (IDWClientAdapterFactory) getClass().getClassLoader()
                .loadClass(adapterFactoryClass).getConstructor().newInstance();
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
    return adapterFactory;
  }

  public InfoManager getInfoManager() throws SQLException {
    if (getInstance().infoManager == null) {
      IDWClientAdapterFactory adapterFactory = getClientAdapterFactory();
      getInstance().infoManager = adapterFactory.createInfoManager();
    }
    return getInstance().infoManager;
  }

  public AuthManager getAuthManager() throws SQLException {
    if (authManager == null) {
      IDWClientAdapterFactory adapterFactory = getClientAdapterFactory();
      authManager = adapterFactory.createAuthManager();
    }
    return authManager;
  }

  public CatalogManager getCatalogManager() throws SQLException {
    if (getInstance().catalogManager == null) {
      IDWClientAdapterFactory adapterFactory = getClientAdapterFactory();
      getInstance().catalogManager = adapterFactory.createCatalogManager();
    }
    return getInstance().catalogManager;
  }

  public SystemManager getSystemManager() {
    if (getInstance().systemManager == null) {
      try {
        getInstance().systemManager = new SystemManager();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    return getInstance().systemManager;
  }

  public boolean useProketPasswords() {
    return getBooleanParameter(PARAM_USE_PROKET_PASSWORD, false);
  }

  public SpecialCatalogEntries getSpecialCatalogEntries() {
    return specialCatalogEntries;
  }

  public String getDocumentExtID() {
    return getParameter(IDWClientKeys.PARAM_DOCUMENT_ID_EXTID, IDwCatalogHooks.EXT_HOOK_CASE_ID);
  }

  public String getDocumentProject() {
    return getParameter(IDWClientKeys.PARAM_DOCUMENT_ID_PROJECT,
            IDwCatalogHooks.PROJECT_HOOK_CASE_ID);
  }

  public String getDocumentGroupExtID() {
    return getParameter(IDWClientKeys.PARAM_DOCUMENT_GROUP_ID_EXTID,
            IDwCatalogHooks.EXT_HOOK_PATIENT_ID);
  }

  public String getDocumentGroupProject() {
    return getParameter(IDWClientKeys.PARAM_DOCUMENT_GROUP_ID_PROJECT,
            IDwCatalogHooks.PROJECT_HOOK_PATIENT_ID);
  }

  public String getDocumentTimeExtID() {
    return getParameter(IDWClientKeys.PARAM_DOCUMENT_TIME_EXTID, "");
  }

  public String getDocumentTimeProject() {
    return getParameter(IDWClientKeys.PARAM_DOCUMENT_TIME_PROJECT, "");
  }

  public String getDocumentSuggesterExtID() {
    return getParameter(IDWClientKeys.PARAM_DOCUMENT_SUGGESTER_EXTID, "");
  }

  public String getDocumentSuggesterProject() {
    return getParameter(IDWClientKeys.PARAM_DOCUMENT_SUGGESTER_PROJECT, "");
  }

  public String getMostCommonTextFieldExtID() {
    return getParameter(IDWClientKeys.PARAM_MOST_COMMON_TEXT_FIELD_EXTID, "");
  }

  public String getMostCommonTextFieldProject() {
    return getParameter(IDWClientKeys.PARAM_MOST_COMMON_TEXT_FIELD_PROJECT, "");
  }

  public String getMostCommonTextFieldXExtID(int index) {
    return getParameter(
            IDWClientKeys.PARAM_MOST_COMMON_TEXT_FIELDX_EXTID.replace("XX", index + ""));
  }

  public String getMostCommonTextFieldXProject(int index) {
    return getParameter(
            IDWClientKeys.PARAM_MOST_COMMON_TEXT_FIELDX_PROJECT.replace("XX", index + ""));
  }

  public String getClientAdapterFactoryClass() throws SQLException {
    String defaultValue = null;
    if (SQLPropertiesConfiguration.getInstance().getSQLConfig().dbType == DBType.MSSQL) {
      defaultValue = "de.uniwue.dw.core.sql.mssql.MSSQLAdapterFactory";
    } else if (SQLPropertiesConfiguration.getInstance().getSQLConfig().dbType == DBType.MySQL) {
      defaultValue = "de.uniwue.dw.core.sql.mysql.MySQLAdapterFactory";
    }
    return getParameter(IDWClientKeys.PARAM_CLIENT_ADAPTER_FACTORY_CLASS, defaultValue);
  }

  public SpecialCatalogEntries setSpecialCatalogEntries(
          ICatalogClientManager catalogClientManager) {
    SpecialCatalogEntriesBuilder builder = new SpecialCatalogEntriesBuilder(catalogClientManager);
    builder.setDocumentID(getDocumentExtID(), getDocumentProject());
    builder.setDocumentGroupId(getDocumentGroupExtID(), getDocumentGroupProject());
    builder.setDocumentTime(getDocumentTimeExtID(), getDocumentTimeProject());
    builder.setSuggester(getDocumentSuggesterExtID(), getDocumentSuggesterProject());
    List<String[]> commonTextFields = new ArrayList<>();
    String[] mostCommonTextField = { getMostCommonTextFieldExtID(),
            getMostCommonTextFieldProject() };
    commonTextFields.add(mostCommonTextField);
    int index = 2;
    while (getMostCommonTextFieldXExtID(index) != null) {
      String extid = getMostCommonTextFieldXExtID(index);
      String project = getMostCommonTextFieldXProject(index);
      String[] pair = { extid, project };
      commonTextFields.add(pair);
      index++;
    }
    builder.setMostCommonTextFields(commonTextFields);
    specialCatalogEntries = builder.build();
    return specialCatalogEntries;
  }

}
