package de.uniwue.dw.core.sql;

import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.core.client.authentication.group.AuthManager;
import de.uniwue.dw.core.client.authentication.group.IGroupCatalogPermissionAdapter;
import de.uniwue.dw.core.client.authentication.group.IUserAdapter;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.model.manager.InfoManager;
import de.uniwue.dw.core.model.manager.adapter.ICatalogChoiceAdapter;
import de.uniwue.dw.core.model.manager.adapter.IDWClientAdapterFactory;
import de.uniwue.dw.core.model.manager.adapter.IUserSettingsAdapter;
import de.uniwue.misc.sql.IParamsAdapter;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLParamsAdapter;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;

public abstract class SQLAdapterFactory implements IDWClientAdapterFactory, IDwSqlSchemaConstant {

  private static Logger logger = LogManager.getLogger(SQLAdapterFactory.class);

  public SQLAdapterFactory() {
  }

  @Override
  public CatalogManager createCatalogManager() throws SQLException {
    CatalogManager catalogManager = new CatalogManager(this);
    logger.debug("Created CatalogManager");
    return catalogManager;
  }

  @Override
  public ICatalogChoiceAdapter createCatalogChoiceAdapter(CatalogManager catalogManager)
          throws SQLException {
    try {
      SQLCatalogChoiceAdapter sqlCatalogChoiceAdapter = new SQLCatalogChoiceAdapter(
              SQLPropertiesConfiguration.getInstance().getSQLManager(), catalogManager);
      logger.debug("Created SQLCatalogChoiceAdapter");
      return sqlCatalogChoiceAdapter;
    } catch (SQLException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public InfoManager createInfoManager() throws SQLException {
    InfoManager infoManager = new InfoManager(this,
            SQLPropertiesConfiguration.getSQLBulkImportDir());
    logger.debug("Created InfoManager");
    return infoManager;
  }

  @Override
  public AuthManager createAuthManager() throws SQLException {
    AuthManager authManager = new AuthManager(this);
    logger.debug("Created AuthManager");
    return authManager;
  }

  @Override
  public IGroupCatalogPermissionAdapter createGroupCatalogPermissionAdapter(AuthManager authManager)
          throws SQLException {
    try {
      SQLGroupCatalogPermission sqlGroupCatalogPermission = new SQLGroupCatalogPermission(
              SQLPropertiesConfiguration.getInstance().getSQLManager());
      logger.debug("Created SQLGroupCatalogPermission");
      return sqlGroupCatalogPermission;
    } catch (SQLException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public IUserAdapter createUserAdapter(AuthManager authManager) throws SQLException {
    SQLUserAdapter sqlUserAdapter = new SQLUserAdapter(
            SQLPropertiesConfiguration.getInstance().getSQLManager());

    return sqlUserAdapter;
  }

  @Override
  public IParamsAdapter createParamsAdapter() throws SQLException {
    SQLParamsAdapter sqlParamsAdapter = new SQLParamsAdapter(
            SQLPropertiesConfiguration.getInstance().getSQLManager(), T_SYSTEM_PARAMS);
    logger.debug("Created SQLParamsAdapter");
    return sqlParamsAdapter;
  }

  @Override
  public IUserSettingsAdapter createUserSettingsAdapter() throws SQLException {
    SQLUserSettingsAdapter sqlUserSettingsAdapter = new SQLUserSettingsAdapter(
            SQLPropertiesConfiguration.getInstance().getSQLManager());
    logger.debug("Created SQLUserSettingsAdapter");
    return sqlUserSettingsAdapter;
  }

  @Override
  public void dropDataTables() throws SQLException {
    SQLManager sqlManager = SQLPropertiesConfiguration.getInstance().getSQLManager();
    if (sqlManager.tableExists(T_REF_ID)) {
      sqlManager.dropTable(T_REF_ID);
    }
    if (sqlManager.tableExists(T_IE_MEMORY)) {
      sqlManager.dropTable(T_IE_MEMORY);
    }
    if (sqlManager.tableExists(T_IE_SPANS)) {
      sqlManager.dropTable(T_IE_SPANS);
    }
    if (sqlManager.tableExists(T_ERROR_LOG)) {
      sqlManager.dropTable(T_ERROR_LOG);
    }
    logger.info("dropped data tables");
  }

}
