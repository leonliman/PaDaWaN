package de.uniwue.dw.core.sql.mysql;

import java.io.File;
import java.sql.SQLException;

import de.uniwue.dw.core.client.authentication.group.AuthManager;
import de.uniwue.dw.core.client.authentication.group.IGroupAdapter;
import de.uniwue.dw.core.client.authentication.group.IGroupCasePermissionAdapter;
import de.uniwue.dw.core.client.authentication.group.IUserInGroupAdapter;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.model.manager.InfoManager;
import de.uniwue.dw.core.model.manager.adapter.ICatalogAdapter;
import de.uniwue.dw.core.model.manager.adapter.ICatalogChoiceAdapter;
import de.uniwue.dw.core.model.manager.adapter.ICatalogCountAdapter;
import de.uniwue.dw.core.model.manager.adapter.ICatalogNumDataAdapter;
import de.uniwue.dw.core.model.manager.adapter.IDWClientAdapterFactory;
import de.uniwue.dw.core.model.manager.adapter.IDeleteAdapter;
import de.uniwue.dw.core.model.manager.adapter.IInfoAdapter;
import de.uniwue.dw.core.sql.SQLAdapterFactory;
import de.uniwue.dw.core.sql.SQLCatalogChoiceAdapter;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;

public class MySQLAdapterFactory extends SQLAdapterFactory implements IDWClientAdapterFactory {

  public MySQLAdapterFactory() {
  }

  @Override
  public ICatalogAdapter createCatalogAdapter(CatalogManager catalogManager) throws SQLException {
    try {
      return new MySQLCatalogAdapter(SQLPropertiesConfiguration.getInstance().getSQLManager(),
              catalogManager);
    } catch (SQLException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public ICatalogChoiceAdapter createCatalogChoiceAdapter(CatalogManager catalogManager)
          throws SQLException {
    try {
      return new SQLCatalogChoiceAdapter(SQLPropertiesConfiguration.getInstance().getSQLManager(),
              catalogManager);
    } catch (SQLException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public ICatalogCountAdapter createCatalogCountAdapter(CatalogManager catalogManager)
          throws SQLException {
    try {
      return new MySQLCatalogCountAdapter(SQLPropertiesConfiguration.getInstance().getSQLManager(),
              catalogManager);
    } catch (SQLException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public ICatalogNumDataAdapter createCatalogNumDataAdapter(CatalogManager catalogManager)
          throws SQLException {
    try {
      return new MySQLCatalogNumDataAdapter(
              SQLPropertiesConfiguration.getInstance().getSQLManager(), catalogManager);
    } catch (SQLException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public IDeleteAdapter createDeleteAdapter(InfoManager infoManager) throws SQLException {
    try {
      return new MySQLDeleteAdapter(SQLPropertiesConfiguration.getInstance().getSQLManager());
    } catch (SQLException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public IInfoAdapter createInfoAdapter(InfoManager anInfoManager, File bulkInsertFolder)
          throws SQLException {
    try {
      return new MySQLInfoAdapter(SQLPropertiesConfiguration.getInstance().getSQLManager(),
              bulkInsertFolder);
    } catch (SQLException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public IGroupAdapter createGroupAdapter(AuthManager authManager) throws SQLException {
    return new MySQLGroupAdapter(SQLPropertiesConfiguration.getInstance().getSQLManager());
  }

  @Override
  public IGroupCasePermissionAdapter createGroupCasePermissionAdapter(AuthManager authManager)
          throws SQLException {
    return new MySQLGroupCasePermissionAdapter(
            SQLPropertiesConfiguration.getInstance().getSQLManager());
  }

  @Override
  public IUserInGroupAdapter createUserInGroupAdapter(AuthManager authManager) throws SQLException {
    return new MySQLUserInGroupAdapter(SQLPropertiesConfiguration.getInstance().getSQLManager());
  }

}
