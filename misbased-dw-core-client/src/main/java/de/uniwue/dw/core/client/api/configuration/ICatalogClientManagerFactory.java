package de.uniwue.dw.core.client.api.configuration;

import java.sql.SQLException;

import de.uniwue.dw.core.model.manager.ICatalogClientManager;

public interface ICatalogClientManagerFactory {

  public ICatalogClientManager getCatalogClientManager() throws SQLException;
}
