package de.uniwue.dw.query.solr.model.manager;

import java.sql.SQLException;

import de.uniwue.dw.core.client.api.configuration.ICatalogClientManagerFactory;
import de.uniwue.dw.core.model.manager.ICatalogClientManager;

public class CombinedCatalogClientManagerFactory implements ICatalogClientManagerFactory {

  @Override
  public ICatalogClientManager getCatalogClientManager() throws SQLException {
    return new CombinedCatalogClientManager();
  }

}
