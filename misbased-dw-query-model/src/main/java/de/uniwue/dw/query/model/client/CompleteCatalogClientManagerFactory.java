package de.uniwue.dw.query.model.client;

import java.sql.SQLException;

import de.uniwue.dw.core.client.api.configuration.ICatalogClientManagerFactory;
import de.uniwue.dw.core.model.manager.CompleteCatalogClientManager;
import de.uniwue.dw.core.model.manager.ICatalogClientManager;
import de.uniwue.misc.util.ConfigException;

public class CompleteCatalogClientManagerFactory  implements ICatalogClientManagerFactory{

  @Override
  public ICatalogClientManager getCatalogClientManager() throws SQLException {
    return new CompleteCatalogClientManager();
  }

}
