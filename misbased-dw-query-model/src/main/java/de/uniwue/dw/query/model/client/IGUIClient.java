package de.uniwue.dw.query.model.client;

import de.uniwue.dw.core.model.manager.ICatalogClientManager;
import de.uniwue.dw.core.model.manager.adapter.IUserManager;
import de.uniwue.dw.query.model.manager.IQueryClientIOManager;

public interface IGUIClient {

  ICatalogClientManager getCatalogClientProvider();

  IQueryClientIOManager getQueryClientIOManager();

  IQueryRunner getQueryRunner() throws GUIClientException;

  IUserManager getUserManager() throws GUIClientException;

  /*
   * Dispose the respective query engine and free all resources that have been used by it
   */
  void dispose() throws GUIClientException;

}
