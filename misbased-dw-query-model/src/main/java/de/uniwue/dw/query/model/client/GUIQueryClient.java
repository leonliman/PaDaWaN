package de.uniwue.dw.query.model.client;

import java.sql.SQLException;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.group.AuthManager;
import de.uniwue.dw.core.model.manager.ICatalogClientManager;
import de.uniwue.dw.query.model.DWQueryConfig;
import de.uniwue.dw.query.model.manager.IQueryClientIOManager;
import de.uniwue.dw.query.model.manager.QueryClientIOManager;

public abstract class GUIQueryClient implements IGUIClient {

  protected IQueryClientIOManager queryClientIOManager;

  protected AuthManager userManager;

  protected ICatalogClientManager catalogClientManager;

  private IQueryRunner queryRunner;

  public GUIQueryClient() throws GUIClientException {
    try {
      catalogClientManager = createCatalogClientManager();
      if (DWQueryConfig.hasToIndexCatalog()) {
        DwClientConfiguration.getInstance().setSpecialCatalogEntries(catalogClientManager);
      }
      userManager = DwClientConfiguration.getInstance().getAuthManager();
      queryClientIOManager = createQueryIOManager();
      queryRunner = createQueryRunner();
    } catch (SQLException e) {
      throw new GUIClientException(e);
    }
  }

  protected IQueryClientIOManager createQueryIOManager() throws GUIClientException, SQLException {
    return new QueryClientIOManager(this);
  }

  @Override
  public ICatalogClientManager getCatalogClientProvider() {
    return this.catalogClientManager;
  }

  @Override
  public AuthManager getUserManager() {
    return userManager;
  }

  @Override
  public IQueryClientIOManager getQueryClientIOManager() {
    return queryClientIOManager;
  }

  public abstract IQueryRunner createQueryRunner() throws GUIClientException;

  @Override
  public IQueryRunner getQueryRunner() {
    return queryRunner;
  }

  @Override
  public void dispose() throws GUIClientException {
    if (queryClientIOManager != null)
      queryClientIOManager.dispose();
    queryClientIOManager = null;
    if (queryRunner != null) {
      queryRunner.dispose();
    }
    queryRunner = null;
  }

  protected ICatalogClientManager createCatalogClientManager() throws GUIClientException {
    return createCompleteCatalogClientManager();
  }

  protected ICatalogClientManager createCompleteCatalogClientManager() throws GUIClientException {
    try {
      return DWQueryConfig.getInstance().getCatalogClientManager();
    } catch (SQLException e) {
      e.printStackTrace();
      throw new GUIClientException(e);
    }
  }
}
