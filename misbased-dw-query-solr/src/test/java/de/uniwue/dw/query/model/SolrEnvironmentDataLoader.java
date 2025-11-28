package de.uniwue.dw.query.model;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.model.manager.ICatalogClientManager;
import de.uniwue.dw.imports.app.ImportTestEnvironmentDataLoader;
import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.client.IGUIClient;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.dw.query.solr.SolrGUIClient;
import de.uniwue.dw.query.solr.client.SolrManager;
import de.uniwue.dw.query.solr.model.manager.CombinedCatalogClientManager;
import de.uniwue.dw.query.solr.model.manager.EmbeddedSolrManager;
import de.uniwue.dw.query.solr.preprocess.NestedDocumentIndexer;
import de.uniwue.dw.solr.api.DWSolrConfig;
import de.uniwue.misc.util.ConfigException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;

public class SolrEnvironmentDataLoader extends ImportTestEnvironmentDataLoader {

  private SolrGUIClient solrGUIClient;

  private CombinedCatalogClientManager ccc;

  public SolrEnvironmentDataLoader()
          throws GUIClientException, SQLException, IOException, URISyntaxException {
    loadConfig();
    startSolrServer();
    solrGUIClient = new SolrGUIClient();
    updateCombinedCatalogClientManager();
  }

  private void updateCombinedCatalogClientManager() throws SQLException {
    disposeCombinedCatalogClientManager();
    ICatalogClientManager ccm = DWQueryConfig.getInstance().getCatalogClientManager();
    if (ccm.getClass().isInstance(CombinedCatalogClientManager.class))
      ccc = (CombinedCatalogClientManager) ccm;
    else
      ccc = new CombinedCatalogClientManager();
  }

  @Override
  public void storeDataInStorageEngine() throws IndexException {
    index();
  }

  private void index() throws IndexException {
    DwClientConfiguration.getInstance().setProperty(IQueryKeys.PARAM_INDEXER_DELETE_INDEX, "true");
    // PropagateIndexer2 indexer = new PropagateIndexer2();
    NestedDocumentIndexer indexer = new NestedDocumentIndexer();
    indexer.update();
  }

  @Override
  public void startStorageEngine() {
  }

  private void startSolrServer() {
    SolrManager solrManager;
    boolean isEmbedded = false;
    try {
      solrManager = DWSolrConfig.getInstance().getSolrManager();
    } catch (IllegalStateException e) {
      solrManager = new EmbeddedSolrManager();
      isEmbedded = true;
    }
    DWSolrConfig.getInstance().setSolrManager(solrManager, isEmbedded);
  }

  @Override
  public IGUIClient getGuiClient() {
    return solrGUIClient;
  }

  @Override
  public ICatalogClientManager getCompleteCatalogClientManager() throws SQLException {
    return ccc;
  }

  @Override
  protected void configHasBeenChanged() throws GUIClientException, SQLException {
    disposeSolrGUIClient();
    startSolrServer();
    solrGUIClient = new SolrGUIClient();
    updateCombinedCatalogClientManager();
  }

  private void disposeCombinedCatalogClientManager() {
    if (ccc != null) {
      ccc.dispose();
      ccc = null;
    }
  }

  private void disposeSolrGUIClient() throws GUIClientException {
    if (solrGUIClient != null) {
      solrGUIClient.dispose();
      solrGUIClient = null;
    }
  }

  public void dispose() throws GUIClientException {
    disposeSolrGUIClient();
    disposeCombinedCatalogClientManager();
    try {
      DWSolrConfig.getInstance().clear();
    } catch (ConfigException e) {
      throw new GUIClientException(e);
    }
  }
}
