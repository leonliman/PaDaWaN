package de.uniwue.dw.query.solr;

import java.sql.SQLException;

import de.uniwue.dw.core.model.manager.CompleteCatalogClientManager;
import de.uniwue.dw.core.model.manager.ICatalogClientManager;
import de.uniwue.dw.query.model.DWQueryConfig;
import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.client.GUIQueryClient;
import de.uniwue.dw.query.model.client.IGUIClient;
import de.uniwue.dw.query.model.client.IQueryRunner;
import de.uniwue.dw.query.model.quickSearch.suggest.IInputSuggester;
import de.uniwue.dw.query.model.quickSearch.suggest.Suggester;
import de.uniwue.dw.query.solr.client.ISearchEngineGUIClient;
import de.uniwue.dw.query.solr.model.manager.CombinedCatalogClientManager;
import de.uniwue.dw.query.solr.model.manager.SolrCatalogClientManager;
import de.uniwue.dw.query.solr.suggest.IndexLookupUtils;
import de.uniwue.dw.solr.api.DWSolrConfig;

public class SolrGUIClient extends GUIQueryClient implements IGUIClient, ISearchEngineGUIClient {

  protected IInputSuggester suggester;

  public SolrGUIClient() throws GUIClientException {
    super();
    DWSolrConfig.getInstance().getSolrManager(); // just a check if it works
    IndexLookupUtils indexLookupUtils = DWSolrConfig.getInstance().getIndexLookupUtils();
    this.suggester = new Suggester(indexLookupUtils);
  }

  @Override
  public IInputSuggester getSuggester() {
    return suggester;
  }

  @Override
  public IQueryRunner createQueryRunner() {
    return new SolrQueryRunner(this);
  }

  @Override
  protected ICatalogClientManager createCatalogClientManager() throws GUIClientException {
    String catalogClientProviderClass = DWQueryConfig.getCatalogClientProviderClass();
    if (catalogClientProviderClass
            .equalsIgnoreCase(SolrCatalogClientManager.class.getSimpleName())) {
      return SolrCatalogClientManager.getInst();
    } else if (catalogClientProviderClass
            .equalsIgnoreCase(CompleteCatalogClientManager.class.getSimpleName())) {
      try {
        return DWQueryConfig.getInstance().getCatalogClientManager();
      } catch (SQLException e) {
        throw new GUIClientException(e);
      }
    } else if (catalogClientProviderClass
            .equalsIgnoreCase(CombinedCatalogClientManager.class.getSimpleName())) {
      try {
        return DWQueryConfig.getInstance().getCatalogClientManager();
      } catch (SQLException e) {
        throw new GUIClientException(e);
      }
    } else {
      throw new IllegalArgumentException(
              "No valid definition of CatalogClientProvide class: " + catalogClientProviderClass);
    }
  }

}
