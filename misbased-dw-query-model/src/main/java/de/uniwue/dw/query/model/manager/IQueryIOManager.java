package de.uniwue.dw.query.model.manager;

import java.util.List;

import de.uniwue.dw.core.model.manager.ICatalogAndTextSuggester;
import de.uniwue.dw.core.model.manager.ICatalogClientManager;
import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.data.RawQuery;
import de.uniwue.dw.query.model.data.StoredQueryTreeEntry;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.quickSearch.QueryQuickSearchRepresentation;
import de.uniwue.dw.query.model.table.QueryTableRepresentation;

public interface IQueryIOManager {

  public void refresh();

  public ICatalogClientManager getCatalogClientManager();

  public RawQuery getQuery(String name);

  public RawQuery getQuery(int aQueryID);

  public StoredQueryTreeEntry getStoredQueryEntry(String name);

  public QueryTableRepresentation loadQuery(String name) throws GUIClientException;

  public QueryTableRepresentation loadQuery(int queryID) throws GUIClientException;

  public StoredQueryTreeEntry getRoot();

  public void add(RawQuery aQuery);

  public RawQuery updateEntry(String name, String xml, String user) throws QueryException;

  public RawQuery insert(String name, String xml, String folder, String user) throws QueryException;

  public void renameEntry(String oldNameWithFolders, String newNameWithFolders)
          throws QueryException;

  public void deleteEntry(String name) throws QueryException;

  public List<RawQuery> getOrderedRawQueries();

  public StoredQueryTreeEntry getStoredQueryEntry(int id);

  public QueryQuickSearchRepresentation loadQueryInQuickSearchRepresentation(String name,
          ICatalogAndTextSuggester suggester) throws GUIClientException;

  public QueryRoot read(String xmlQuery) throws QueryException;

  /*
   * Dispose the respective query engine and free all resources that have been used by it
   */
  public void dispose();

}
