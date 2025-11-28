package de.uniwue.dw.query.model.manager;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.manager.ICatalogAndTextSuggester;
import de.uniwue.dw.query.model.DWQueryConfig;
import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.client.GUIQueryClient;
import de.uniwue.dw.query.model.client.IGUIClient;
import de.uniwue.dw.query.model.client.UserPrivileges;
import de.uniwue.dw.query.model.data.LoggedQuery;
import de.uniwue.dw.query.model.data.RawQuery;
import de.uniwue.dw.query.model.data.StoredQueryTreeEntry;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.manager.adapter.IQueryLogAdapter;
import de.uniwue.dw.query.model.quickSearch.ModelConverter;
import de.uniwue.dw.query.model.quickSearch.QueryQuickSearchRepresentation;
import de.uniwue.dw.query.model.table.QueryTableRepresentation;

public class QueryClientIOManager implements IQueryClientIOManager {

  protected IGUIClient guiClient;

  protected IQueryIOManager queryIOManager;

  protected IQueryLogAdapter queryLogAdapter;

  public QueryClientIOManager(GUIQueryClient guiClient) throws SQLException {
    this.guiClient = guiClient;
    queryLogAdapter = DWQueryConfig.getInstance().getQueryAdapterFactory().createQueryLogAdapter();
    this.queryIOManager = new QueryIOManager(guiClient.getCatalogClientProvider());
  }

  @Override
  public StoredQueryTreeEntry getStoredQueryTreeEntry(String name) {
    return queryIOManager.getStoredQueryEntry(name);
  }

  @Override
  public StoredQueryTreeEntry getStoredQueryTreeEntry(int id) {
    return queryIOManager.getStoredQueryEntry(id);
  }

  @Override
  public QueryTableRepresentation loadQuery(String name) throws GUIClientException {
    return this.queryIOManager.loadQuery(name);
  }

  @Override
  public QueryTableRepresentation loadQuery(int queryID) throws GUIClientException {
    return this.queryIOManager.loadQuery(queryID);
  }

  @Override
  public QueryQuickSearchRepresentation loadQueryInQuickSearchRepresentation(String name,
          ICatalogAndTextSuggester suggester) throws GUIClientException {
    return this.queryIOManager.loadQueryInQuickSearchRepresentation(name, suggester);
  }

  @Override
  public RawQuery saveQuery(String name, String xml, String folder, String user)
          throws GUIClientException {
    try {
      RawQuery rawQuery = queryIOManager.insert(name, xml, folder, user);
      queryIOManager.refresh();
      return rawQuery;
    } catch (QueryException e) {
      throw new GUIClientException(e);
    }
  }

  @Override
  public void renameQuery(String oldNameWithFolders, String newNameWithFolders)
          throws GUIClientException {
    try {
      queryIOManager.renameEntry(oldNameWithFolders, newNameWithFolders);
    } catch (QueryException e) {
      throw new GUIClientException(e);
    }
  }

  @Override
  public void deleteQuery(String name) throws GUIClientException {
    try {
      queryIOManager.deleteEntry(name);
    } catch (QueryException e) {
      throw new GUIClientException(e);
    }
  }

  @Override
  public RawQuery updateQuery(String name, String xml, String user) throws GUIClientException {
    try {
      return queryIOManager.updateEntry(name, xml, user);
    } catch (QueryException e) {
      throw new GUIClientException(e);
    }
  }

  @Override
  public QueryRoot read(String xmlQuery) throws GUIClientException {
    try {
      return queryIOManager.read(xmlQuery);
    } catch (QueryException e) {
      throw new GUIClientException(e);
    }
  }

  @Override
  public RawQuery getQuery(String name) {
    return queryIOManager.getQuery(name);
  }

  private List<RawQuery> filter(List<RawQuery> list, User user) {
    return list.stream().filter(UserPrivileges.entitledForQuery(user)).collect(Collectors.toList());
  }

  public List<RawQuery> getOrderedRawQueries(User user) {
    return filter(queryIOManager.getOrderedRawQueries(), user);
  }

  public StoredQueryTreeEntry getStoredQueriesRootEntry() {
    return queryIOManager.getRoot();
  }

  public List<StoredQueryTreeEntry> getChildren(StoredQueryTreeEntry parent, User user) {
    return parent.getChilds().stream().filter(UserPrivileges.entitledForStoredQuery(user))
            .collect(Collectors.toList());
  }

  public StoredQueryTreeEntry getStoredQueryTreeForUser(User user) {
    StoredQueryTreeEntry newTreeroot = queryIOManager.getRoot().copy();
    createNewSubTreeForUser(newTreeroot, user);
    return newTreeroot;
  }

  @Override
  public Optional<StoredQueryTreeEntry> getStoredQueryForUser(String storedQueyPath, User user) {
    List<StoredQueryTreeEntry> queue = new LinkedList<>();
    queue.add(queryIOManager.getRoot());
    while (!queue.isEmpty()) {
      StoredQueryTreeEntry elem = queue.remove(0);
      if (elem.getPath().equals(storedQueyPath))
        return Optional.of(elem);
      else
        queue.addAll(getChildren(elem, user));
    }
    return Optional.empty();
  }

  private void createNewSubTreeForUser(StoredQueryTreeEntry parent, User user) {
    List<StoredQueryTreeEntry> originalTreechilds = getChildren(parent, user);
    List<StoredQueryTreeEntry> newTreeChilds = originalTreechilds.stream().map(n -> n.copy())
            .collect(Collectors.toList());
    newTreeChilds.forEach(n -> n.setParent(parent));
    parent.setChilds(newTreeChilds);
    newTreeChilds.forEach(n -> createNewSubTreeForUser(n, user));
  }

  @Override
  public List<RawQuery> getOrderedRawQueries() {
    return queryIOManager.getOrderedRawQueries();
  }

  @Override
  public List<LoggedQuery> getLoggedQueries(User user, int numberOfQueries)
          throws GUIClientException {
    try {
      return queryLogAdapter.getLatestLoggedQueries(user.getUsername(), numberOfQueries);
    } catch (SQLException e) {
      e.printStackTrace();
      throw new GUIClientException(e);
    }
  }

  @Override
  public QueryRoot loadLoggedQuery(int logID, User user) throws GUIClientException {
    try {
      LoggedQuery query = queryLogAdapter.getLoggedQueryByID(logID);
      if (query == null) {
        throw new GUIClientException("QueryID does not exist.");
      } else {
        if (user.isAdmin() || user.getUsername().equalsIgnoreCase(query.getUserName())) {
          QueryRoot queryRoot = this.read(query.getXml());
          return queryRoot;
        } else {
          throw new GUIClientException("User does not have the rights to load that query");
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      throw new GUIClientException(e);
    }
  }

  @Override
  public QueryQuickSearchRepresentation loadLoggedQueryInQuickSearchRepresentation(int logID,
          User user, ICatalogAndTextSuggester suggester) throws GUIClientException {
    QueryRoot queryRoot = loadLoggedQuery(logID, user);
    return ModelConverter.queryRoot2QuickSearchRepresentation(queryRoot, suggester);
  }

  @Override
  public IQueryIOManager getQueryIOManager() {
    return this.queryIOManager;
  }

  @Override
  public void dispose() {
    if (queryIOManager != null)
      queryIOManager.dispose();
    queryIOManager = null;
  }

  @Override
  public void logQuery(String xml, String user, int queryID, String exportType, String engineName,
          String engineVersion) throws GUIClientException {
    try {
      if (!DWQueryConfig.queryUserIgnoreLogging().toLowerCase().equals(user.toLowerCase())) {
        queryLogAdapter.insert(xml, user, queryID, exportType, engineName, engineVersion);
      }
    } catch (SQLException e) {
      throw new GUIClientException(e);
    }
  }

  @Override
  public void updateLogQuery(String user, int queryID, long resultCount, long duration)
          throws GUIClientException {
    try {
      if (!DWQueryConfig.queryUserIgnoreLogging().toLowerCase().equals(user.toLowerCase())) {
        queryLogAdapter.updateEntry(queryID, resultCount, duration);
      }
    } catch (SQLException e) {
      throw new GUIClientException(e);
    }
  }

}
