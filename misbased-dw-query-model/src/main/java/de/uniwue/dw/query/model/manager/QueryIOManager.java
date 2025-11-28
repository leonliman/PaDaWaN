package de.uniwue.dw.query.model.manager;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import de.uniwue.dw.core.model.manager.ICatalogAndTextSuggester;
import de.uniwue.dw.core.model.manager.ICatalogClientManager;
import de.uniwue.dw.query.model.DWQueryConfig;
import de.uniwue.dw.query.model.QueryReader;
import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.data.RawQuery;
import de.uniwue.dw.query.model.data.StoredQueryTreeEntry;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.exception.QueryException.QueryExceptionType;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.manager.adapter.IQueryAdapter;
import de.uniwue.dw.query.model.quickSearch.ModelConverter;
import de.uniwue.dw.query.model.quickSearch.QueryQuickSearchRepresentation;
import de.uniwue.dw.query.model.table.QueryTableRepresentation;
 
public class QueryIOManager implements IQueryIOManager {

  private IQueryAdapter queryAdapter;

  public HashMap<String, RawQuery> rawQueries = new HashMap<String, RawQuery>();

  public HashMap<Integer, RawQuery> id2rawQueries = new HashMap<Integer, RawQuery>();

  private HashMap<String, StoredQueryTreeEntry> name2storedQuery = new HashMap<>();

  private HashMap<Integer, StoredQueryTreeEntry> id2storedQueries = new HashMap<Integer, StoredQueryTreeEntry>();

  private StoredQueryTreeEntry root = null;

  private ICatalogClientManager catalogClientManager;

  public QueryIOManager(ICatalogClientManager catalogClientManager)
          throws SQLException {
    this.catalogClientManager = catalogClientManager;
    init();
  }

  private void init() throws SQLException {
    root = new StoredQueryTreeEntry("Gespeicherte Anfragen", null);
    queryAdapter = DWQueryConfig.getInstance().getQueryAdapterFactory().createQueryAdapter(this);
  }

  public void dropTables() throws SQLException {
    queryAdapter.dropTable();
  }
  
  public void refresh() {
    try {
      init();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public RawQuery getQuery(String name) {
    return rawQueries.get(name.toLowerCase());
  }

  public RawQuery getQuery(int aQueryID) {
    return id2rawQueries.get(aQueryID);
  }

  public StoredQueryTreeEntry getStoredQueryEntry(String name) {
    return name2storedQuery.get(name);
  }

  public QueryTableRepresentation loadQuery(String name) throws GUIClientException {
    StoredQueryTreeEntry storedQueryEntry = getStoredQueryEntry(name);
    return loadQuery(storedQueryEntry);
  }

  public QueryTableRepresentation loadQuery(int queryID) throws GUIClientException {
    StoredQueryTreeEntry storedQueryEntry = getStoredQueryEntry(queryID);
    return loadQuery(storedQueryEntry);
  }

  private QueryTableRepresentation loadQuery(StoredQueryTreeEntry storedQueryEntry)
          throws GUIClientException {
    QueryRoot query = loadQueryRoot(storedQueryEntry);
    return new QueryTableRepresentation(query, this);
  }

  private QueryRoot loadQueryRoot(StoredQueryTreeEntry storedQueryEntry) throws GUIClientException {
    storedQueryEntry.getQuery();
    QueryRoot query = QueryReader.read(catalogClientManager, storedQueryEntry.getQuery().getXml());
    return query;
  }

  public StoredQueryTreeEntry getRoot() {
    return root;
  }

  public void add(RawQuery aQuery) {
    rawQueries.put(aQuery.getName().toLowerCase(), aQuery);
    id2rawQueries.put(aQuery.getId(), aQuery);
    StoredQueryTreeEntry storedQuery = insertIntoStoredQueryTree(aQuery);
    id2storedQueries.put(aQuery.getId(), storedQuery);
  }

  private StoredQueryTreeEntry insertIntoStoredQueryTree(RawQuery rawQuery) {
    StoredQueryTreeEntry container = getOrCreateQueryContainer(rawQuery.getName());
    StoredQueryTreeEntry treeEntry = createStroredQueryTreeEntry(rawQuery, container);
    return treeEntry;
  }

  private StoredQueryTreeEntry getOrCreateQueryContainer(String queryPath) {
    String[] containersAndQueryLeaf = queryPath.split("/");
    StoredQueryTreeEntry currentContainer = root;
    for (int i = 0; i <= containersAndQueryLeaf.length - 2; i++) {
      String childName = containersAndQueryLeaf[i];
      StoredQueryTreeEntry child = getChildWithName(currentContainer, childName);
      child = child == null ? createStroredQueryTreeEntry(childName, currentContainer) : child;
      currentContainer = child;
    }
    return currentContainer;
  }

  private static StoredQueryTreeEntry getChildWithName(StoredQueryTreeEntry parent,
          String childName) {
    for (StoredQueryTreeEntry child : parent.getChilds()) {
      if (child.isStructure() && child.getLabel().equals(childName)) {
        return child;
      }
    }
    return null;
  }

  private StoredQueryTreeEntry createStroredQueryTreeEntry(String label,
          StoredQueryTreeEntry parent) {
    StoredQueryTreeEntry entry = new StoredQueryTreeEntry(label, parent);
    storeInHashMap(entry);
    return entry;
  }

  private StoredQueryTreeEntry createStroredQueryTreeEntry(RawQuery rawQuery,
          StoredQueryTreeEntry parent) {
    StoredQueryTreeEntry entry = new StoredQueryTreeEntry(rawQuery, parent);
    storeInHashMap(entry);
    return entry;
  }

  private void storeInHashMap(StoredQueryTreeEntry entry) {
    name2storedQuery.put(entry.getPath(), entry);
  }

  public RawQuery updateEntry(String name, String xml, String user) throws QueryException {
    String fullName = name.toLowerCase();
    if (!name.toLowerCase().startsWith(user)) {
      fullName = user + "/" + name.toLowerCase();
    }
    RawQuery rawQuery = rawQueries.get(fullName);
    rawQuery.setXml(xml);
    try {
      queryAdapter.updateEntry(rawQuery);
    } catch (SQLException e) {
      throw new QueryException(e);
    }
    return rawQuery;
  }

  public RawQuery insert(String name, String xml, String folder, String user)
          throws QueryException {
    String fullName = name.toLowerCase();
    if ((folder != null) && !folder.isEmpty()) {
      fullName = folder + "/" + name.toLowerCase();
    }
    fullName = user + "/" + fullName;
    if (rawQueries.containsKey(fullName)) {
      throw new QueryException(QueryExceptionType.QUERY_ALREADY_EXISTS);
    }
    try {
      RawQuery result = queryAdapter.insert(fullName, xml);
      add(result);
      return result;
    } catch (SQLException e) {
      throw new QueryException(e);
    }
  }

  public void renameEntry(String oldNameWithFolders, String newNameWithFolders)
          throws QueryException {
    if (rawQueries.containsKey(newNameWithFolders)) {
      throw new QueryException(QueryExceptionType.QUERY_ALREADY_EXISTS);
    }
    RawQuery rawQuery = rawQueries.get(oldNameWithFolders.toLowerCase());
    rawQuery.setName(newNameWithFolders);
    rawQueries.remove(oldNameWithFolders.toLowerCase());
    rawQueries.put(newNameWithFolders.toLowerCase(), rawQuery);
    try {
      queryAdapter.updateEntry(rawQuery);
      init();
    } catch (SQLException e) {
      throw new QueryException(e);
    }
  }

  public void deleteEntry(String name) throws QueryException {
    RawQuery query = rawQueries.get(name);
    rawQueries.remove(query.getName());
    id2rawQueries.remove(query.getId());
    try {
      queryAdapter.deleteEntry(query.getId());
      init();
    } catch (SQLException e) {
      throw new QueryException(e);
    }
  }

  public List<RawQuery> getOrderedRawQueries() {
    Collection<RawQuery> values = rawQueries.values();
    ArrayList<RawQuery> list = new ArrayList<RawQuery>(values);
    Collections.sort(list, new Comparator<RawQuery>() {

      @Override
      public int compare(RawQuery arg0, RawQuery arg1) {
        return arg0.getName().compareTo(arg1.getName());
      }

    });
    return list;
  }

  public StoredQueryTreeEntry getStoredQueryEntry(int id) {
    return id2storedQueries.get(id);
  }

  public QueryQuickSearchRepresentation loadQueryInQuickSearchRepresentation(String name,
          ICatalogAndTextSuggester suggester) throws GUIClientException {
    StoredQueryTreeEntry storedQueryEntry = getStoredQueryEntry(name);
    QueryRoot queryRoot = loadQueryRoot(storedQueryEntry);
    return ModelConverter.queryRoot2QuickSearchRepresentation(queryRoot, suggester);
  }

  public QueryRoot read(String xmlQuery) throws QueryException {
    return QueryReader.read(catalogClientManager, xmlQuery);
  }

  @Override
  public void dispose() {
    if (catalogClientManager != null)
      catalogClientManager.dispose();
    if (queryAdapter != null)
      queryAdapter.dispose();
    catalogClientManager = null;
    id2rawQueries = null;
    id2storedQueries = null;
    name2storedQuery = null;
    queryAdapter = null;
    rawQueries = null;
    root = null;
  }

  @Override
  public ICatalogClientManager getCatalogClientManager() {
    return this.catalogClientManager;
  }
}
