package de.uniwue.dw.query.model.manager;

import java.util.List;
import java.util.Optional;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.manager.ICatalogAndTextSuggester;
import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.data.LoggedQuery;
import de.uniwue.dw.query.model.data.RawQuery;
import de.uniwue.dw.query.model.data.StoredQueryTreeEntry;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.quickSearch.QueryQuickSearchRepresentation;
import de.uniwue.dw.query.model.table.QueryTableRepresentation;

public interface IQueryClientIOManager {

  public IQueryIOManager getQueryIOManager();

  /**
   * Retrieves an ordered list of all queries that are visible for a given user.
   * 
   * @param user
   * @return
   */
  List<RawQuery> getOrderedRawQueries(User user);

  /**
   * 
   * Retrieve the root entry of stored queries.
   * 
   * @return
   */
  StoredQueryTreeEntry getStoredQueriesRootEntry();

  /**
   * Retrieves all stored queries that are visible for a given user and returns it in a tree
   * representation.
   * 
   * @param user
   * @return
   */
  StoredQueryTreeEntry getStoredQueryTreeForUser(User user);

  /**
   * Retrieves all children of a stored query that are visible for a given user.
   * 
   * @param parent
   * @param user
   * @return
   */
  List<StoredQueryTreeEntry> getChildren(StoredQueryTreeEntry parent, User user);

  /**
   * Loads a stored query by the given name and returns it in the table representation.
   * 
   * @param name
   * @param guiClient
   * @return
   * @throws GUIClientException
   */
  QueryTableRepresentation loadQuery(String name) throws GUIClientException;

  /**
   * Loads a stored query by the given id and returns it in the table representation.
   * 
   * @param queryID
   * @param guiClient
   * @return
   * @throws GUIClientException
   */
  QueryTableRepresentation loadQuery(int queryID) throws GUIClientException;

  /**
   * Loads a stored query by the given name and returns it in the quick-search representation.
   * 
   * @param name
   * @param guiClient
   * @return
   * @throws GUIClientException
   */
  QueryQuickSearchRepresentation loadQueryInQuickSearchRepresentation(String name,
          ICatalogAndTextSuggester suggester) throws GUIClientException;

  void logQuery(String xml, String user, int queryID, String exportType, String engineName,
          String engineVersion) throws GUIClientException;

  void updateLogQuery(String user, int queryID, long resultCount, long duration)
          throws GUIClientException;

  /**
   * Returns the last logged queries of the user.
   * 
   * @param user
   * @param numberOfQueries
   * @return
   * @throws GUIClientException
   */
  List<LoggedQuery> getLoggedQueries(User user, int numberOfQueries) throws GUIClientException;

  /**
   * Loads a logged query by the given logID and returns it in QueryRoot representation. It will be
   * checked if the user has the permission. He must be owner of the query or must be admin.
   * 
   * @param logID
   * @param user
   * @return
   * @throws GUIClientException
   */
  QueryRoot loadLoggedQuery(int logID, User user) throws GUIClientException;

  /**
   * Loads a logged query by the given logID and returns it in quick search representation. It will
   * be checked if the user has the permission. He must be owner of the query or must be admin.
   * 
   * @param logID
   * @param user
   * @return
   * @throws GUIClientException
   */
  QueryQuickSearchRepresentation loadLoggedQueryInQuickSearchRepresentation(int logID, User user,
          ICatalogAndTextSuggester suggester) throws GUIClientException;

  /*
   * Save a query with the given name
   */
  RawQuery saveQuery(String name, String xml, String folder, String user) throws GUIClientException;

  /*
   * Update the xml of the given query
   */
  RawQuery updateQuery(String name, String xml, String user) throws GUIClientException;

  /*
   * Create a parsed query object from the given MXML string
   */
  QueryRoot read(String xmlQuery) throws GUIClientException;

  /*
   * Get the raw query with the given name
   */
  RawQuery getQuery(String name);

  /*
   * Get the raw query with the given name. The name is the absolute path of the query. e.g.:
   * user1/folder2/query3
   */
  StoredQueryTreeEntry getStoredQueryTreeEntry(String name);

  /*
   * Get the raw query with the given id
   */
  StoredQueryTreeEntry getStoredQueryTreeEntry(int id);

  /*
   * delete the query with the given name
   */
  void deleteQuery(String name) throws GUIClientException;

  /*
   * Rename the the query with the given old name with the new name
   */
  void renameQuery(String oldNameWithFolders, String newNameWithFolders) throws GUIClientException;

  /*
   * Get all queries that exist so far
   */
  List<RawQuery> getOrderedRawQueries();

  /*
   * Dispose the respective query engine and free all resources that have been used by it
   */
  public void dispose();

  public Optional<StoredQueryTreeEntry> getStoredQueryForUser(String storedQueyPath, User user);

}
