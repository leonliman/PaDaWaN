package de.uniwue.dw.core.model.manager;

import java.util.List;

import de.uniwue.dw.core.client.authentication.CountType;
import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.data.CatalogEntry;

public interface ICatalogClientManager {

  // public CatalogManager getCatalogManager();

  /*
   * Retrieve the root catalogEntry of the catalog
   */
  CatalogEntry getRoot();

  /**
   * Retrieves all children of a catalog entry, which are visible for a given user.
   * 
   * @param parent
   * @param user
   * @return
   */
  List<CatalogEntry> getChildsOf(CatalogEntry parent, User user);

  /**
   * Retrieves all children of a catalog entry, which are visible for a given user. The result is
   * also filtered according to the given minimum occurrence of an entry.
   * 
   * @param parent
   * @param user
   * @param countType
   * @param minOccurrence
   * @return
   */
  List<CatalogEntry> getChildsOf(CatalogEntry parent, User user, CountType countType,
          int minOccurrence);

  // /**
  // * Retrieves all catalog entries, which are visible for a given user.
  // *
  // * @param user
  // * @return
  // */
  // List<CatalogEntry> getEntries(User user);

  // /**
  // * Retrieves all catalog entries, which are visible for a given user. The result is also
  // filtered
  // * according to the given minimum occurrence of an entry.
  // *
  // * @param user
  // * @param countType
  // * @param minOccurrence
  // * @return
  // */
  // List<CatalogEntry> getEntries(User user, CountType countType, int minOccurrence);

  /**
   * Retrieve all catalogEntries that match the current filter word and are visible for the given
   * user.
   * 
   * @param word
   * @param user
   * @return
   */
  List<CatalogEntry> getEntriesByWordFilter(String word, User user, int limit);

  /**
   * Retrieve all catalogEntries that match the current filter word and are visible for the given
   * user. The result is also filtered according to the given minimum occurrence of an entry.
   * 
   * @param word
   * @param user
   * @param countType
   * @param minOccurrence
   * @return
   */
  List<CatalogEntry> getEntriesByWordFilter(String word, User user, CountType countType,
          int minOccurrence, int limit);

  /**
   * Retrieves a catalog tree that contains all nodes that match the given search filter and that
   * are visible for the user.
   * 
   * @param searchPhrase
   *          search filter. can contain a single word or whitespace separated words.
   * @param user
   * @return
   */
  CatalogEntry getTreeByWordFilter(String searchPhrase, User user);

  /**
   * Retrieves a catalog tree that contains all nodes that match the given search filter and that
   * are visible for the user. The result is also filtered according to the given minimum occurrence
   * of an entry.
   * 
   * @param searchPhrase
   * @param user
   * @param countType
   * @param minOccurrence
   * @return
   */
  CatalogEntry getTreeByWordFilter(String searchPhrase, User user, CountType countType,
          int minOccurrence);

  /**
   * 
   * @param entry
   * @param user
   * @param countType
   * @param minOccurrence
   * @return
   */
  CatalogEntry getAllAncestorsAndSiblings(CatalogEntry entry, User user, CountType countType,
          int minOccurrence);

  public CatalogEntry getEntryByID(int attrId, User user) throws DataSourceException;

  /*
   * Retrieve a single catalogEntry
   */
  CatalogEntry getEntryByRefID(String extID, String project, User user) throws DataSourceException;

  CatalogEntry getEntryByRefID(String extID, String project, User user,
          boolean throwExceptionIfNotExists) throws DataSourceException;

  /*
   * Dispose the respective query engine and free all resources that have been used by it
   */
  public void dispose();
  
  public void reinitialize() throws DataSourceException;

  public boolean isDisposed();

}
