package de.uniwue.dw.core.model.manager;

import java.util.List;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.misc.util.ConfigException;

public interface ICatalogAndTextSuggester {

  public List<String> suggestTextTokens(String token, int numberOfSuggestions);

  public List<String> suggestTextTokens(String token, CatalogEntry catalogEntry, int numberOfSuggestions);

  public List<CatalogEntry> suggestCatalogEntries(String token, User user, int numberOfSuggestions);

  public List<CatalogEntry> suggestCatalogEntries(String token, User user,
          CatalogEntryType dataType, int numberOfCatalogSuggestions);

  public CatalogEntry getCatalogEntryByName(String name, String domain, User user)
          throws DataSourceException;

  public CatalogEntry getCatalogEntryByName(String name, User user) throws DataSourceException;

  public CatalogEntry getCatalogEntryByUniqueName(String uniqueName, User user)
          throws DataSourceException;

  public CatalogEntry getCatalogEntryByNameOrUniqueName(String name, User user)
          throws DataSourceException;

  public List<CatalogEntry> getMostCommonTextFields(User user) throws ConfigException;

  // Case-ID
  public CatalogEntry getDocumentIDCatalogEntry() throws ConfigException;

  // Patient-ID
  public CatalogEntry getDocumentGroupIDCatalogEntry() throws ConfigException;

  public CatalogEntry getDocumentTimeCatalogEntry() throws ConfigException;

  List<CatalogEntry> getCatalogEntrySons(CatalogEntry entry, User user,
          int numberOfCatalogSuggestions);

  CatalogEntry getCatalogEntryByExtid(String extId, String project, User user)
          throws DataSourceException;

}
