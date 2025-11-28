package de.uniwue.dw.query.model.quickSearch.suggest;

import java.util.List;

import de.uniwue.dw.core.model.data.CatalogEntry;

public interface IContentSuggester {
  
  /**
   * suggests tokens of all values of all catalog entry fields
   * @param token
   * @return
   */
  public List<String> suggestTextTokens(String token);

  /**
   * sugggests tokens of a all values of a given catalog entry field
   * @param token
   * @param catalogEntry
   * @return
   */
  public List<String> suggestTextTokensOfEntry(String token, CatalogEntry catalogEntry);

}
