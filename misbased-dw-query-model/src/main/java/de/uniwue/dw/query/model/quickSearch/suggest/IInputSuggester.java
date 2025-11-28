package de.uniwue.dw.query.model.quickSearch.suggest;

import java.util.List;

import de.uniwue.dw.core.client.authentication.User;

public interface IInputSuggester {

  /**
   * suggest content form from data fields like tokens from texts
   */
  public List<SuggestObject> suggestTextToken(String token);
  
  /**
   * suggest content form from data fields like tokens from texts
   */
  public List<SuggestObject> suggestTextToken(String token, int numberOfSuggestions);

  /**
   * suggest catalog entries matching the query token for a given user
   */
  public List<SuggestObject> suggestCatalogEntries(String token, User user,
          int numberOfCatalogSuggestions);

  /**
   * suggest catalog entries and text tokens for matching a given token for the a given user
   */
  public List<SuggestObject> suggestTextTokensAndCatalogEntries(String token, User user,
          int numberOfSuggestions);

  /**
   * suggests conjunctions
   */
  public List<SuggestObject> suggestConjunctions(int position);

  /**
   * returns suggestions for an empty input
   */
  public List<SuggestObject> suggestQueryEntriesForEmtpyInput(User user, int inputPosition);

  /**
   * suggests complete queries for a given input token und a user. this methods summarizes all other
   * methods in this interface
   */
  public List<SuggestObject> suggestQueryEntries(String token, User user);
}
