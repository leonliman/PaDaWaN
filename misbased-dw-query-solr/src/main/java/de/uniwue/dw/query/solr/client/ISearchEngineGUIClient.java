package de.uniwue.dw.query.solr.client;

import de.uniwue.dw.query.model.quickSearch.suggest.IInputSuggester;

public interface ISearchEngineGUIClient {

  public IInputSuggester getSuggester();

}
