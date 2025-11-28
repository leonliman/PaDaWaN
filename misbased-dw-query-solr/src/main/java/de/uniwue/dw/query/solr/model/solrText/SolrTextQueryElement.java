package de.uniwue.dw.query.solr.model.solrText;

public interface SolrTextQueryElement {

  public String consumeNextToken(String text);

  public String toSolrQuery(String solrFieldName, String queryType);

}
