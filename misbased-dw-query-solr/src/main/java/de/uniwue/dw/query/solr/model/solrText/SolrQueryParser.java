package de.uniwue.dw.query.solr.model.solrText;

import de.uniwue.dw.query.model.lang.QueryAttribute;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SolrQueryParser {
  private static final Logger logger = LogManager.getLogger(SolrQueryParser.class);

  public static String parse(String query, String solrFieldName, String queryType) {
    ElementList list = new ElementList();
    list.parse(query);
    logger.trace("passed list: " + list);
    String solrQuery = list.toSolrQuery(solrFieldName, queryType);
    logger.debug("solrQuery: " + solrQuery);
    return solrQuery;
  }

  public static String parse(ParseTree parseTree, QueryAttribute queryElem) {
    String query = new SolrTextContentVisitor(queryElem).visit(parseTree);
    if (queryElem.getDesiredContent().startsWith("{") && queryElem.getDesiredContent().endsWith("}")) {
      query = query.replace("\"", "\\\"");
      query = "(_query_:\"{!complexphrase}" + query + "\")";
    }
    return query;
  }

}
