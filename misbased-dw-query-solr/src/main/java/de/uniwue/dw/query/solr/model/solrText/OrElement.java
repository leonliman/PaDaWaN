package de.uniwue.dw.query.solr.model.solrText;

public class OrElement implements SolrTextQueryElement {

  public static final String OR = "OR";

  private static final String REGEX = "or[\\s\\(].*";

  public static boolean canConsumeNextToken(String query) {
    return query.toLowerCase().matches(REGEX);
  }

  @Override
  public String consumeNextToken(String query) {
    if (canConsumeNextToken(query)) {
      return query.substring(2);
    } else
      throw new IllegalArgumentException("OR-Query must contain '" + OR + "' in: " + query);

  }

  public String toString() {
    return OR;
  }

  @Override
  public String toSolrQuery(String solrFieldName, String queryType) {
    return OR;
  }

  public static void main(String[] args) {
    String s = "or(asdf";
    System.out.println(s.matches(REGEX));
  }
}
