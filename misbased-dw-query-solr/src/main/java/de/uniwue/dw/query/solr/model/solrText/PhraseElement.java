package de.uniwue.dw.query.solr.model.solrText;

public class PhraseElement implements SolrTextQueryElement {
  public static final String START_CHAR = "\"";

  private String phrase;

  public static boolean canConsumeNextToken(String text) {
    return (text.startsWith(START_CHAR));
  }

  @Override
  public String consumeNextToken(String text) {
    int closingQuotePosition = text.indexOf('"', 1);
    if (closingQuotePosition == -1)
      throw new IllegalArgumentException("Missing closing '\"' in: " + text);
    phrase = text.substring(0, closingQuotePosition + 1);
    return text.substring(closingQuotePosition + 1);
  }

  public String toString() {
    return phrase;
  }

  @Override
  public String toSolrQuery(String solrFieldName, String queryType) {
    return solrFieldName + ":" + phrase;
  }
  // public static void main(String[] args) {
  // String
  // }
}
