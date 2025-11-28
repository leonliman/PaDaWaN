package de.uniwue.dw.query.solr.model.solrText;

public class TextElement implements SolrTextQueryElement {

  private static final String TEXT_SPLIT_REGEX = "[ \\(\\)\\[\\]]";

  private String text;

  public static boolean canConsumeNextToken(String text) {
    return !(text.startsWith("(") || text.startsWith("[")) && !text.isEmpty();
  }

  @Override
  public String consumeNextToken(String query) {
    String[] split = query.split(TEXT_SPLIT_REGEX);
    // if (split.length > 1) {
    String tail = query.substring(split[0].length(), query.length());
    this.text = split[0];
    return tail;
    // } else
    // throw new IllegalArgumentException("Query must contain a token in: "
    // + query);
  }

  public static void main(String[] args) {
    String s = "aaa bbb";
    String tail = new TextElement().consumeNextToken(s);
    System.out.println("[" + tail + "]");
  }

  public String toString() {
    return text;
  }

  @Override
  public String toSolrQuery(String solrFieldName, String queryType) {
    return solrFieldName + ":" + text;
  }
}
