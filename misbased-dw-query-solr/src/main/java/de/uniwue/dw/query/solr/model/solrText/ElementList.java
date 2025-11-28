package de.uniwue.dw.query.solr.model.solrText;

import java.util.ArrayList;
import java.util.List;

public class ElementList implements SolrTextQueryElement {
  public static final String START_CHAR = "(";

  private List<SolrTextQueryElement> elems = new ArrayList<>();

  public static boolean canConsumeNextToken(String text) {
    return (text.startsWith(START_CHAR));
  }

  @Override
  public String consumeNextToken(String text) {
    int braces = 0;
    int i = 0;
    do {
      char curChar = text.charAt(i);
      if (curChar == '(')
        braces++;
      else if (curChar == ')')
        braces--;
      i++;
    } while (braces != 0 && i < text.length());
    if (braces == 0) {
      String elementList = text.substring(0, i);
      elementList = elementList.substring(1, elementList.length() - 1).trim();
      parse(elementList);
      String tail = text.substring(i, text.length());
      return tail;
    } else
      throw new IllegalArgumentException("Missing closing ')' in: " + text);
  }

  private static SolrTextQueryElement findNextElement(String query) {
    if (SurroundQuery.canConsumeNextToken(query))
      return new SurroundQuery();
    if (PhraseElement.canConsumeNextToken(query))
      return new PhraseElement();
    if (ElementList.canConsumeNextToken(query))
      return new ElementList();
    if (OrElement.canConsumeNextToken(query))
      return new OrElement();
    else
      return new TextElement();
  }

  public void parse(String elementList) {
    String query = elementList;
    while (!query.isEmpty()) {
      SolrTextQueryElement nextElement = findNextElement(query);
      String tail = nextElement.consumeNextToken(query);
      query = tail.trim();
      elems.add(nextElement);
    }
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("(");
    boolean first = true;
    for (SolrTextQueryElement elem : elems) {
      if (first)
        first = false;
      else
        sb.append(" ");
      sb.append(elem.toString());
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public String toSolrQuery(String solrFieldName, String queryType) {
    StringBuilder sb = new StringBuilder();
    sb.append("(");
    for (int i = 0; i <= elems.size() - 1; i++) {
      SolrTextQueryElement curElem = elems.get(i);
      if (!(i == 0 || curElem instanceof OrElement || elems.get(i - 1) instanceof OrElement))
        sb.append("AND ");
      sb.append(curElem.toSolrQuery(solrFieldName, queryType) + " ");
    }
    sb.append(")");
    return sb.toString();
  }

  public List<SolrTextQueryElement> getElements() {
    return elems;
  }
}
