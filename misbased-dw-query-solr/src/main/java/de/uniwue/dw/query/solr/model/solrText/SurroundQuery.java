package de.uniwue.dw.query.solr.model.solrText;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.uniwue.misc.util.StringUtilsUniWue;

public class SurroundQuery implements SolrTextQueryElement {

  public static final String START_CHAR = "[";

  public static final String REGEX = "(\\d*)\\s*(=?)(.*)";

  public static final int DEFAULT_SPAN = 7;

  private static final int MAX_BASIC_QUERIES = 1000000;

  private static final String WILDCARD = "*";

  private int span = DEFAULT_SPAN;

  private boolean orderd = false;

  private String reference;

  private List<String> tokens = new ArrayList<>();

  private List<List<String>> combinations = new ArrayList<List<String>>();

  public static boolean canConsumeNextToken(String text) {
    return (text.startsWith(START_CHAR));
  }

  @Override
  public String consumeNextToken(String query) {
    int end = query.indexOf("]");
    if (end < 0)
      throw new IllegalArgumentException(
              "SurroundQuery must contain an ending ']' in query: " + query);
    String head = query.substring(0, end + 1);
    String tail = query.substring(end + 1, query.length());
    parse(head);
    checkIfTokensStartWithWildcard();
    return tail;
  }

  private void checkIfTokensStartWithWildcard() {
    String exceptionText = "Bei einer Umkreissuche darf kein Wort mit einem * beginnen. Fehlerhaftes Wort: ";
    for (String token : tokens)
      if (token.startsWith(WILDCARD))
        throw new IllegalArgumentException(exceptionText + token);
    for (List<String> list : combinations)
      for (String token : list)
        if (token.startsWith(WILDCARD))
          throw new IllegalArgumentException(exceptionText + token);
  }

  private void parse(String sourroundQuery) {
    String query = sourroundQuery.substring(1, sourroundQuery.length() - 1).trim();
    Pattern pattern = Pattern.compile(REGEX);
    Matcher matcher = pattern.matcher(query);
    if (matcher.find()) {
      String parsedSpan = matcher.group(1);
      String parsedOrder = matcher.group(2);
      String parsedTokens = matcher.group(3).trim();
      span = parsedSpan.isEmpty() ? span : Integer.valueOf(parsedSpan);
      orderd = parsedOrder.isEmpty() ? orderd : true;
      combinations = findCombinations(parsedTokens);
      // System.out.println("parsedTokens: "+parsedTokens);
      String[] split = parsedTokens.split("\\s+");
      for (int i = 0; i <= split.length - 1; i++) {
        String term = split[i];
        boolean isReference = i == 0 ? true : false;
        if (term.startsWith("+")) {
          isReference = true;
          term = term.substring(1);
        }
        tokens.add(term);
        if (isReference)
          reference = term;
      }
    }
  }

  private interface SurroundQueryElements {

  }

  private class AndList extends LinkedList<SurroundQueryElements> implements SurroundQueryElements {
    public String toString() {
      StringBuilder sb = new StringBuilder("AND(");
      for (SurroundQueryElements e : this)
        sb.append(e + " ");
      sb.append(")");
      return sb.toString();
    }
  }

  private class OrList extends ArrayList<SurroundQueryElements> implements SurroundQueryElements {
    public String toString() {
      StringBuilder sb = new StringBuilder("OR(");
      for (SurroundQueryElements e : this)
        sb.append(e + " ");
      sb.append(")");
      return sb.toString();
    }
  }

  private class StringElemnt implements SurroundQueryElements {
    public String string;

    public StringElemnt(String string) {
      this.string = string;
    }

    public String toString() {
      return string;
    }
  }

  private List<List<String>> findCombinations(String parsedTokens) {
    parsedTokens = "(" + parsedTokens + ")";
    ElementList list = new ElementList();
    list.consumeNextToken(parsedTokens);
    SurroundQueryElements structure = transformRepresentation(list);
    // System.out.println(structure);
    List<List<String>> combinations = generateCombinations(structure);
    // for (List<String> s : combinations)
    // System.out.println(StringUtilsUniWue.concat(s, ", "));
    return combinations;
  }

  private SurroundQueryElements transformRepresentation(SolrTextQueryElement ele) {
    if (ele instanceof ElementList) {
      OrList orList = new OrList();
      ElementList list = (ElementList) ele;
      AndList curListElems = new AndList();
      boolean isOrList = false;
      for (SolrTextQueryElement e : list.getElements()) {
        if (e instanceof OrElement) {
          isOrList = true;
          orList.add(curListElems);
          curListElems = new AndList();
        } else {
          curListElems.add(transformRepresentation(e));
        }
      }
      if (isOrList) {
        orList.add(curListElems);
        return orList;
      } else {
        return curListElems;
      }
    } else if (ele instanceof TextElement) {
      return new StringElemnt(ele.toString());
    } else
      throw new IllegalArgumentException("Unsupported Element in SurroundQuery");
  }

  private static List<List<String>> generateCombinations(SurroundQueryElements structure) {
    List<List<String>> result = new ArrayList<List<String>>();
    if (structure instanceof OrList) {
      for (SurroundQueryElements child : (OrList) structure) {
        List<List<String>> childCominations = generateCombinations(child);
        result.addAll(childCominations);
      }
    } else if (structure instanceof AndList) {
      AndList andList = (AndList) structure;
      if (andList.isEmpty())
        return result;
      SurroundQueryElements head = andList.poll();
      List<List<String>> headCombos = generateCombinations(head);
      List<List<String>> tailCombos = generateCombinations(andList);
      for (List<String> headString : headCombos) {
        if (tailCombos.isEmpty()) {
          result.add(headString);
        } else
          for (List<String> tailString : tailCombos) {
            List<String> tokens = new ArrayList<String>();
            tokens.addAll(headString);
            tokens.addAll(tailString);
            result.add(tokens);
          }
      }
    } else if (structure instanceof StringElemnt) {
      List<String> tokens = new ArrayList<String>();
      tokens.add(((StringElemnt) structure).string);
      result.add(tokens);
    }
    return result;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    String order = orderd ? "ordered" : "unorderd";
    sb.append(order + ", ");
    sb.append("span: " + span + ", ");
    sb.append("reference: " + reference + ", ");
    sb.append("tokens: ");
    for (String s : tokens)
      sb.append(s + " ");
    sb.append("]");
    return sb.toString();
  }

  public static void main(String[] args) {
    String string = "[ *nase mund]";
    // String string = "[ a OR (b OR c) d]";
    SurroundQuery surroundQuery = new SurroundQuery();
    surroundQuery.consumeNextToken(string);
    System.out.println(surroundQuery);
    System.out.println(surroundQuery.toSolrQuery("fieldname", ""));
  }

  @Override
  public String toSolrQuery(String solrFieldName, String querType) {
    List<String> solrQueries = new ArrayList<String>();
    for (List<String> combo : combinations) {
      StringBuilder sb = new StringBuilder();
      sb.append("(");
      sb.append("(_query_:\"{!surround maxBasicQueries=" + MAX_BASIC_QUERIES + "}");
      sb.append(solrFieldName);
      sb.append(":");
      sb.append(span);
      String order = orderd ? "w" : "n";
      sb.append(order);
      sb.append("(");
      boolean first = true;
      for (String token : combo) {
        if (first)
          first = false;
        else
          sb.append(", ");
        sb.append(token.toLowerCase());
      }
      sb.append(")\")");
      sb.append(")");
      solrQueries.add(sb.toString());
    }
    String concat = StringUtilsUniWue.concat(solrQueries, " OR ");
    concat = "(" + concat + ")";
    return concat;
  }
}
