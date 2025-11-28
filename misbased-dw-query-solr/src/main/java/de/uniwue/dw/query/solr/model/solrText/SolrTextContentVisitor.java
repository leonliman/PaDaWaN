package de.uniwue.dw.query.solr.model.solrText;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentBaseVisitor;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser.DistanceContext;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser.SequenceContext;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser.WordContext;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser.WordsContext;
import de.uniwue.dw.query.model.result.RegexQueryBuilder;
import de.uniwue.dw.query.model.result.TokenCollectorVisitor;
import de.uniwue.dw.query.solr.SolrUtil;

public class SolrTextContentVisitor extends TextContentBaseVisitor<String> {

  private Logger logger = LogManager.getLogger(SolrTextContentVisitor.class);

  private static final int MAX_BASIC_QUERIES = 1000000;

  private String solrTextField;

  private String solrStringField;

  // public SolrTextContentVisitor(String solrFieldName) {
  // this.solrFieldName = solrFieldName;
  // }
  //
  public SolrTextContentVisitor(QueryAttribute queryAttribute) {
    solrTextField = SolrUtil.getSolrFieldName(queryAttribute);
    solrStringField = SolrUtil.getSolrFieldNameForString(queryAttribute.getCatalogEntry());
  }

  @Override
  protected String aggregateResult(String aggregate, String nextResult) {
    if (aggregate == null && nextResult == null)
      return null;
    if (aggregate != null && nextResult == null)
      return aggregate;
    else if (aggregate == null && nextResult != null)
      return nextResult;
    else {
      return aggregate + " " + nextResult;
    }
  }

  private static ParseTree getFirstTypeOfChilds(ParseTree ctx,
          Class<? extends ParseTree> targetClass) {
    for (int i = 0; i <= ctx.getChildCount() - 1; i++) {
      ParseTree child = ctx.getChild(i);
      if (targetClass.isInstance(child)) {
        return child;
      }
    }
    return null;
  }

  @Override
  public String visitBooleanOr(TextContentParser.BooleanOrContext ctx) {
    return joinChilds(ctx, " OR ");
  }

  private String joinChilds(ParserRuleContext ctx, String delimiter) {
    List<String> queryParts = new ArrayList<>();
    for (int i = 0; i <= ctx.getChildCount() - 1; i++) {
      ParseTree child = ctx.getChild(i);
      String queryPart = child.accept(this);
      if (queryPart != null && !queryPart.isEmpty())
        queryParts.add(queryPart);
    }
    String queryString = queryParts.stream().collect(Collectors.joining(delimiter));
    if (!queryString.isEmpty())
      queryString = "(" + queryString + ")";
    return queryString;
  }

  @Override
  public String visitBooleanAnd(TextContentParser.BooleanAndContext ctx) {
    return joinChilds(ctx, " AND ");
  }

  @Override
  public String visitWords(TextContentParser.WordsContext ctx) {
    return joinChilds(ctx, " AND ");
  }

  @Override
  public String visitWord(TextContentParser.WordContext ctx) {
    return solrTextField + ":" + ctx.getText();
  }

  private static List<ParseTree> getAllTypeOfChilds(ParseTree ctx,
          Class<? extends ParseTree> targetClass) {
    List<ParseTree> result = new ArrayList<>();
    for (int i = 0; i <= ctx.getChildCount() - 1; i++) {
      ParseTree child = ctx.getChild(i);
      if (targetClass.isInstance(child)) {
        result.add(child);
      }
    }
    return result;
  }

  @Override
  public String visitNear(TextContentParser.NearContext ctx) {
    ParseTree distanceParseTree = getFirstTypeOfChilds(ctx, DistanceContext.class);
    int distance = TokenCollectorVisitor.DEFAULT_SPAN_DISTANCE;
    if (distanceParseTree != null) {
      distance = Integer.parseInt(distanceParseTree.getText());
    }
    ParseTree sequence = getFirstTypeOfChilds(ctx, SequenceContext.class);
    boolean isOrderd = sequence == null ? false : true;
    ParseTree wordsParseTree = getFirstTypeOfChilds(ctx, WordsContext.class);
    List<String> words = getAllTypeOfChilds(wordsParseTree, WordContext.class).stream()
            .map(ParseTree::getText).collect(Collectors.toList());
    return createSpanQuery(solrTextField, words, distance, isOrderd);
  }

  public static String createSpanQuery(String solrFieldName, List<String> tokens, int distance,
          boolean orderd) {
    StringBuilder query = new StringBuilder();
    query.append("(");
    query.append("(_query_:\"{!surround maxBasicQueries=" + MAX_BASIC_QUERIES + "}");
    query.append(solrFieldName);
    query.append(":");
    query.append(distance);
    String order = orderd ? "w" : "n";
    query.append(order);
    query.append("(");
    boolean first = true;
    for (String token : tokens) {
      if (first)
        first = false;
      else
        query.append(", ");
      query.append(token.toLowerCase());
    }
    query.append(")\")");
    query.append(")");
    return query.toString();
  }

  // @Override
  // public String visitRegex(TextContentParser.RegexContext ctx) {
  // String regex = RegexQueryBuilder.buildRegex(ctx);
  // // String regex = ctx.getText();
  // // regex = replacePredifienedEntities(regex);
  // String any = ".*?";
  // int firstDash = regex.indexOf('/');
  // regex = insertAfter(regex, firstDash, any);
  //
  // int lastDash = regex.lastIndexOf('/');
  // regex = insertAfter(regex, lastDash - 1, any);
  //
  // logger.trace("regex: " + regex);
  // return solrStringField + ":" + regex;
  // }

  @Override
  public String visitRegexQuery(TextContentParser.RegexQueryContext ctx) {
    String regex = RegexQueryBuilder.buildRegex(ctx);
    logger.debug("regex: " + regex);
    // String regex = ctx.getText();
    // regex = replacePredifienedEntities(regex);
    String any = ".*?";
    // int firstDash = regex.indexOf('/');
    // regex = insertAfter(regex, firstDash, any);
    // int lastDash = regex.lastIndexOf('/');
    // regex = insertAfter(regex, lastDash - 1, any);

    regex = "/" + any + regex + any + "/";
    logger.trace("regex: " + regex);
    return solrStringField + ":" + regex;
  }

  private String replacePredifienedEntities(String regex) {
    String number = "[0-9]+";
    String zahl = "ZAHL";
    regex = regex.replace(zahl, number);
    return regex;
  }

  public static void main(String[] args) {
    String regex = "/abc/";
    String any = ".*?";

    int firstDash = regex.indexOf('/');
    regex = insertAfter(regex, firstDash, any);

    int lastDash = regex.lastIndexOf('/');
    regex = insertAfter(regex, lastDash - 1, any);

    System.out.println(regex);
  }

  private static String insertAfter(String base, int position, String insertion) {
    position++;
    String result = base.substring(0, position) + insertion
            + base.substring(position, base.length());
    return result;
  }

  @Override
  public String visitPhrase(TextContentParser.PhraseContext ctx) {
    return solrTextField + ":" + ctx.getText();
  }
}
