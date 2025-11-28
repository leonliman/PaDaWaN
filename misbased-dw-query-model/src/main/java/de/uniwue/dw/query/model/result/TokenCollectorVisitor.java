package de.uniwue.dw.query.model.result;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.tree.ParseTree;

import de.uniwue.dw.query.model.quickSearch.parser.TextContentBaseVisitor;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser.DistanceContext;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser.OutputdefinitionContext;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser.ReferenceContext;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser.SequenceContext;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser.WordContext;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser.WordsContext;
import de.uniwue.dw.query.model.result.NearQueryHighlighter.NearQuery;

public class TokenCollectorVisitor extends TextContentBaseVisitor<String> {

  public static final int DEFAULT_SPAN_DISTANCE = 7;

  public List<String> tokens = new ArrayList<String>();

  private List<String[]> regexContents = new ArrayList<String[]>();

  private List<NearQuery> nearQueries = new ArrayList<>();

  private boolean createNamedCapturingGrops = false;

  public TokenCollectorVisitor() {
  }

  public TokenCollectorVisitor(boolean createNamedCapturingGrops) {
    this.createNamedCapturingGrops = createNamedCapturingGrops;
  }

  @Override
  public String visitQueryPart(TextContentParser.QueryPartContext ctx) {
    return visitChildren(ctx);
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

  @Override
  public String visit(ParseTree tree) {
    tokens.clear();
    return tree.accept(this);
  }

  @Override
  public String visitWord(TextContentParser.WordContext ctx) {
    tokens.add(ctx.getText());
    return null;
  }

  // @Override
  // public String visitRegexQuery(TextContentParser.RegexQueryContext ctx) {
  // String regex = RegexQueryBuilder.buildRegex(ctx);
  // logger.debug("regex: " + regex);
  // // String regex = ctx.getText();
  // // regex = replacePredifienedEntities(regex);
  // String any = ".*?";
  // // int firstDash = regex.indexOf('/');
  // // regex = insertAfter(regex, firstDash, any);
  // // int lastDash = regex.lastIndexOf('/');
  // // regex = insertAfter(regex, lastDash - 1, any);
  //
  // regex = "/" + any + regex + any + "/";
  // logger.trace("regex: " + regex);
  // return solrStringField + ":" + regex;
  // }

  @Override
  public String visitRegexQuery(TextContentParser.RegexQueryContext ctx) {
    // ParseTree regexText = getFirstTypeOfChilds(ctx, RegexContext.class);
    // ParseTree groupText = getFirstTypeOfChilds(ctx, GroupContext.class);
    // ParseTree reference = getFirstTypeOfChilds(ctx, ReferenceContext.class);
    String regex = RegexQueryBuilder.buildRegex(ctx, createNamedCapturingGrops);
    // String regex = extractRegex(regexText);
    // String group = groupText == null ? null : groupText.getText();
    ParseTree outputdefinitionPT = getFirstTypeOfChilds(ctx, OutputdefinitionContext.class);
    String outputdefinition = null;
    if (outputdefinitionPT != null)
      outputdefinition = outputdefinitionPT.getText();
    if (regex != null) {
      String[] pair = { regex, outputdefinition };
      regexContents.add(pair);
    }
    return null;
  }

  private static String extractRegex(ParseTree regexText) {
    if (regexText != null) {
      String text = regexText.getText();
      if (text != null && !text.isEmpty()) {
        if (text.startsWith("/"))
          text = text.substring(1);
        if (text.endsWith("/"))
          text = text.substring(0, text.length() - 1);
        return text;
      }
    }
    return null;
  }

  private ParseTree getFirstTypeOfChilds(ParseTree ctx, Class<? extends ParseTree> targetClass) {
    for (int i = 0; i <= ctx.getChildCount() - 1; i++) {
      ParseTree child = ctx.getChild(i);
      if (targetClass.isInstance(child)) {
        return child;
      }
    }
    return null;
  }

  public List<String[]> getRegexContents() {
    return regexContents;
  }

  @Override
  public String visitNear(TextContentParser.NearContext ctx) {
    ParseTree distanceParseTree = getFirstTypeOfChilds(ctx, DistanceContext.class);
    int distance = DEFAULT_SPAN_DISTANCE;
    if (distanceParseTree != null) {
      distance = Integer.parseInt(distanceParseTree.getText());
    }
    ParseTree sequence = getFirstTypeOfChilds(ctx, SequenceContext.class);
    boolean isOrderd = false;
    if (sequence != null)
      isOrderd = true;
    ParseTree wordsParseTree = getFirstTypeOfChilds(ctx, WordsContext.class);
    List<String> words = getAllTypeOfChilds(wordsParseTree, WordContext.class).stream()
            .map(ParseTree::getText).collect(Collectors.toList());
    nearQueries.add(new NearQuery(words, distance, isOrderd));
    return null;
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

  public List<NearQuery> getNearQueries() {
    return nearQueries;
  }

  @Override
  public String visitPhrase(TextContentParser.PhraseContext ctx) {
    String text = ctx.getText();
    if (text != null) {
      String regex = "\\\".*?\\\"";
      if (text.matches(regex))
        text = text.substring(1, text.length() - 1);
      tokens.add(text);
    }
    return visitChildren(ctx);
  }

  @Override
  public String visitGroup(TextContentParser.GroupContext ctx) {
    return ctx.getText();
  }

  public String visitNamedEntity(TextContentParser.NamedEntityContext ctx) {
    return "1";
  }

  @Override
  public String visitReference(TextContentParser.ReferenceContext ctx) {
    return visitChildren(ctx);
  }
}
