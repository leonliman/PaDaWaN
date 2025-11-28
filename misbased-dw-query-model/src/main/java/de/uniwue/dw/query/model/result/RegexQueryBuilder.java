package de.uniwue.dw.query.model.result;

import static de.uniwue.dw.query.model.result.Highlighter.ZAHL;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser.BoundContext;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser.NamedEntityContext;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser.NumericConditionContext;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser.NumericOperatorContext;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser.ReferenceContext;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser.RegexConditionContext;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser.RegexContext;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser.RegexQueryContext;

public class RegexQueryBuilder {

  // public static final String ZAHL = "ZAHL";

  private static final int MAX_DIGITS = 10;

  private static Logger logger = LogManager.getLogger(RegexQueryBuilder.class);

  public static void main(String[] args) {
    // String input = "/abcn ([0-9]) bac /[$1>50] ";
    // logger.debug(input);
    // TextContentLexer lexer = new TextContentLexer(new ANTLRInputStream(input));
    // TextContentParser parser = new TextContentParser(new CommonTokenStream(lexer));
    // ParseTree parseContext = parser.expression();
    // print(parseContext, parser);
    //
    // ParseTree queryPart = getFirstTypeOfChilds(parseContext, QueryPartContext.class);
    // ParseTree regexQuery = getFirstTypeOfChilds(queryPart, RegexQueryContext.class);
    // ParseTree regexText = getFirstTypeOfChilds(regexQuery, RegexContext.class);
    // ParseTree groupText = getFirstTypeOfChilds(regexQuery, GroupContext.class);
    // print(regexText, parser);
    // print(groupText, parser);
    //
    // String procedQueryString = applyRegexCondition(regexQuery, true);
    // System.out.println(procedQueryString);

//    String s = insertNumericRegexConditionIntoGroup("Blutdruck (?<g1>[0-9]+)", "1", "abc");
    String s = removeNamedCapturingGroups("Blutdruck (?<g1>[0-9]+) asdf");
     System.out.println(s);
  }

  public static String buildRegex(RegexQueryContext ctx) {
    return buildRegex(ctx, false);
  }

  public static String buildRegex(RegexQueryContext ctx, boolean createNamedCapturingGrops) {
    // ParseTree queryPart = getFirstTypeOfChilds(ctx, QueryPartContext.class);
    // ParseTree regexQuery = getFirstTypeOfChilds(queryPart, RegexQueryContext.class);
    return applyRegexCondition(ctx, createNamedCapturingGrops);
  }

  private static String applyRegexCondition(ParseTree regexQuery,
          boolean createNamedCapturingGrops) {
    ParseTree regexText = getFirstTypeOfChilds(regexQuery, RegexContext.class);
    ParseTree regexCondition = getFirstTypeOfChilds(regexQuery, RegexConditionContext.class);
    String regex = extractRegex(regexText);
    regex = addBracketsAroundNamedEntities(regex);
    // if (createNamedCapturingGrops)
    regex = createNamedCapturingGrops(regex);
    if (regexCondition != null) {
      String groupNumber = getReferenceGroup(regexCondition);
      ParseTree numericCondition = getNumericCondition(regexCondition);
      String regexReplacement = buildRegexForNumericCondition(numericCondition);
      String replacedQueryString = insertNumericRegexConditionIntoGroup(regex, groupNumber,
              regexReplacement);
      // String replacedQueryString = regex.replace(reference, regexReplacement);
      replacedQueryString = handleReplacementBounderies(replacedQueryString, regexReplacement);
      regex = replacedQueryString;
    }
    String replacedQueryString = replacePredifienedEntities(regex);
    if (!createNamedCapturingGrops)
      replacedQueryString = removeNamedCapturingGroups(replacedQueryString);
    return replacedQueryString;

  }

  private static String removeNamedCapturingGroups(String replacedQueryString) {
    replacedQueryString = replacedQueryString.replaceAll("\\?<g[0-9]+>", "");
    return replacedQueryString;
  }

  private static String insertNumericRegexConditionIntoGroup(String regex, String groupNumber,
          String regexReplacement) {
    // System.out.println(regex);
    // System.out.println(groupNumber);
    // System.out.println(regexReplacement);
    regexReplacement = "(?<g" + groupNumber + ">" + regexReplacement + ")";
    String groupRegex = "\\(\\?<g" + groupNumber + "(.*?)\\)";
    Matcher matcher = Pattern.compile(groupRegex).matcher(regex);
    if (matcher.find()) {
      // System.out.println(matcher.group(1));
      String result = matcher.replaceFirst(regexReplacement);
      // System.out.println(result);
      return result;
    }
    return regex;
  }

  private static String createNamedCapturingGrops(String regex) {
    int index = 0;
    int groupNumber = 1;
    index = regex.indexOf("(", index);
    while (index >= 0) {
      index++;
      String groupName = "g" + groupNumber;
      groupNumber++;
      String namedCapturingGroup = "?<" + groupName + ">";
      regex = regex.substring(0, index) + namedCapturingGroup
              + regex.substring(index, regex.length());
      index += namedCapturingGroup.length();

      index = regex.indexOf("(", index);
    }
    // System.out.println(regex);
    return regex;
  }

  private static String addBracketsAroundNamedEntities(String regex) {
    regex = regex.replace(ZAHL, "(ZAHL)");
    return regex;
  }

  private static String handleReplacementBounderies(String s, String insertion) {
    int start = s.indexOf(insertion);
    // System.out.println(s.length());
    // System.out.println(start);
    int end = start + insertion.length();
    // System.out.println(end);
    // System.out.println(s.charAt(6));

    String pre = s.substring(0, start);
    String post = s.substring(end);
    // System.out.println("pre: '" + pre + "'");
    // System.out.println("post: '" + post + "'");
    String preBound = "[^0-9]";
    String postBound = "[^0-9]";
    if (pre.length() > 0) {
      preBound = "";
    }
    if (post.length() > 0) {
      postBound = "";
    }

    // System.out.println("pre: '" + pre + "'");
    // System.out.println("preBound: '" + preBound + "'");
    // System.out.println("post: '" + post + "'");
    // System.out.println("postBound: '" + postBound + "'");

    String regex = pre + preBound + insertion + postBound + post;
    // System.out.println(regex);
    return regex;
  }

  private static String buildRegexForNumericCondition(ParseTree numericCondition) {
    String operator = getOperator(numericCondition);
    String bound = getBound(numericCondition);
    if (bound != null || operator != null) {
      if (operator.equals("<"))
        return buildRegexForLowerThan(bound);
      else if (operator.equals("<="))
        return buildRegexForLowerEquals(bound);
      else if (operator.equals("="))
        return buildRegexForEqauls(bound);
      else if (operator.equals(">="))
        return buildRegexForGreaterEquals(bound);
      else if (operator.equals(">"))
        return buildRegexForGreaterThan(bound);
    }
    return null;
  }

  private static String buildRegexForGreaterThan(String bound) {
    return buildRegexForGreater(bound, false);
  }

  private static String buildRegexForGreater(String bound, boolean eqaualsAllowed) {
    int digits = bound.length();
    // System.out.println("bound: " + bound + " #digits: " + digits);
    List<String> alternatives = new ArrayList<>();
    int i = 0;
    for (int index = digits - 1; 0 <= index; index--) {
      String digit = bound.charAt(index) + "";
      if (!eqaualsAllowed)
        digit = increment(digit);
      if (digit.length() == 1) {
        String pre = bound.substring(0, index);
        String allNumbers = "[0-9]{" + i + "," + MAX_DIGITS + "}";
        String regexPart = pre + "[" + digit + "-9]";
        if (i > 0)
          regexPart = regexPart + allNumbers;
        regexPart = "(" + regexPart + ")";
        alternatives.add(regexPart);
      } else {
        // the original digit was 9 and got incremented to 10. that's to high, regex for that digit
        // can be skipped.
      }
      i++;
    }
    String allNumbers = "([1-9][0-9]{" + digits + "," + MAX_DIGITS + "})";
    alternatives.add(allNumbers);
    String regex = alternatives.stream().collect(Collectors.joining("|"));
    if (!regex.isEmpty())
      regex = "(" + regex + ")";
    // System.out.println(regex);
    return regex;
  }

  private static String buildRegexForLower(String bound, boolean eqaualsAllowed) {
    int digits = bound.length();
    // System.out.println("bound: " + bound + " #digits: " + digits);
    List<String> alternatives = new ArrayList<>();
    int i = 0;
    for (int index = digits - 1; 0 <= index; index--) {
      String digit = bound.charAt(index) + "";
      if (!eqaualsAllowed)
        digit = decrement(digit);
      if (Integer.parseInt(digit) != -1) {
        String pre = bound.substring(0, index);
        String allNumbers = "[0-9]{0," + i + "}";
        String regexPart = pre + "[0-" + digit + "]";
        if (i > 0)
          regexPart = regexPart + allNumbers;
        regexPart = "(" + regexPart + ")";
        alternatives.add(regexPart);
      } else {
        // the original digit was 0 and got decremented to -1. that's to low, regex for that digit
        // can be skipped.
      }
      i++;
    }
    if (digits >= 2) {
      int maxDigits = digits - 1;
      String allNumbers = "([0-9]{0," + maxDigits + "})";
      alternatives.add(allNumbers);
    }
    String regex = alternatives.stream().collect(Collectors.joining("|"));
    if (!regex.isEmpty())
      regex = "(" + regex + ")";
    // System.out.println(regex);
    return regex;
  }

  private static String buildRegexForGreaterEquals(String bound) {
    return buildRegexForGreater(bound, true);
  }

  private static String buildRegexForEqauls(String bound) {
    return bound;
  }

  private static String buildRegexForLowerEquals(String bound) {
    return buildRegexForLower(bound, true);
  }

  private static String buildRegexForLowerThan(String bound) {
    return buildRegexForLower(bound, false);
  }

  private static String increment(String digit) {
    int i = Integer.parseInt(digit);
    i++;
    return String.valueOf(i);
  }

  private static String decrement(String digit) {
    int i = Integer.parseInt(digit);
    i--;
    return String.valueOf(i);
  }

  private static String getBound(ParseTree numericCondition) {
    ParseTree bound = getFirstTypeOfChilds(numericCondition, BoundContext.class);
    if (bound != null)
      return bound.getText();
    return null;
  }

  private static String getOperator(ParseTree numericCondition) {
    ParseTree operator = getFirstTypeOfChilds(numericCondition, NumericOperatorContext.class);
    if (operator != null)
      return operator.getText();
    return null;
  }

  private static ParseTree getNumericCondition(ParseTree regexCondition) {
    ParseTree numericCondition = getFirstTypeOfChilds(regexCondition,
            NumericConditionContext.class);
    return numericCondition;
  }

  private static String getReferenceGroup(ParseTree regexCondition) {
    ParseTree reference = getFirstTypeOfChilds(regexCondition, ReferenceContext.class);
    ParseTree namedEntity = getFirstTypeOfChilds(reference, NamedEntityContext.class);
    if (namedEntity != null)
      return "1";
    else {
      // refence is a group
      String ref = reference.getText();
      ref = ref.replace("$", "");
      return ref;
    }
  }

  private static String replacePredifienedEntities(String regex) {
    String number = "([0-9]+)";
    String zahl = ZAHL;
    regex = regex.replace(zahl, number);
    return regex;
  }

  private static void print(ParseTree parseContext, Parser parser) {
    if (parseContext != null) {
      String className = parseContext.getClass().getSimpleName().replace("Context", "");
      print(parseContext, parser, className);
    }
  }

  private static void print(ParseTree parseContext, Parser parser, String string) {
    if (string == null)
      string = "";
    else
      string = string + ": ";
    if (parseContext != null)
      System.out.println(string + parseContext.toStringTree(parser));
  }

  private static QueryAttribute getQueryAttribute() throws QueryException {
    CatalogEntry catalogEntry = new CatalogEntry(0, "name", CatalogEntryType.Bool, "extid", 0, 0,
            "project", "uName", "desc", new Timestamp(0));
    QueryAttribute queryAttribute = new QueryAttribute(null, catalogEntry);
    return queryAttribute;
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

}
