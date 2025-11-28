package de.uniwue.dw.query.model.result;

import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.query.model.DWQueryConfig;
import de.uniwue.dw.query.model.lang.ExtractionMode;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.result.NearQueryHighlighter.NearQuery;
import de.uniwue.misc.util.RegexUtil;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Highlighter {

  public static final String ZAHL = "ZAHL";

  private HashMap<String, Pattern> patterns = new HashMap<String, Pattern>();

  public Highlighter() {
  }

  public void clear() {
    patterns.clear();
  }

  private Pattern getPattern(String aToken) {
    Pattern result = patterns.get(aToken);
    if (result == null) {
      result = createPatternRegex(aToken);
      patterns.put(aToken, result);
    }
    return result;
  }

  private static Pattern createPatternRegex(String aToken) {
    Pattern result;
    String wildcard = "WILDCARD";
    aToken = aToken.replace("*", wildcard);
    if (aToken.contains("-")) {
      String[] tokenSplit = aToken.split("-");
      StringBuilder tokenSplitResult = new StringBuilder();
      for (int i = 0; i < tokenSplit.length; i++) {
        tokenSplitResult.append(Pattern.quote(tokenSplit[i]));
        if (i < tokenSplit.length - 1)
          tokenSplitResult.append("(\\s|-)");
      }
      aToken = tokenSplitResult.toString();
    } else {
      aToken = Pattern.quote(aToken);
    }
    String regex = "(^|\\s|\\xA0|\\.|,|:|-|\\?|\\(|\\[|\\{|²)(" + aToken + ")($|\\s|\\xA0|\\.|,|=|:|-|\\?|\\)|\\]|\\})";
    regex = regex.replaceAll("\\s+", "\\\\E\\\\s+\\\\Q");
    regex = regex.replaceAll(wildcard, "\\\\E[\\\\wöäüÖÄÜß]*\\\\Q");
    // System.out.println(regex);
    result = Pattern.compile(regex, Pattern.MULTILINE | Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    return result;
  }

  public static void main(String[] args) {
    String input = "abasdfc abced    adf";
    // s = s.replaceAll("\\s+", "\\\\s+");
    // System.out.println(s);

    String query = "abced adf";
    Pattern compile = createPatternRegex(query);

    String regex =
            "(^|\\s|\\xA0|\\.|,|:|-|\\?|\\(|\\[|\\{)(" + "abc[\\wa-z]*" + ")($|\\s|\\xA0|\\.|,|:|-|\\?|\\)|\\]|\\})";
    // regex = regex.replaceAll("\\s+", "\\\\s+");
    // System.out.println(regex);
    // Pattern compile=Pattern.compile(regex);

    Matcher matcher = compile.matcher(input);
    if (matcher.find()) {
      System.out.println(matcher.group());
    }
  }

  public boolean hasToHighlight(QueryAttribute attribute) {
    return attribute.getContentOperator().isQueryWordOperator();
  }

  public String processExtractionMode(String snippet, QueryAttribute attribute) {
    if (snippet == null) {
      return null;
    }
    String result = snippet.replaceAll("[\\s|\\xA0]", " ");
    if (attribute.getExtractionMode() == ExtractionMode.NextNumber) {
      int indexOf = snippet.indexOf(DWQueryConfig.queryHighlightPost());
      if (indexOf > 0) {
        result = result.substring(indexOf + DWQueryConfig.queryHighlightPost().length());
        Matcher numberMatcher = RegexUtil.numbersRegex.matcher(result);
        boolean find = numberMatcher.find();
        if (find) {
          String cut = result.substring(numberMatcher.start(), numberMatcher.end());
          result = cut;
        } else {
          result = null;
        }
      } else {
        result = null;
      }
    }
    if (attribute.getExtractionMode() == ExtractionMode.BetweenHits) {
      result = showOnlySnippetTextBetweenQueryTerms(result, attribute.getDesiredContent());
    }
    return result;
  }

  private String showOnlySnippetTextBetweenQueryTerms(String result, String argument) {
    int indexOfFirstToken = result.indexOf(DWQueryConfig.queryHighlightPre());
    if (indexOfFirstToken >= 0)
      result = result.substring(indexOfFirstToken);
    int indexOfLastToken = result.lastIndexOf(DWQueryConfig.queryHighlightPost())
            + DWQueryConfig.queryHighlightPost().length();
    if (indexOfLastToken >= 0) {
      result = result.substring(0, indexOfLastToken);
    }
    return result;
  }

  public boolean highlight(QueryAttribute attribute, Information anInfo) {
    ParseTree desiredContentAsParseTree = attribute.getDesiredContentAsParseTree();
    // DesiredContentCheckerVisitor checkerVisitor = new DesiredContentCheckerVisitor();
    // checkerVisitor.visit(desiredContentAsParseTree);
    TokenCollectorVisitor tokenCollector = new TokenCollectorVisitor(true);
    tokenCollector.visit(desiredContentAsParseTree);
    final List<String> tokens = tokenCollector.tokens;
    final List<NearQuery> nearQueries = tokenCollector.getNearQueries();
    final List<String[]> regexContents = tokenCollector.getRegexContents();

    // until Issue #126 is fixed the words are just added as simple tokens
    // for (final NearQuery aNearQuery : nearQueries) {
    // tokens.addAll(aNearQuery.words);
    // }

    String value = anInfo.getValue();
    String newValueShort = null;
    String newValue = value;
    String[] newValues = highlightNonRegexTokens(tokens, value, newValueShort, newValue);
    newValueShort = newValues[0];
    newValue = newValues[1];

    if (regexContents.size() > 0) {
      newValues = highlightRegexTokens(regexContents, value, newValueShort, newValue);
      newValueShort = newValues[0];
      newValue = newValues[1];
    }

    // commented out until highlighting bug is fixed (Issue #126)
    if (nearQueries.size() > 0) {
      newValues = NearQueryHighlighter.highlight(nearQueries, value, newValueShort, newValue);
      newValueShort = newValues[0];
      newValue = newValues[1];
    }

    if (attribute.getExtractionMode() != ExtractionMode.None) {
      newValueShort = processExtractionMode(newValueShort, attribute);
      if (newValueShort == null) {
        return false;
      }
    }
    if (newValueShort == null) { // no hit found
      return false;
    } else {
      anInfo.setValueShort(newValueShort);
      anInfo.setValue(newValue);
    }
    return true;
  }

  public String[] highlight(ParseTree parseTree, String value) {
    TokenCollectorVisitor tokenCollector = new TokenCollectorVisitor(true);
    tokenCollector.visit(parseTree);
    final List<String> tokens = tokenCollector.tokens;
    final List<NearQuery> nearQueries = tokenCollector.getNearQueries();
    final List<String[]> regexContents = tokenCollector.getRegexContents();

    // until Issue #126 is fixed the words are just added as simple tokens
    // for (final NearQuery aNearQuery : nearQueries) {
    // tokens.addAll(aNearQuery.words);
    // }

    String newValueShort = null;
    String newValue = value;
    String[] newValues = highlightNonRegexTokens(tokens, value, newValueShort, newValue);
    newValueShort = newValues[0];
    newValue = newValues[1];

    if (regexContents.size() > 0) {
      newValues = highlightRegexTokens(regexContents, value, newValueShort, newValue);
      newValueShort = newValues[0];
      newValue = newValues[1];
    }
    return new String[] { newValueShort, newValue };
  }

  // private static String[] highlightRegexTokens(List<String[]> regexContents, String value,
  // String newValueShort, String newValue) {
  // for (String[] regexContent : regexContents) {
  // List<String> hits = new ArrayList<>();
  // String regex = regexContent[0];
  // String outputDefinition = regexContent[1];
  // // Integer group = getGroup(groupText);
  // boolean displayRegexOnly = true;
  // if (outputDefinition == null || outputDefinition.isEmpty()) {
  // displayRegexOnly = false;
  // outputDefinition = "$0";
  // } else {
  // outputDefinition = setNamedCapturingGroups(outputDefinition);
  // }
  // Pattern pattern = Pattern.compile(regex,
  // Pattern.MULTILINE | Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  // Matcher matcher = pattern.matcher(value);
  //
  // StringBuffer sb = new StringBuffer();
  // while (matcher.find()) {
  // int start = matcher.start();
  // matcher.appendReplacement(sb, outputDefinition);
  // String replaced = sb.toString();
  // String hit = replaced.substring(start);
  // String hitGroup0 = matcher.group();
  // // System.out.println(hit);
  // // String hit = matcher.group(group);
  // if (hit != null) {
  // hits.add(hit);
  // String highlitedHit = DWQueryConfig.queryHighlightPre() + hit
  // + DWQueryConfig.queryHighlightPost();
  // String highlitedhitGroup0 = DWQueryConfig.queryHighlightPre() + hitGroup0
  // + DWQueryConfig.queryHighlightPost();
  // newValue = newValue.replace(hitGroup0, highlitedhitGroup0);
  // if (!displayRegexOnly) {
  // if (newValueShort == null) {
  // newValueShort = value.substring(
  // Math.max(0,
  // matcher.start() - (int) DWQueryConfig.queryHighlightFragSize() / 2),
  // Math.min(value.length(),
  // matcher.end() + (int) DWQueryConfig.queryHighlightFragSize() / 2));
  // }
  // newValueShort = newValueShort.replace(hit, highlitedHit);
  // }
  // }
  // }
  // if (displayRegexOnly) {
  // if (!hits.isEmpty()) {
  // newValueShort = newValueShort == null ? "" : newValueShort;
  // if (!newValueShort.isEmpty())
  // newValueShort += ", ";
  // newValueShort += hits.stream().collect(Collectors.joining(", "));
  // }
  // }
  // }
  // String[] newValues = { newValueShort, newValue };
  // return newValues;
  // }

  private static String[] highlightRegexTokens(List<String[]> regexContents, String value, String newValueShort,
          String newValue) {
    String pre = DWQueryConfig.queryHighlightPre();
    String post = DWQueryConfig.queryHighlightPost();

    for (String[] regexContent : regexContents) {
      List<String> hits = new ArrayList<>();
      String regex = regexContent[0];
      String outputDefinition = regexContent[1];
      Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE | Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

      if (outputDefinition == null || outputDefinition.isEmpty()) {
        String highlightedNewValue = newValueShort;
        if (highlightedNewValue == null)
          highlightedNewValue = value;
        Matcher valueShortMatcher = pattern.matcher(highlightedNewValue);
        highlightedNewValue = valueShortMatcher.replaceAll(pre + "$0" + post);
        if (newValueShort == null)
          newValueShort = cutOutSnippet(highlightedNewValue);
        else
          newValueShort = highlightedNewValue;
      } else {
        if (newValueShort != null && !newValueShort.isEmpty())
          hits.add(newValueShort);
        outputDefinition = setNamedCapturingGroups(outputDefinition);
        Matcher matcher = pattern.matcher(value);
        int lastGroupEnd = 0;
        while (matcher.find()) {
          StringBuffer sb = new StringBuffer();
          int start = matcher.start();
          int end = matcher.end();
          matcher.appendReplacement(sb, outputDefinition);
          String replaced = sb.toString();
          String hit = replaced.substring(start - lastGroupEnd);
          lastGroupEnd = end;
          if (hit != null && !hit.isEmpty()) {
            hits.add(hit);
          }
        }
        newValueShort = hits.stream().collect(Collectors.joining(", "));

      }

      Matcher valueMatcher = pattern.matcher(value);
      newValue = valueMatcher.replaceAll(pre + "$0" + post);

    }
    if (newValueShort.isEmpty())
      newValueShort = null;
    String[] newValues = { newValueShort, newValue };
    return newValues;
  }

  private static String cutOutSnippet(String value) {
    int start = value.indexOf(DWQueryConfig.queryHighlightPre());
    int end = value.indexOf(DWQueryConfig.queryHighlightPost());
    if (start >= 0 && end >= 0) {
      String snippet = value.substring(Math.max(0, start - (int) DWQueryConfig.queryHighlightFragSize() / 2),
              Math.min(value.length(), end + (int) DWQueryConfig.queryHighlightFragSize() / 2));
      return snippet;
    }
    return value;
  }

  // private static String[] highlightRegexTokens(List<String[]> regexContents, String value,
  // String newValueShort, String newValue) {
  // for (String[] regexContent : regexContents) {
  // List<String> hits = new ArrayList<>();
  // String regex = regexContent[0];
  // String groupText = regexContent[1];
  // Integer group = getGroup(groupText);
  // boolean displayRegexOnly = true;
  // if (group == null) {
  // displayRegexOnly = false;
  // group = 0;
  // }
  // Pattern pattern = Pattern.compile(regex);
  // Matcher matcher = pattern.matcher(value);
  //
  // Matcher m2 = pattern.matcher(input);
  // m2.find();
  // int start = m2.start();
  // m2.appendReplacement(sb, output);
  // String replaced = sb.toString();
  // System.out.println(replaced);
  // System.out.println(replaced.substring(start));
  //
  //
  // while (matcher.find()) {
  // String hit = matcher.group(group);
  // if (hit != null) {
  // hits.add(hit);
  // String highlitedHit = DWQueryConfig.queryHighlightPre() + hit
  // + DWQueryConfig.queryHighlightPost();
  // newValue = newValue.replace(hit, highlitedHit);
  // if (!displayRegexOnly) {
  // if (newValueShort == null) {
  // newValueShort = value.substring(
  // Math.max(0,
  // matcher.start() - (int) DWQueryConfig.queryHighlightFragSize() / 2),
  // Math.min(value.length(),
  // matcher.end() + (int) DWQueryConfig.queryHighlightFragSize() / 2));
  // }
  // newValueShort = newValueShort.replace(hit, highlitedHit);
  // }
  // }
  // }
  // if (displayRegexOnly) {
  // if (!hits.isEmpty()) {
  // newValueShort = newValueShort == null ? "" : newValueShort;
  // if (!newValueShort.isEmpty())
  // newValueShort += ", ";
  // newValueShort += hits.stream().collect(Collectors.joining(", "));
  // }
  // }
  // }
  // String[] newValues = { newValueShort, newValue };
  // return newValues;
  // }

  private static String setNamedCapturingGroups(String outputDefinition) {
    outputDefinition = outputDefinition.replace(ZAHL, "$1");
    String referenceRegex = "\\$([0-9]+)";
    Pattern pattern = Pattern.compile(referenceRegex);
    Matcher matcher = pattern.matcher(outputDefinition);
    if (matcher.find()) {
      String replaceFirst = matcher.replaceAll("\\${g$1}");
      return replaceFirst;
    }
    return referenceRegex;
  }

  private static Integer getGroup(String groupText) {
    Integer group = null;
    if (groupText != null) {
      try {
        group = Integer.parseInt(groupText);
      } catch (NumberFormatException e) {
        // ignore
      }
    }
    return group;
  }

  private String[] highlightNonRegexTokens(List<String> tokens, String value, String newValueShort, String newValue) {
    if (value == null)
      return new String[] { null, null };
    for (String aToken : tokens) {
      Pattern compile = getPattern(aToken);
      Matcher matcher = compile.matcher(value);
      if (matcher.find()) {
        if (newValueShort == null) {
          newValueShort = value.substring(
                  Math.max(0, matcher.start() - (int) DWQueryConfig.queryHighlightFragSize() / 2),
                  Math.min(value.length(), matcher.end() + (int) DWQueryConfig.queryHighlightFragSize() / 2));
        } else {
          Matcher subMatcher = compile.matcher(newValueShort);
          if (!subMatcher.find()) {
            newValueShort += "\n##NEW-HIT##\n" + value.substring(
                    Math.max(0, matcher.start() - (int) DWQueryConfig.queryHighlightFragSize() / 2),
                    Math.min(value.length(), matcher.end() + (int) DWQueryConfig.queryHighlightFragSize() / 2));
          }
        }
        newValueShort = compile.matcher(newValueShort).replaceAll(
                "$1" + DWQueryConfig.queryHighlightPre() + "$2" + DWQueryConfig.queryHighlightPost() + "$3");
        newValue = compile.matcher(newValue).replaceAll(
                "$1" + DWQueryConfig.queryHighlightPre() + "$2" + DWQueryConfig.queryHighlightPost() + "$3");
      }
    }
    String[] newValues = { newValueShort, newValue };
    return newValues;
  }

}
