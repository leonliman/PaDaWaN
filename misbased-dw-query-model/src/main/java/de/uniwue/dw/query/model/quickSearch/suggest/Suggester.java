package de.uniwue.dw.query.model.quickSearch.suggest;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.manager.DataSourceException;
import de.uniwue.dw.core.model.manager.ICatalogAndTextSuggester;
import de.uniwue.dw.query.model.DWQueryConfig;
import de.uniwue.dw.query.model.lang.ReductionOperator;
import de.uniwue.dw.query.model.quickSearch.AttributeDuringTipping;
import de.uniwue.dw.query.model.quickSearch.QuickSearchAPI;
import de.uniwue.dw.query.model.quickSearch.QuickSearchUtil;
import de.uniwue.misc.util.ConfigException;

public class Suggester implements IInputSuggester {

  public static final int DEFAULT_NUMBER_OF_SUGGESTED_TEXT_TOKENS = 10;

  private static final Logger logger = LogManager.getLogger(Suggester.class);

  private HashMap<User, List<SuggestObject>> user2queryExamples = new HashMap<>();

  protected static DecimalFormat formatter = (DecimalFormat) NumberFormat
          .getInstance(Locale.GERMANY);

  protected ICatalogAndTextSuggester lookupUtils;

  public Suggester(ICatalogAndTextSuggester indexLookupUtils) {
    lookupUtils = indexLookupUtils;
  }

  public List<SuggestObject> suggestTextToken(String token) {
    return suggestTextToken(token, DEFAULT_NUMBER_OF_SUGGESTED_TEXT_TOKENS);
  }

  @Override
  public List<SuggestObject> suggestTextToken(String token, int numberOfSuggestions) {
    List<String> suggestedTokens = lookupUtils.suggestTextTokens(token, numberOfSuggestions);
    return toSuggestObjectList(suggestedTokens);
  }

  public List<CatalogEntry> suggestCatalogEntry(String token, User user,
          int numberOfCatalogSuggestions) {
    List<CatalogEntry> suggestedEntries = lookupUtils.suggestCatalogEntries(token, user,
            numberOfCatalogSuggestions);
    return suggestedEntries;
  }

  public List<SuggestObject> suggestCatalogEntries(String token, User user,
          int numberOfCatalogSuggestions) {
    return toSuggestObjects(suggestCatalogEntry(token, user, numberOfCatalogSuggestions));
  }

  public List<CatalogEntry> suggestCatalogEntry(String token, User user, CatalogEntryType dataType,
          int numberOfCatalogSuggestions) {
    List<CatalogEntry> suggestedEntries = lookupUtils.suggestCatalogEntries(token, user, dataType,
            numberOfCatalogSuggestions);
    return suggestedEntries;
  }

  public List<CatalogEntry> getCatalogEntrySons(CatalogEntry entry, User user,
          int numberOfCatalogSuggestions) {
    List<CatalogEntry> suggestedEntries = lookupUtils.getCatalogEntrySons(entry, user,
            numberOfCatalogSuggestions);
    return suggestedEntries;
  }

  public List<SuggestObject> suggestQueryEntries(String token, User user) {
    token = token.trim();
    List<SuggestObject> result = new ArrayList<>();
    int numberOfCatalogSuggestions = 6;
    int numberOfTextSuggestions = 2;
    AttributeDuringTipping attribute = QuickSearchAPI.parseAttributeDuringTipping(token,
            lookupUtils);
    if (isTextQuery(attribute)) {
      result.addAll(suggestTextQueryTokens(attribute, user, 8));
      numberOfCatalogSuggestions = 0;
      numberOfTextSuggestions = 0;
    } else {
      CatalogEntry catalogEntry = QuickSearchUtil.parseEntryByName(attribute.getCatalogEntryText(),
              user, lookupUtils);
      if (catalogEntry != null) {
        // if (!attribute.hasOperatorsOrArguments())
        // result.add(toSuggestObject(catalogEntry));
        result.addAll(toSuggestObjectList(suggestOperatorsForCatalogEntry(catalogEntry, attribute),
                catalogEntry, attribute));
        result.addAll(toSuggestObjects(
                getCatalogEntrySons(catalogEntry, user, numberOfCatalogSuggestions)));
        numberOfCatalogSuggestions = 0;
        numberOfTextSuggestions = 0;
      }
    }
    if (attribute.hasCatalogEntryText()) {
      String entryText = attribute.getCatalogEntryText();
      if (attribute.getUnparsedInput() != null && !attribute.getUnparsedInput().isEmpty()) {
        entryText += " " + attribute.getUnparsedInput();
      }
      result.addAll(
              toSuggestObjects(suggestCatalogEntry(entryText, user, numberOfCatalogSuggestions)));
      result.addAll(
              suggestTextTokensOfMostCommonTextField(entryText, user, numberOfTextSuggestions));
    }
    return result;
  }

  private Collection<SuggestObject> suggestTextTokensOfMostCommonTextField(String queryToken,
          User user, int numberOfSuggestions) {
    List<SuggestObject> result = new ArrayList<>();
    if (numberOfSuggestions <= 0)
      return result;
    CatalogEntry mostCommonTextField;
    try {
      if (lookupUtils.getMostCommonTextFields(user).isEmpty()) {
        return result;
      }
      mostCommonTextField = lookupUtils.getMostCommonTextFields(user).get(0);
      AttributeDuringTipping attr = new AttributeDuringTipping();
      attr.setIn("in:");
      attr.setQueryTokens(queryToken);
      result = suggestTextToken(attr, mostCommonTextField, numberOfSuggestions);
    } catch (ConfigException e) {
      e.printStackTrace();
    }
    return result;
  }

  protected List<SuggestObject> suggestTextQueryTokens(AttributeDuringTipping attribute, User user,
          int numberOfTokens) {
    if (!attribute.hasCatalogEntryText()) {
      return suggestMostCommonTextFields(attribute, user);
    } else {
      String catalogEntryName = attribute.getCatalogEntryText();
      String prefix = "";
      if (attribute.hasNegation())
        prefix += attribute.getNegation();
      prefix += attribute.getIn();
      CatalogEntry catalogEntry = QuickSearchUtil.parseEntryByName(catalogEntryName, user,
              lookupUtils);
      if (catalogEntry == null) {
        List<SuggestObject> textCatalogEntry = toSuggestObjects(
                suggestCatalogEntry(catalogEntryName, user, CatalogEntryType.Text, 7));
        final String finalPrefix = prefix;
        textCatalogEntry.forEach(n -> n.setSuggestText(finalPrefix + n.getSuggestText()));
        return textCatalogEntry;
      } else {
        return suggestTextToken(attribute, catalogEntry, numberOfTokens);
      }
    }
  }

  protected List<SuggestObject> suggestMostCommonTextFields(AttributeDuringTipping attribute,
          User user) {

    String prefix = attribute.getNegation() + attribute.getIn();
    List<SuggestObject> result = new ArrayList<>();
    try {
      result = lookupUtils.getMostCommonTextFields(user).stream().map(n -> toSuggestObject(n))
              .collect(Collectors.toList());
      result.forEach(n -> n.setSuggestText(prefix + n.getSuggestText()));
    } catch (ConfigException e) {
      e.printStackTrace();
    }
    return result;
  }

  private List<SuggestObject> suggestTextToken(AttributeDuringTipping attribute,
          CatalogEntry catalogEntry, int numberOfSuggestions) {
    String prefix = "";
    if (attribute.hasNegation())
      prefix += attribute.getNegation();
    prefix += attribute.getIn() + format(catalogEntry) + " ";
    if (attribute.hasPlus())
      prefix += attribute.getPlus() + " ";
    String queryToken = "";
    if (attribute.hasQueryTokens()) {
      String tokens = attribute.getQueryTokens();
      String[] split = tokens.split(" ");
      String lastToken = split[split.length - 1];
      queryToken = lastToken;
      int lastIndexOfQueryToken = tokens.lastIndexOf(queryToken);
      String previosTokens = tokens.substring(0, lastIndexOfQueryToken).trim();
      prefix += previosTokens + " ";

    }
    final String finalPrexfix = prefix;
    List<String> suggestTextTokens = lookupUtils.suggestTextTokens(queryToken, catalogEntry,
            numberOfSuggestions);
    suggestTextTokens = suggestTextTokens.stream().map(n -> finalPrexfix + n)
            .collect(Collectors.toList());
    return toSuggestObjectList(suggestTextTokens);
  }

  protected static boolean isTextQuery(AttributeDuringTipping attribute) {
    return attribute.hasIn();
  }

  protected static List<String> createSuggestText(CatalogEntry entry, String[] operators) {
    String name = QuickSearchUtil.formatCatalogEntryWithAtNotation(entry);
    return Arrays.asList(operators).stream().map(n -> name + " " + n).collect(Collectors.toList());
  }

  public List<SuggestObject> suggestTextTokensAndCatalogEntries(String token, User user,
          int numberOfSuggestions) {
    List<SuggestObject> textTokens = suggestTextToken(token);
    List<SuggestObject> catalogEntries = toSuggestObjects(
            suggestCatalogEntry(token, user, numberOfSuggestions));

    List<SuggestObject> suggestedTextTokens = new ArrayList<>();
    List<SuggestObject> suggestedCatalogEntries = new ArrayList<>();

    int index = 0;
    int suggetions = 0;
    while (suggetions < numberOfSuggestions) {
      if (addIfExits(textTokens, index, suggestedTextTokens))
        suggetions++;
      if (addIfExits(catalogEntries, index, suggestedCatalogEntries))
        suggetions++;
    }
    List<SuggestObject> result = new ArrayList<>();
    result.addAll(suggestedTextTokens);
    result.addAll(suggestedCatalogEntries);

    return result;
  }

  protected static boolean addIfExits(List<SuggestObject> source, int index,
          List<SuggestObject> target) {
    if (source.size() > index) {
      return target.add(source.get(index));
    }
    return false;
  }

  protected static String[] suggestOperatorsForCatalogEntry(CatalogEntry entry,
          AttributeDuringTipping attribute) {
    String name = format(entry);
    String exits = name;
    switch (entry.getDataType()) {
      case Text:
        String queryTokens = attribute.hasQueryTokens() ? " " + attribute.getQueryTokens() : "";
        String containsPositive = "in:" + name + queryTokens;
        String containsNotPositive = "-in" + name + queryTokens;
        return new String[] { exits, containsPositive, containsNotPositive };
      case Bool:
      case Structure:
        String notExits = "-" + name;
        String successors = name + " " + QuickSearchAPI.SUCCCESORS;
        return new String[] { exits, notExits, successors };
      case Number: {
        return createNumericOperators(exits, entry, attribute);
      }
      case DateTime:
        String lower = name + "< 1.1.2010";
        String higher = name + " >= 24.12.2010";
        String interval = name + " 1.1.2010 ... 1.2.2010";
        String perYear = name + " 2010 - 2016";
        return new String[] { lower, higher, interval, perYear };
      default:
        return new String[] {};
    }
  }

  private static String[] createNumericOperators(String name, CatalogEntry entry,
          AttributeDuringTipping attribute) {
    String prefix = name;
    if (attribute.hasReductionOperator()) {
      prefix += " " + attribute.getReductionOperator();
    }
    if (attribute.hasLowerBound()) {
      prefix += " " + attribute.getLowerBound();
      prefix += "...";
      if (attribute.hasDots()) {
        if (attribute.hasUpperBound()) {
          return new String[] { prefix + attribute.getUpperBound() };
        } else {
          if (Double.valueOf(attribute.getLowerBound()) < entry.getHighBound())
            return new String[] { prefix + formatNumber(entry.getHighBound()) };
          else
            return new String[] {
                prefix + formatNumber(Double.valueOf(attribute.getLowerBound()) + 1) };
        }
      } else {
        // no Dots
        String nameWithDots = prefix;
        if (Double.valueOf(attribute.getLowerBound()) < entry.getHighBound()) {
          String nameWithUpperBound = prefix + formatNumber(entry.getHighBound());
          return new String[] { nameWithDots, nameWithUpperBound };
        } else
          return new String[] { nameWithDots };
      }
    } else if (attribute.hasNumericOperator()) {
      prefix += " " + attribute.getNumericOperator();
      if (attribute.hasBound())
        return new String[] { prefix + " " + attribute.getBound() };
      else
        return new String[] { prefix + " " + formatNumber(entry.getLowBound()) };
    } else {
      String lower = prefix + " < " + formatNumber(entry.getLowBound());
      String higher = prefix + " >= " + formatNumber(entry.getHighBound());
      String intervalArgument = "0...100";
      if (entry.getLowBound() != 0 || entry.getHighBound() != 0) {
        intervalArgument = formatNumber(entry.getLowBound()) + "..."
                + formatNumber(entry.getHighBound());
      }
      String interval = prefix + " " + intervalArgument;
      String intervalWithInfinity = prefix + " ..." + intervalArgument + "...";
      if (!attribute.hasReductionOperator()) {
        String max = name + " " + ReductionOperator.MAX.getQuickSearchText();
        String min = name + " " + ReductionOperator.MIN.getQuickSearchText();
        String first = name + " " + ReductionOperator.EARLIEST.getQuickSearchText();
        String last = name + " " + ReductionOperator.LATEST.getQuickSearchText();
        return new String[] { lower, higher, interval, intervalWithInfinity, max, min, first,
            last };
      }
      return new String[] { lower, higher, interval };
    }

  }

  protected static String formatNumber(double lowBound) {
    return formatter.format(lowBound);
  }

  protected static List<SuggestObject> toSuggestObjectList(SuggestObject[] strings) {
    return Arrays.asList(strings);
  }

  protected static List<SuggestObject> toSuggestObjectList(List<String> strings) {
    return strings.stream().map(Suggester::toSuggestObject).collect(Collectors.toList());
  }

  protected static List<SuggestObject> toSuggestObjectList(String[] suggestTexts,
          CatalogEntry catalogEntry, AttributeDuringTipping attribut) {
    List<String> suggestTextList = Arrays.asList(suggestTexts);
    return suggestTextList.stream()
            .map(suggestText -> new SuggestObject(suggestText, catalogEntry, attribut))
            .collect(Collectors.toList());
  }

  protected static SuggestObject toSuggestObject(String string) {
    return new SuggestObject(string, false);
  }

  protected static SuggestObject toSuggestObject(CatalogEntry entry) {
    return toSuggestObject(entry, 0);
  }

  protected static SuggestObject toSuggestObject(CatalogEntry entry, int positionInInput) {
    return toSuggestObject(entry, positionInInput, null, null);
  }

  public static SuggestObject toSuggestObject(CatalogEntry entry, int positionInInput,
          String prefix, String argument) {
    String name = format(entry);
    prefix = prefix == null ? "" : prefix;
    argument = argument == null ? "" : " " + argument;
    String suggestText = prefix + name + argument;
    return new SuggestObject(suggestText, entry.getUniqueName(), entry.getDataType(),
            entry.getCountAbsolute(), entry.getCountDistinctCaseID(), entry.getCountDistinctPID(),
            true, positionInInput, null, null, entry.getAttrId());
  }

  protected static List<SuggestObject> toSuggestObjects(List<CatalogEntry> entries) {
    return entries.stream().map(Suggester::toSuggestObject).collect(Collectors.toList());
  }

  public List<SuggestObject> suggestConjunctions(int position) {
    String[] conjunctions = { "UND", "ODER" };
    List<SuggestObject> result = new ArrayList<>();
    for (String s : conjunctions) {
      SuggestObject o = new SuggestObject(s, false);
      o.setPositioInInput(position);
      result.add(o);
    }
    return result;
  }

  protected static String format(CatalogEntry entry) {
    return QuickSearchUtil.formatCatalogEntryWithUniquiNameNotation(entry);
  }

  public List<SuggestObject> suggestQueryEntriesForEmtpyInput(User user, int inputPosition) {
    List<SuggestObject> exampleQueries = user2queryExamples.get(user);
    if (exampleQueries == null) {
      exampleQueries = crateQueryEntriesForEmtpyInput(user, inputPosition);
      user2queryExamples.put(user, exampleQueries);
    } else {
      exampleQueries.forEach(n -> n.setPositioInInput(inputPosition));
    }
    return exampleQueries;
  }

  private List<SuggestObject> crateQueryEntriesForEmtpyInput(User user, int inputPosition) {
    List<SuggestObject> result = new ArrayList<>();
    for (String[] parts : DWQueryConfig.getQuickSearchExamples()) {
      String prefix = parts[0];
      String extID = parts[1];
      String project = parts[2];
      String argument = parts[3];
      try {
        CatalogEntry entry = lookupUtils.getCatalogEntryByExtid(extID, project, user);
        if (entry != null) {
          result.add(toSuggestObject(entry, inputPosition, prefix, argument));
        }
      } catch (DataSourceException e) {
        logger.warn("catalog entry does not exist for given user! extid: {}, project: {}, user: {}",
                extID, project, user.getUsername());
      }
    }
    return result;
  }

}
