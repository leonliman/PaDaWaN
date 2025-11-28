package de.uniwue.dw.query.model.quickSearch;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.manager.DataSourceException;
import de.uniwue.dw.core.model.manager.ICatalogAndTextSuggester;
import de.uniwue.dw.query.model.data.StoredQueryTreeEntry;
import de.uniwue.dw.query.model.manager.IQueryClientIOManager;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchBaseVisitor;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser;
import de.uniwue.misc.util.ConfigException;

public class QuickSearchUniqueNameNotationFormatVisitor extends QuickSearchBaseVisitor<String> {

  private static final String NO_WITHE_SPACE = "#=<!>=#";

  private static final String UNKNOWN_CATALOG_ENTRY = "Der Eintrag '%s' existiert nicht.<br>"
          + "Wenn Sie danach in Arztbriefen suchen wollen, geben Sie folgendes ein:<br>" + "%s";

  private static final String CATALOG_ENTRY_IS_EMPTY_ERROR_MESSAGE = "Sie haben keinen Katalogeintrag angegeben.";

  private static final String WRONG_CATALOG_ENTRY_FOR_NUMERIC_OPERATOR = "Der Eintrag '%s' ist vom Typ %s.<br>"
          + "Sie können keinen numerischen Vergleichsoperator verwenden.";

  private static final String WRONG_CATALOG_ENTRY_FOR_TEXT_OPERATOR = "Der Eintrag '%s' ist vom Typ %s.<br>"
          + "Sie können darin keine Textsuche machen.";

  private static final String UNKNOWN_STORED_QUERY = "Die gepeicherte Suche '%s' ist nicht verfügbar.";

  private String input;

  private User user;

  private ICatalogAndTextSuggester suggester;

  private List<SyntaxError> errors = new ArrayList<>();

  /**
   * this is the last catalog entry which was parsed in the tree.
   */
  private CatalogEntry lastEntry;

  private IQueryClientIOManager queryClientIOManager;

  public QuickSearchUniqueNameNotationFormatVisitor(String input, User user,
          ICatalogAndTextSuggester suggester, IQueryClientIOManager queryClientIOManager) {
    this.input = input;
    this.user = user;
    this.suggester = suggester;
    this.queryClientIOManager = queryClientIOManager;
  }

  public String visitChildren(RuleNode node, String delimiter) {
    String result = defaultResult();
    int n = node.getChildCount();
    for (int i = 0; i < n; i++) {
      if (!shouldVisitNextChild(node, result)) {
        break;
      }

      ParseTree c = node.getChild(i);
      String childResult = c.accept(this);
      result = aggregateResult(result, childResult, delimiter);
    }

    return result;
  }

  protected String aggregateResult(String aggregate, String nextResult, String delimiter) {
    if (aggregate == null && nextResult == null)
      return null;
    if (aggregate != null && nextResult == null)
      return aggregate;
    else if (aggregate == null && nextResult != null)
      return nextResult;
    else {
      if (aggregate.endsWith(NO_WITHE_SPACE)) {
        aggregate = aggregate.substring(0, aggregate.length() - NO_WITHE_SPACE.length());
        delimiter = "";
      }
      return aggregate + delimiter + nextResult;
    }
  }

  @Override
  protected String aggregateResult(String aggregate, String nextResult) {
    return aggregateResult(aggregate, nextResult, " ");
  }

  @Override
  public String visitParse(QuickSearchParser.ParseContext ctx) {
    return visitChildren(ctx).trim();
  }

  @Override
  public String visitBooleanOr(QuickSearchParser.BooleanOrContext ctx) {
    String first = ctx.getChild(0).accept(this);
    String second = ctx.getChild(2).accept(this);
    return first + " ODER " + second;
  }

  @Override
  public String visitParenExpression(QuickSearchParser.ParenExpressionContext ctx) {
    return "(" + ctx.getChild(1).accept(this) + ")";
  }

  @Override
  public String visitBooleanAnd(QuickSearchParser.BooleanAndContext ctx) {
    String first = ctx.getChild(0).accept(this);
    String second = ctx.getChild(2).accept(this);
    return first + " UND " + second;
  }

  @Override
  public String visitQueryAttribute(QuickSearchParser.QueryAttributeContext ctx) {
    return visitChildren(ctx, "");
  }

  @Override
  public String visitCatalogEntry(QuickSearchParser.CatalogEntryContext ctx) {
    String uniqueName = ctx.getText();
    if (uniqueName.isEmpty()) {
      String errorText = getCatalogEntryIsEmptyErrorMessage();
      SyntaxError error = new SyntaxError(ctx.getStart().getStartIndex(), errorText);
      errors.add(error);
      return uniqueName;
    }
    if (QuickSearch2QueryRootVisitor.isStoredQueryElem(uniqueName)) {
      Optional<StoredQueryTreeEntry> storedQueryElem = QuickSearch2QueryRootVisitor
              .getStoredQueryElem(uniqueName, user, queryClientIOManager);
      if (!storedQueryElem.isPresent()) {
        String errorText = getUnknownStoredQueryErrorMessage(uniqueName);
        SyntaxError error = new SyntaxError(ctx.getStart().getStartIndex(), errorText);
        errors.add(error);
      }
    } else {
      CatalogEntry entry = null;
      try {
        entry = suggester.getCatalogEntryByUniqueName(uniqueName, user);
      } catch (DataSourceException e) {
        // entry does not exist.
      }
      lastEntry = entry;
      if (entry == null) {
        String errorText = getUnknownCatalogEntryErrorMessage(uniqueName);
        SyntaxError error = new SyntaxError(ctx.getStart().getStartIndex(), errorText);
        errors.add(error);
      }
    }
    return uniqueName;
  }

  private String getUnknownStoredQueryErrorMessage(String uniqueName) {
    String storedQuery = QuickSearch2QueryRootVisitor.getStoredQueryName(uniqueName);
    return String.format(UNKNOWN_STORED_QUERY, storedQuery);

  }

  private String getCatalogEntryIsEmptyErrorMessage() {
    return CATALOG_ENTRY_IS_EMPTY_ERROR_MESSAGE;
  }

  private String getUnknownCatalogEntryErrorMessage(String input) {
    List<CatalogEntry> commonTextFields;
    try {
      commonTextFields = suggester.getMostCommonTextFields(user);
      if (commonTextFields.size() > 0) {
        String query = "in:" + commonTextFields.get(0).getUniqueName() + " " + input;
        return String.format(UNKNOWN_CATALOG_ENTRY, input, query);
      }
    } catch (ConfigException e) {
      e.printStackTrace();
    }
    return "Unknown Entry '" + input + "'";
  }

  @Override
  public String visitNegation(QuickSearchParser.NegationContext ctx) {
    // return "-" + NO_WITHE_SPACE;
    return "-";
  }

  @Override
  public String visitIn(QuickSearchParser.InContext ctx) {
    return "in:" + NO_WITHE_SPACE;
  }

  @Override
  public String visitInput(QuickSearchParser.InputContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public String visitPlus(QuickSearchParser.PlusContext ctx) {
    return "+";
  }

  @Override
  public String visitReductionOperator(QuickSearchParser.ReductionOperatorContext ctx) {
    return ctx.getText().toUpperCase();
  }

  @Override
  public String visitClosedInterval(QuickSearchParser.ClosedIntervalContext ctx) {
    return getChildsText(ctx, "");
  }

  private String getChildsText(ParserRuleContext ctx, String delimiter) {
    List<String> texts = new ArrayList<>();
    for (int i = 0; i <= ctx.getChildCount() - 1; i++) {
      texts.add(ctx.getChild(i).getText());
    }
    return texts.stream().collect(Collectors.joining(delimiter));
  }

  @Override
  public String visitQueryTokens(QuickSearchParser.QueryTokensContext ctx) {
    int start = ctx.getStart().getStartIndex();
    int stop = ctx.getStop().getStopIndex() + 1;
    if (stop > start)
      return input.substring(start, stop);
    else
      return "";
  }

  @Override
  public String visitManyIntervals(QuickSearchParser.ManyIntervalsContext ctx) {
    return getChildsText(ctx, "");
  }

  @Override
  public String visitOpenInterval(QuickSearchParser.OpenIntervalContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public String visitLowerBound(QuickSearchParser.LowerBoundContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public String visitUpperBound(QuickSearchParser.UpperBoundContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public String visitNumericAttribute(QuickSearchParser.NumericAttributeContext ctx) {
    String result = visitChildren(ctx);

    if (lastEntry != null) {
      CatalogEntryType type = lastEntry.getDataType();
      if (!isNumberOrDateAttribute(type)) {
        String uniquiName = QuickSearchUtil.formatCatalogEntryWithUniquiNameNotation(lastEntry);
        String typeString = type.getDisplayString();
        SyntaxError error = new SyntaxError(ctx.getStart().getStartIndex(),
                String.format(WRONG_CATALOG_ENTRY_FOR_NUMERIC_OPERATOR, uniquiName, typeString));
        errors.add(error);
      }
    }
    return result;
  }

  @Override
  public String visitDots(QuickSearchParser.DotsContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public String visitBoolAttribute(QuickSearchParser.BoolAttributeContext ctx) {
    String result = visitChildren(ctx);
    return result;
  }

  @Override
  public String visitBound(QuickSearchParser.BoundContext ctx) {
    return ctx.getText();
  }

  private boolean isNumberOrDateAttribute(CatalogEntryType type) {
    return type == CatalogEntryType.Number || type == CatalogEntryType.DateTime;
  }

  @Override
  public String visitNumericOperator(QuickSearchParser.NumericOperatorContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public String visitConjunction(QuickSearchParser.ConjunctionContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public String visitQuery(QuickSearchParser.QueryContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public String visitTextAttribute(QuickSearchParser.TextAttributeContext ctx) {
    String result = visitChildren(ctx);

    if (lastEntry != null) {
      CatalogEntryType type = lastEntry.getDataType();
      if (isNumberOrDateAttribute(type)) {
        String uniquiName = QuickSearchUtil.formatCatalogEntryWithUniquiNameNotation(lastEntry);
        String typeString = type.getDisplayString();
        SyntaxError error = new SyntaxError(ctx.getStart().getStartIndex(),
                String.format(WRONG_CATALOG_ENTRY_FOR_TEXT_OPERATOR, uniquiName, typeString));
        errors.add(error);
      }
    }
    return result;
  }

  public List<SyntaxError> getErrors() {
    return errors;
  }

  @Override
  public String visitSucessor(QuickSearchParser.SucessorContext ctx) {
    return "Nachfolger";
  }

  @Override
  public String visitNumber(QuickSearchParser.NumberContext ctx) {
    return getChildsText(ctx, "");
  }

  @Override
  public String visitYearPeriod(QuickSearchParser.YearPeriodContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public String visitGroupExpression(QuickSearchParser.GroupExpressionContext ctx) {
    String childText = ctx.getChild(1).accept(this);
    return "{" + childText + "}";
  }

  @Override
  public String visitTerminal(TerminalNode node) {
    return node.getText();
  }

}
