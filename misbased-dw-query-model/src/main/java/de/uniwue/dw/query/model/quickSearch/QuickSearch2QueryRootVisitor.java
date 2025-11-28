package de.uniwue.dw.query.model.quickSearch;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.hooks.IDwCatalogHooks;
import de.uniwue.dw.core.model.manager.DataSourceException;
import de.uniwue.dw.core.model.manager.ICatalogAndTextSuggester;
import de.uniwue.dw.query.model.data.StoredQueryTreeEntry;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.ContentOperator;
import de.uniwue.dw.query.model.lang.QueryAnd;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryIDFilter;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;
import de.uniwue.dw.query.model.lang.QueryOr;
import de.uniwue.dw.query.model.lang.QueryStructureContainingElem;
import de.uniwue.dw.query.model.lang.QueryStructureElem;
import de.uniwue.dw.query.model.lang.QuerySubQuery;
import de.uniwue.dw.query.model.lang.ReductionOperator;
import de.uniwue.dw.query.model.manager.IQueryClientIOManager;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchBaseVisitor;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser.BoolAttributeContext;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser.ClosedIntervalContext;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser.DotsContext;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser.LowerBoundContext;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser.ManyIntervalsContext;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser.NegationContext;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser.NumberContext;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser.NumberOrDateContext;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser.NumericAttributeContext;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser.NumericConditionContext;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser.OpenIntervalContext;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser.PlusContext;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser.QueryTokensContext;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser.ReductionOperatorContext;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser.SucessorContext;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser.TextAttributeContext;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser.UpperBoundContext;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser.YearPeriodContext;

public class QuickSearch2QueryRootVisitor extends QuickSearchBaseVisitor<QueryStructureElem>
        implements IDwCatalogHooks {

  private static Logger logger = LogManager.getLogger(QuickSearch2QueryRootVisitor.class);

  private String input;

  private User user;

  private ICatalogAndTextSuggester suggester;

  private IQueryClientIOManager queryClientIOManager;

  public QuickSearch2QueryRootVisitor(String input, User user, ICatalogAndTextSuggester suggester,
          IQueryClientIOManager queryClientIOManager) {
    this.input = input;
    this.user = user;
    this.suggester = suggester;
    this.queryClientIOManager = queryClientIOManager;
  }

  @Override
  protected QueryStructureElem aggregateResult(QueryStructureElem aggregate,
          QueryStructureElem nextResult) {
    if (aggregate == null && nextResult == null)
      return null;
    if (aggregate != null && nextResult == null)
      return aggregate;
    else if (aggregate == null && nextResult != null)
      return nextResult;
    else {
      QueryStructureContainingElem list = new QueryAnd(null);
      return addToListElem(list, aggregate, nextResult);
    }
  }

  private static QueryStructureContainingElem addToListElem(QueryStructureContainingElem list,
          QueryStructureElem... childs) {
    for (QueryStructureElem child : childs) {
      if (child != null) {
        try {
          list.addChild(child);
          child.setParent(list);
        } catch (QueryException e) {
          e.printStackTrace();
        }
      }
    }
    return list;
  }

  @Override
  public QueryStructureElem visitBooleanOr(QuickSearchParser.BooleanOrContext ctx) {
    QueryStructureContainingElem list = new QueryOr(null);
    QueryStructureElem first = ctx.getChild(0).accept(this);
    QueryStructureElem second = ctx.getChild(2).accept(this);
    return addToListElem(list, first, second);
  }

  @Override
  public QueryStructureElem visitBooleanAnd(QuickSearchParser.BooleanAndContext ctx) {
    QueryStructureContainingElem list = new QueryAnd(null);
    QueryStructureElem first = ctx.getChild(0).accept(this);
    QueryStructureElem second = ctx.getChild(2).accept(this);
    return addToListElem(list, first, second);
  }

  @Override
  public QueryStructureElem visitGroupExpression(QuickSearchParser.GroupExpressionContext ctx) {
    QueryIDFilter group = new QueryIDFilter(null);
    group.setFilterIDType(FilterIDType.GROUP);
    int childCount = ctx.getChildCount();
    // for (int i=0;i<childCount;i++) {
    // ParseTree parseTree = ctx.getChild(i);
    // QueryStructureElem elem = parseTree.accept(this);
    // System.out.println(elem);
    // }
    QueryStructureElem child = ctx.getChild(1).accept(this);
    return addToListElem(group, child);
  }

  @Override
  public QueryStructureElem visitParenExpression(QuickSearchParser.ParenExpressionContext ctx) {
    return visitChildren(ctx);
  }

  // @Override
  // public QueryStructureElem visitQueryAttribute1(QuickSearchParser.QueryAttribute1Context ctx) {
  // return visitChildren(ctx);
  // }

  @Override
  public QueryStructureElem visitQueryAttribute(QuickSearchParser.QueryAttributeContext ctx) {
    ParseTree negationContext = getFirstType(ctx.children, NegationContext.class);
    ParseTree boolAttribute = getFirstType(ctx.children, BoolAttributeContext.class);
    ParseTree numericAttribute = getFirstType(ctx.children, NumericAttributeContext.class);
    ParseTree textAttribute = getFirstType(ctx.children, TextAttributeContext.class);

    boolean negation = negationContext != null;

    QueryStructureElem attribute = null;
    try {
      if (boolAttribute != null)
        attribute = createBoolOrQuerySubAttribute(boolAttribute, negation);
      else if (numericAttribute != null) {
        attribute = createNumericAttribute(numericAttribute, negation);
      } else {
        attribute = createTextAttribute(textAttribute, negation);
      }
    } catch (DataSourceException e) {
      logger.error(e);
    } catch (QueryException e) {
      logger.error(e);
    }
    return attribute;
  }

  public static boolean isStoredQueryElem(String uniqueName) {
    return (uniqueName.startsWith(ModelConverter.STORED_QUERY_PREFIX));
  }

  public static Optional<StoredQueryTreeEntry> getStoredQueryElem(String uniqueName, User user,
          IQueryClientIOManager queryClientIOManager) {
    if (uniqueName.startsWith(ModelConverter.STORED_QUERY_PREFIX)) {
      String storedQueyPath = getStoredQueryName(uniqueName);
      Optional<StoredQueryTreeEntry> storedQueryOp = queryClientIOManager
              .getStoredQueryForUser(storedQueyPath, user);
      return storedQueryOp;
    }
    return Optional.empty();
  }

  public static String getStoredQueryName(String uniqueName) {
    String storedQueyPath = uniqueName.substring(ModelConverter.STORED_QUERY_PREFIX.length());
    return storedQueyPath;
  }

  private QueryStructureElem createBoolOrQuerySubAttribute(ParseTree ctx, boolean negation)
          throws DataSourceException, QueryException {
    String uniqueName = ctx.getChild(0).getText();
    if (isStoredQueryElem(uniqueName)) {
      String storedQueyPath = uniqueName.substring(ModelConverter.STORED_QUERY_PREFIX.length());
      Optional<StoredQueryTreeEntry> storedQueryOp = getStoredQueryElem(uniqueName, user,
              queryClientIOManager);
      StoredQueryTreeEntry storedQuery = storedQueryOp.orElseThrow(() -> new DataSourceException(
              "No stored query for user. storedQuery: " + storedQueyPath + " user: " + user));
      return new QuerySubQuery(null, storedQuery.getPath(), -1);
    } else
      return createBoolAttribute(ctx, negation);
  }

  private QueryAttribute createBoolAttribute(ParseTree ctx, boolean negation)
          throws DataSourceException, QueryException {
    String uniqueName = ctx.getChild(0).getText();
    CatalogEntry entry = null;
    entry = suggester.getCatalogEntryByUniqueName(uniqueName, user);
    try {
      entry = DwClientConfiguration.getInstance().getCatalogManager()
              .getEntryByRefID(entry.getExtID(), entry.getProject());
    } catch (SQLException e) {
      e.printStackTrace();
    }
    ContentOperator operator = negation ? ContentOperator.NOT_EXISTS : ContentOperator.EXISTS;
    QueryAttribute attribute = new QueryAttribute(entry, operator, null);
    ParseTree reductionOperatorParseTree = getFirstTypeOfChilds(ctx,
            ReductionOperatorContext.class);
    if (reductionOperatorParseTree != null) {
      ReductionOperator reductionOperator = ReductionOperator
              .parse(reductionOperatorParseTree.getText());
      attribute.setReductionOperator(reductionOperator);
    }
    ParseTree sucessor = getFirstTypeOfChilds(ctx, SucessorContext.class);
    if (sucessor != null)
      attribute.setDesiredContent(QuickSearchAPI.SUCCCESORS);
    return attribute;
  }

  private QueryAttribute createNumericAttribute(ParseTree ctx, boolean negation)
          throws DataSourceException, QueryException {
    QueryAttribute attribute = createBoolAttribute(ctx, negation);
    ParseTree reductionOperatorParseTree = getFirstTypeOfChilds(ctx,
            ReductionOperatorContext.class);
    if (reductionOperatorParseTree != null) {
      ReductionOperator reductionOperator = ReductionOperator
              .parse(reductionOperatorParseTree.getText());
      attribute.setReductionOperator(reductionOperator);
    }
    ParseTree numericCondition = getFirstTypeOfChilds(ctx, NumericConditionContext.class);
    setOperatorAndArgument(attribute, numericCondition);
    return attribute;
  }

  private QueryAttribute createTextAttribute(ParseTree ctx, boolean negation)
          throws QueryException {
    String uniqueName = ctx.getChild(1).getText();
    CatalogEntry entry = null;
    try {
      entry = suggester.getCatalogEntryByUniqueName(uniqueName, user);
    } catch (DataSourceException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    ParseTree plusTree = getFirstTypeOfChilds(ctx, PlusContext.class);
    ParseTree queryTokensTree = getFirstTypeOfChilds(ctx, QueryTokensContext.class);
    boolean plus = plusTree == null ? false : true;
    String argument = "";
    if (queryTokensTree != null && queryTokensTree instanceof ParserRuleContext) {
      ParserRuleContext a = (ParserRuleContext) queryTokensTree;
      int start = a.getStart().getStartIndex();
      int stop = a.getStop().getStopIndex() + 1;
      argument = input.substring(start, stop);
      if (argument.startsWith("'") && argument.endsWith("'"))
        argument = argument.substring(1, argument.length() - 1);
    }
    ContentOperator operator = getTextOperator(negation, plus);
    QueryAttribute attribute = new QueryAttribute(entry, operator, argument);
    return attribute;
  }

  private static ContentOperator getTextOperator(boolean negation, boolean plus) {
    if (negation && plus)
      return ContentOperator.CONTAINS_NOT_POSITIVE;
    else if (negation && !plus)
      return ContentOperator.CONTAINS_NOT;
    else if (!negation && plus)
      return ContentOperator.CONTAINS_POSITIVE;
    else if (!negation && !plus)
      return ContentOperator.CONTAINS;
    return ContentOperator.EXISTS;
  }

  private ParseTree getFirstType(List<ParseTree> children, Class<? extends ParseTree> targetClass) {
    for (ParseTree child : children) {
      if (targetClass.isInstance(child)) {
        return child;
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

  private List<ParseTree> getAllTypeOfChilds(ParseTree ctx) {
    // System.out.println(ctx.getText());
    List<ParseTree> result = new ArrayList<>();
    for (int i = 0; i <= ctx.getChildCount() - 1; i++) {
      ParseTree child = ctx.getChild(i);
      if (NumberContext.class.isInstance(child) || NumberOrDateContext.class.isInstance(child)
              || LowerBoundContext.class.isInstance(child)
              || UpperBoundContext.class.isInstance(child)) {
        result.add(child);
      }
    }
    return result;
  }

  // public CatalogEntry getDischargeLetterID(String domain) {
  // return suggester.getCatalogEntryByExtid(EXT_HOOK_ID_LETTER_COMPLETE_TEXT, PROJECT_HOOK_LETTER);
  // }

  private void setOperatorAndArgument(QueryAttribute attribute, ParseTree ctx) {
    if (ctx instanceof NumericConditionContext) {
      if (ctx instanceof ClosedIntervalContext) {
        attribute.setContentOperator(ContentOperator.BETWEEN);
        List<ParseTree> limits = getAllTypeOfChilds(ctx);
        String delimiter = QueryAttribute.PERIOD_DELIMITER;
        String argument = limits.stream().map(ParseTree::getText)
                .collect(Collectors.joining(delimiter));
        attribute.setDesiredContent(argument);
      } else if (ctx instanceof OpenIntervalContext) {
        ContentOperator contentOperator = ContentOperator.parse(ctx.getChild(0).getText());
        attribute.setContentOperator(contentOperator);
        attribute.setDesiredContent(ctx.getChild(1).getText());
      } else if (ctx instanceof ManyIntervalsContext) {
        boolean minusInfinityIsBoundary = DotsContext.class.isInstance(ctx.getChild(0));
        boolean plusInfinityIsBoundary = DotsContext.class
                .isInstance(ctx.getChild(ctx.getChildCount() - 1));
        List<ParseTree> limits = getAllTypeOfChilds(ctx);
        String delimiter = QueryAttribute.PERIOD_DELIMITER;
        String argument = limits.stream().map(ParseTree::getText)
                .collect(Collectors.joining(delimiter));
        if (minusInfinityIsBoundary)
          argument = QueryAttribute.PERIOD_DELIMITER + argument;
        if (plusInfinityIsBoundary)
          argument = argument + QueryAttribute.PERIOD_DELIMITER;
        attribute.setContentOperator(ContentOperator.PER_INTERVALS);
        attribute.setDesiredContent(argument);
      } else if (ctx instanceof YearPeriodContext) {
        attribute.setContentOperator(ContentOperator.PER_YEAR);
        List<ParseTree> limits = getAllTypeOfChilds(ctx);
        String delimiter = QueryAttribute.PERIOD_DELIMITER;
        String argument = limits.stream().map(ParseTree::getText)
                .collect(Collectors.joining(delimiter));
        attribute.setDesiredContent(argument);
      }
    }
  }

  public static String[] getCatalogEntryNameAndDomain(ParseTree ctx) {
    String name = ctx.getChild(0).getText();
    String domain = null;
    if (name.matches("'.+'"))
      name = name.substring(1, name.length() - 1);
    if (ctx.getChildCount() == 3)
      domain = ctx.getChild(2).getText();
    return new String[] { name, domain };

  }

  @Override
  public QueryStructureElem visitCatalogEntry(QuickSearchParser.CatalogEntryContext ctx) {
    return visitChildren(ctx);
  }

  //
  // @Override
  // public QueryStructureElem visitChildren(RuleNode node) {
  // QueryStructureElem result = defaultResult();
  // int n = node.getChildCount();
  // for (int i=0; i<n; i++) {
  // if (!shouldVisitNextChild(node, result)) {
  // break;
  // }
  //
  // ParseTree c = node.getChild(i);
  // QueryStructureElem childResult = c.accept(this);
  // result = aggregateResult(result, childResult);
  // }
  //
  // return result;
  // }
}
