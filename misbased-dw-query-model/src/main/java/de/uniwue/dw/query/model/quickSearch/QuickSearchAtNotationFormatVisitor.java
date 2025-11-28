package de.uniwue.dw.query.model.quickSearch;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.ParserRuleContext;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.hooks.IDwCatalogHooks;
import de.uniwue.dw.core.model.manager.DataSourceException;
import de.uniwue.dw.core.model.manager.ICatalogAndTextSuggester;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchBaseVisitor;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser;

public class QuickSearchAtNotationFormatVisitor extends QuickSearchBaseVisitor<String> {

  private User user;

  private ICatalogAndTextSuggester suggester;

  public QuickSearchAtNotationFormatVisitor(User user, ICatalogAndTextSuggester suggester) {
    this.user = user;
    this.suggester = suggester;
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
  public String visitParse(QuickSearchParser.ParseContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public String visitQueryAttribute1(QuickSearchParser.QueryAttribute1Context ctx) {
    return visitChildren(ctx);
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
    return visitChildren(ctx);
  }

  @Override
  public String visitCatalogEntry(QuickSearchParser.CatalogEntryContext ctx) {
    String[] nameAndDomain = QuickSearch2QueryRootVisitor.getCatalogEntryNameAndDomain(ctx);
    String name = nameAndDomain[0];
    String domain = nameAndDomain[1];
    CatalogEntry catalogEntry=null;
    try {
      catalogEntry = suggester.getCatalogEntryByName(name, domain, user);
    } catch (DataSourceException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    if (catalogEntry == null) {
      // Brieftextsuche
      domain = IDwCatalogHooks.PROJECT_HOOK_LETTER;
    } else if (domain == null) {
      domain = catalogEntry.getProject();
    }
    name = QuickSearchUtil.formatCatalogEntryName(name);
    return name + "@" + domain;
  }

  @Override
  public String visitNegation(QuickSearchParser.NegationContext ctx) {
    return "-";
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
  public String visitOpenInterval(QuickSearchParser.OpenIntervalContext ctx) {
    return getChildsText(ctx, " ");
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
  public String visitNumericOperator(QuickSearchParser.NumericOperatorContext ctx) {
    return visitChildren(ctx);
  }

//  @Override
//  public String visitNumber(QuickSearchParser.NumberContext ctx) {
//    return visitChildren(ctx);
//  }
}
