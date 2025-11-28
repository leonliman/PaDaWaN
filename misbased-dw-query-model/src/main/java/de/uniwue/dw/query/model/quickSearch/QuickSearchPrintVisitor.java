package de.uniwue.dw.query.model.quickSearch;

import org.antlr.v4.runtime.ParserRuleContext;

import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchBaseVisitor;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser;

public class QuickSearchPrintVisitor extends QuickSearchBaseVisitor<Void> {

  private int indention = 0;

  /**
   * {@inheritDoc}
   *
   * <p>
   * The default implementation returns the result of calling {@link #visitChildren} on {@code ctx}.
   * </p>
   */

  @Override
  public Void visitAliasName(QuickSearchParser.AliasNameContext ctx) {
    return printNode(ctx);
  }

  @Override
  public Void visitAlias(QuickSearchParser.AliasContext ctx) {
    return printNode(ctx);
  }

  @Override
  public Void visitParse(QuickSearchParser.ParseContext ctx) {
    return printNode(ctx);
  }

  public Void visitGroupExpression(QuickSearchParser.GroupExpressionContext ctx) {
    return printNode(ctx);
  }

  @Override
  public Void visitBooleanOr(QuickSearchParser.BooleanOrContext ctx) {
    return printNode(ctx);
  }

  @Override
  public Void visitBooleanAnd(QuickSearchParser.BooleanAndContext ctx) {
    return printNode(ctx);
  }

  @Override
  public Void visitParenExpression(QuickSearchParser.ParenExpressionContext ctx) {
    return printNode(ctx);
  }

  @Override
  public Void visitQueryAttribute(QuickSearchParser.QueryAttributeContext ctx) {
    return printNode(ctx);
  }

  @Override
  public Void visitCatalogEntry(QuickSearchParser.CatalogEntryContext ctx) {
    return printNode(ctx);
  }

  @Override
  public Void visitClosedInterval(QuickSearchParser.ClosedIntervalContext ctx) {
    return printNode(ctx);
  }

  @Override
  public Void visitOpenInterval(QuickSearchParser.OpenIntervalContext ctx) {
    return printNode(ctx);
  }

  @Override
  public Void visitLowerBound(QuickSearchParser.LowerBoundContext ctx) {
    return printNode(ctx);
  }

  @Override
  public Void visitUpperBound(QuickSearchParser.UpperBoundContext ctx) {
    return printNode(ctx);
  }

  @Override
  public Void visitNumericOperator(QuickSearchParser.NumericOperatorContext ctx) {
    return printNode(ctx);
  }

  // @Override
  // public Void visitNumber(QuickSearchParser.NumberContext ctx) {
  // return printNode(ctx);
  // }

  private Void printNode(ParserRuleContext ctx) {
    indent();
    String className = ctx.getClass().getName().replace("Context", "");
    className = className.substring(className.indexOf('$') + 1);
    System.out.println(className + " " + ctx.getText());
    indention++;
    visitChildren(ctx);
    indention--;
    return null;
  }

  private void indent() {
    for (int i = 0; i < indention; i++)
      System.out.print("  ");
  }
}
