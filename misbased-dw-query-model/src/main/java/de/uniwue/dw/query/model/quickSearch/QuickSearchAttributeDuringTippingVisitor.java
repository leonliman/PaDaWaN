package de.uniwue.dw.query.model.quickSearch;

import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchBaseVisitor;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser;

public class QuickSearchAttributeDuringTippingVisitor
        extends QuickSearchBaseVisitor<AttributeDuringTipping> {

  private String input;

  private AttributeDuringTipping attribute = new AttributeDuringTipping();

  public QuickSearchAttributeDuringTippingVisitor(String input) {
    this.input = input;
  }

  public QuickSearchAttributeDuringTippingVisitor(String input, String unparsableText) {
    this.input = input;
    this.attribute.setUnparsedInput(unparsableText);
  }

  @Override
  public AttributeDuringTipping visitBoolAttribute(QuickSearchParser.BoolAttributeContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public AttributeDuringTipping visitDots(QuickSearchParser.DotsContext ctx) {
    this.attribute.setDots(ctx.getText());
    return visitChildren(ctx);
  }

  @Override
  public AttributeDuringTipping visitNumericAttribute(
          QuickSearchParser.NumericAttributeContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public AttributeDuringTipping visitTextAttribute(QuickSearchParser.TextAttributeContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public AttributeDuringTipping visitCatalogEntry(QuickSearchParser.CatalogEntryContext ctx) {
    this.attribute.setCatalogEntryText(ctx.getText());
    return visitChildren(ctx);
  }

  @Override
  public AttributeDuringTipping visitQueryTokens(QuickSearchParser.QueryTokensContext ctx) {
    int start = ctx.getStart().getStartIndex();
    int stop = ctx.getStop().getStopIndex() + 1;
    // System.out.println(start+" "+stop);
    String queryTokens = input.substring(start, stop);
    this.attribute.setQueryTokens(queryTokens);
    return visitChildren(ctx);
  }

  @Override
  public AttributeDuringTipping visitClosedInterval(QuickSearchParser.ClosedIntervalContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public AttributeDuringTipping visitOpenInterval(QuickSearchParser.OpenIntervalContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public AttributeDuringTipping visitNegation(QuickSearchParser.NegationContext ctx) {
    this.attribute.setNegation(ctx.getText());
    return visitChildren(ctx);
  }

  @Override
  public AttributeDuringTipping visitReductionOperator(
          QuickSearchParser.ReductionOperatorContext ctx) {
    this.attribute.setReductionOperator(ctx.getText());
    return visitChildren(ctx);
  }

  @Override
  public AttributeDuringTipping visitPlus(QuickSearchParser.PlusContext ctx) {
    this.attribute.setPlus(ctx.getText());
    return visitChildren(ctx);
  }

  @Override
  public AttributeDuringTipping visitIn(QuickSearchParser.InContext ctx) {
    this.attribute.setIn(ctx.getText());
    return visitChildren(ctx);
  }

  @Override
  public AttributeDuringTipping visitLowerBound(QuickSearchParser.LowerBoundContext ctx) {
    this.attribute.setLowerBound(ctx.getText());
    return visitChildren(ctx);
  }

  @Override
  public AttributeDuringTipping visitUpperBound(QuickSearchParser.UpperBoundContext ctx) {
    this.attribute.setUpperBound(ctx.getText());
    return visitChildren(ctx);
  }

  @Override
  public AttributeDuringTipping visitBound(QuickSearchParser.BoundContext ctx) {
    this.attribute.setBound(ctx.getText());
    return visitChildren(ctx);
  }

  @Override
  public AttributeDuringTipping visitNumericOperator(QuickSearchParser.NumericOperatorContext ctx) {
    this.attribute.setNumericOperator(ctx.getText());
    return visitChildren(ctx);
  }

  @Override
  public AttributeDuringTipping visitText(QuickSearchParser.TextContext ctx) {
    int start = ctx.getStart().getStartIndex();
    int stop = ctx.getStop().getStopIndex() + 1;
    String unparsedInput = input.substring(start, stop);
    this.attribute.setUnparsedInput(unparsedInput);
    return visitChildren(ctx);
  }

  public AttributeDuringTipping getAttributeDuringTipping() {
    return attribute;
  }


}
