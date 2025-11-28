package de.uniwue.dw.query.model.quickSearch;

public class AttributeDuringTipping {

  private String catalogEntryText = "";

  private String queryTokens = "";

  private String in = "";

  private String reductionOperator = "";

  private String plus = "";

  private String lowerBound = "";

  private String upperBound = "";

  private String bound = "";

  private String numericOperator = "";

  private String unparsedInput = "";

  private String dots = "";

  private String negation="";

  public void setCatalogEntryText(String text) {
    catalogEntryText = text;

  }

  public void setQueryTokens(String queryTokens) {
    this.queryTokens = queryTokens;
  }

  public void setIn(String text) {
    this.in = text;
  }

  public void setReductionOperator(String text) {
    reductionOperator = text;
  }

  public void setPlus(String text) {
    this.plus = text;
  }

  public void setLowerBound(String text) {
    this.lowerBound = text;
  }

  public void setUpperBound(String text) {
    this.upperBound = text;
  }

  public void setBound(String text) {
    bound = text;
  }

  public void setNumericOperator(String text) {
    this.numericOperator = text;
  }

  public void setUnparsedInput(String unparsedInput) {
    this.unparsedInput = unparsedInput;
  }

  public void setDots(String text) {
    this.dots = text;
  }

  public void setNegation(String text) {
    this.negation = text;
  }

  public String getCatalogEntryText() {
    return catalogEntryText;
  }

  public String getQueryTokens() {
    return queryTokens;
  }

  public String getIn() {
    return in;
  }

  public String getReductionOperator() {
    return reductionOperator;
  }

  public String getPlus() {
    return plus;
  }

  public String getLowerBound() {
    return lowerBound;
  }

  public String getUpperBound() {
    return upperBound;
  }

  public String getBound() {
    return bound;
  }

  public String getNumericOperator() {
    return numericOperator;
  }

  public String getUnparsedInput() {
    return unparsedInput;
  }

  public String getDots() {
    return dots;
  }
  

  public String getNegation() {
    return negation;
  }

  public boolean hasCatalogEntryText() {
    return !catalogEntryText.trim().isEmpty();
  }

  public boolean hasQueryTokens() {
    return !queryTokens.trim().isEmpty();
  }

  public boolean hasIn() {
    return !in.trim().isEmpty();
  }

  public boolean hasReductionOperator() {
    return !reductionOperator.trim().isEmpty();
  }

  public boolean hasPlus() {
    return !plus.trim().isEmpty();
  }

  public boolean hasLowerBound() {
    return !lowerBound.trim().isEmpty();
  }

  public boolean hasUpperBound() {
    return !upperBound.trim().isEmpty();
  }

  public boolean hasBound() {
    return !bound.trim().isEmpty();
  }

  public boolean hasNumericOperator() {
    return !numericOperator.trim().isEmpty();
  }

  public boolean hasUnparsedInput() {
    return !unparsedInput.trim().isEmpty();
  }

  public boolean hasDots() {
    return !dots.trim().isEmpty();
  }

  public boolean hasNegation(){
    return !negation.trim().isEmpty();
  }
  
  public void print() {
    p("in", in);
    p("catalog", catalogEntryText);
    p("plus", plus);
    p("reduction", reductionOperator);
    p("lowerBound", lowerBound);
    p("dots", dots);
    p("upperBound", upperBound);
    p("numericOperator", numericOperator);
    p("bound", bound);
    p("queryTokens", queryTokens);
    p("unparsedInput", unparsedInput);
  }

  private static void p(String string, Object o) {
    if (o instanceof String)
      System.out.println(string + ": '" + o + "'");
    else
      System.out.println(string + ": " + o);
  }

  public boolean hasOperatorsOrArguments() {
    return hasQueryTokens() || hasLowerBound() || hasNumericOperator() || hasReductionOperator();
  }

}
