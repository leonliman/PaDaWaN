package de.uniwue.dw.query.model.lang;

public enum ContentOperator {

  //@formatter:off
  EMPTY("egal (nur anzeigen)"),
  EQUALS("="), 
  LESS("<"),
  LESS_OR_EQUAL("<="),
  MORE(">"), 
  MORE_OR_EQUAL(">="), 
  BETWEEN("von.. bis.."), 
  CONTAINS("enthält Wörter"), 
  CONTAINS_NOT("enthält nicht Wörter",true), 
  CONTAINS_POSITIVE("enthält Befunde"), 
  CONTAINS_NOT_POSITIVE("enthält nicht Befunde",true),
  PER_YEAR("nach Jahren"), 
  PER_MONTH("Monate des Jahres"), 
  PER_INTERVALS("Intervallgrenzen"), 
  NOT_EXISTS( "nicht vorhanden",true), 
  EXISTS("vorhanden"); 
  //@formatter:on

  private String displayString;
  
  private boolean isNegated=false;

  ContentOperator(String operator) {
   this(operator,false);
  }

  ContentOperator(String operator,boolean isNegated) {
    this.displayString = operator;
    this.isNegated=isNegated;
  }
  public String getDisplayString() {
    return displayString;
  }

  public static ContentOperator parse(String s) {
    for (ContentOperator op : ContentOperator.values()) {
      if (s.equalsIgnoreCase(op.toString()) || s.equalsIgnoreCase(op.getDisplayString()))
        return op;
    }
    throw new IllegalArgumentException("Operator unknown for arguement: " + s);
  }

  public boolean isQueryWordOperator() {
    return this == ContentOperator.CONTAINS || this == ContentOperator.CONTAINS_NOT
            || this == ContentOperator.CONTAINS_POSITIVE
            || this == ContentOperator.CONTAINS_NOT_POSITIVE;
  }

  public boolean isNegated() {
   return this.isNegated;
  }

}
