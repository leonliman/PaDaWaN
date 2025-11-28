package de.uniwue.dw.query.model.lang;

public enum ReductionOperator {

  //@formatter:off
  MAX("max. Wert", "MAX"), 
  MIN("min. Wert", "MIN"), 
  EARLIEST("erster Wert", "FIRST"), 
  LATEST("letzter Wert", "LAST"), 
  NONE("alle Werte", ""),
  ANY("irgendeinen", "");
  //@formatter:on

  public static final ReductionOperator[] ALL_VALUES = { MAX, MIN, EARLIEST, LATEST, NONE, ANY };

  public static final ReductionOperator[] TEMOPORAL_VALUES = { EARLIEST, LATEST, NONE };

  private String text;
  private String quickSearchText;

  private ReductionOperator(String text,String quickSearchText) {
    this.text = text;
    this.quickSearchText=quickSearchText;
  }

  public String getDisplayString() {
    return text;
  }

  public static ReductionOperator parse(String s) {
    s = s.trim();
    for (ReductionOperator op : ReductionOperator.values()) {
      if (s.equalsIgnoreCase(op.toString()) || s.equalsIgnoreCase(op.getDisplayString())|| s.equalsIgnoreCase(op.getQuickSearchText())) {
        return op;
      }
    }
    throw new IllegalArgumentException("Operator unknown for input: " + s);
  }
  
  public String getQuickSearchText() {
    return quickSearchText;
  }

  public static void main(String[] args) {
    System.out.println(MAX);
  }
}
