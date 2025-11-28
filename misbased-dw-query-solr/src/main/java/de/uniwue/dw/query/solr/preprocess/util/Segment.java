package de.uniwue.dw.query.solr.preprocess.util;

public class Segment {

  private int start, end;

  private String text;

  private NegationType negationType = NegationType.NOT_NEGATED;

  public Segment(String semgentText, int start, NegationType negationType) {
    this.start = start;
    this.text = semgentText;
    this.end = start + semgentText.length();
    this.negationType = negationType;
  }

  public int getStart() {
    return start;
  }

  public void setStart(int start) {
    this.start = start;
  }

  public int getEnd() {
    return end;
  }

  public void setEnd(int end) {
    this.end = end;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  public NegationType getNegationType() {
    return negationType;
  }

  public void setNegationType(NegationType negationType) {
    this.negationType = negationType;
  }

  public boolean isNegated() {
    return negationType == NegationType.NEGATED;
  }

  public boolean isNotNegated() {
    return negationType == NegationType.NOT_NEGATED;
  }

}
