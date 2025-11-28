package de.uniwue.dw.query.model.quickSearch;

import com.fasterxml.jackson.annotation.JsonProperty;

import de.uniwue.dw.query.model.lang.ContentOperator;
import de.uniwue.dw.query.model.table.QueryTableEntry.Position;

public class QuickSearchPeriod {

  private String from, to;

  private ContentOperator unit;

  private boolean isColumn;

  public QuickSearchPeriod() {
  }

  public QuickSearchPeriod(String from, String to, ContentOperator unit, boolean isColumn) {
    this.from = from;
    this.to = to;
    this.unit = unit;
    this.isColumn = isColumn;
  }

  public String getFrom() {
    return from;
  }

  public void setFrom(String from) {
    this.from = from;
  }

  public String getTo() {
    return to;
  }

  public void setTo(String to) {
    this.to = to;
  }

  public ContentOperator getUnit() {
    return unit;
  }

  public void setUnit(ContentOperator unit) {
    this.unit = unit;
  }

  public boolean isColumn() {
    return isColumn;
  }

  @JsonProperty("position")
  public Position getPosition() {
    if (isColumn)
      return Position.column;
    else
      return Position.row;
  }

  @JsonProperty("position")
  public void setPosition(Position aPosition) {
    if (aPosition == Position.column)
      isColumn = true;
    else
      isColumn = false;
  }

  public void setColumn(boolean isColumn) {
    this.isColumn = isColumn;
  }

  public String toString() {
    return "Period: " + from + " - " + to + " " + unit + " isColumn: " + isColumn;
  }

}
