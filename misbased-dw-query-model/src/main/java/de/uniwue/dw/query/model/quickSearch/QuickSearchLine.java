package de.uniwue.dw.query.model.quickSearch;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import de.uniwue.dw.query.model.lang.DisplayStringVisitor;
import de.uniwue.dw.query.model.quickSearch.suggest.SuggestObject;
import de.uniwue.dw.query.model.table.QueryTableEntry.Position;

public class QuickSearchLine {

  private String textType = "";

  private String queryText;

  // attribute for statistic tool
  private Position position;

  // attributes for patient search engine

  private boolean isDisplayInResult = true;

  private boolean isFilter = true;

  private boolean isFilterUnknown = true;

  private Integer docId;

  private Integer caseId;

  private List<SuggestObject> suggestAttributes = new ArrayList<SuggestObject>();

  public QuickSearchLine() {
  }

  public QuickSearchLine(String queryText, Position position) {
    this.queryText = DisplayStringVisitor.makeNice(queryText);
    this.position = position;
  }

  public QuickSearchLine(String queryText, boolean isFilter, boolean isDisplayInResult) {
    this.queryText = queryText;
    this.isFilter = isFilter;
    this.isDisplayInResult = isDisplayInResult;
  }

  public QuickSearchLine(String queryText, Position position, boolean isDisplayInResult,
          boolean isFilter, boolean isFilterUnknown, List<SuggestObject> suggestAttributes,
          Integer docId, Integer caseId) {
    this.queryText = queryText;
    this.position = position;
    this.isDisplayInResult = isDisplayInResult;
    this.isFilter = isFilter;
    this.isFilterUnknown = isFilterUnknown;
    this.suggestAttributes = suggestAttributes;
    this.docId = docId;
    this.caseId = caseId;
  }

  public Integer getCaseId() {
    return caseId;
  }

  public void setCaseId(Integer caseId) {
    this.caseId = caseId;
  }

  public Integer getDocId() {
    return docId;
  }

  public void setDocId(Integer docId) {
    this.docId = docId;
  }

  public List<SuggestObject> getSuggestAttributes() {
    return suggestAttributes;
  }

  public void setSuggestAttributes(List<SuggestObject> suggestAttributes) {
    this.suggestAttributes = suggestAttributes;
  }

  public String getTextType() {
    return textType;
  }

  public void setTextType(String textType) {
    this.textType = textType;
  }

  public Position getPosition() {
    return position;
  }

  public void setPosition(Position position) {
    this.position = position;
  }

  public String getQueryText() {
    return queryText;
  }

  public void setQueryText(String queryText) {
    this.queryText = queryText;
  }

  @JsonProperty("isFilterPatientCases")
  public boolean isFilter() {
    return isFilter;
  }

  @JsonProperty("isFilterPatientCases")
  public void setFilter(boolean isFilter) {
    this.isFilter = isFilter;
  }

  @JsonProperty("isUnknownFilter")
  public boolean isFilterUnknown() {
    return isFilterUnknown;
  }

  @JsonProperty("isUnknownFilter")
  public void setFilterUnknown(boolean isFilterUnknown) {
    this.isFilterUnknown = isFilterUnknown;
  }

  @JsonProperty("isShow")
  public boolean isDisplayInResult() {
    return isDisplayInResult;
  }

  @JsonProperty("isShow")
  public void setDisplayInResult(boolean isDisplayInResult) {
    this.isDisplayInResult = isDisplayInResult;
  }

  @JsonProperty("isFilter")
  public boolean isPositionFilter() {
    return position == Position.filter;
  }

  @JsonProperty("isFilter")
  public void setPositionFilter(boolean aValue) {
    position = Position.filter;
  }

  @JsonProperty("isRow")
  public boolean isPositionRow() {
    return position == Position.row;
  }

  @JsonProperty("isRow")
  public void setPositionRow(boolean aValue) {
    position = Position.row;
  }

  @JsonProperty("isColumn")
  public boolean isPositionColumn() {
    return position == Position.column;
  }

  @JsonProperty("isColumn")
  public void setPositionColumn(boolean aValue) {
    position = Position.column;
  }

  public String toString() {
    return position + " " + queryText;
  }

}
