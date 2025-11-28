package de.uniwue.dw.query.model.quickSearch;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryQuickSearchRepresentation {

  private List<QuickSearchLine> lines = new ArrayList<QuickSearchLine>();

  private QuickSearchPeriod period;

  private boolean isPatientQuery;

  private boolean isDistributionQuery;

  private int previewRows;

  @JsonProperty("searchLines")
  public List<QuickSearchLine> getLines() {
    return lines;
  }

  @JsonProperty("searchLines")
  public void setLines(List<QuickSearchLine> lines) {
    this.lines = lines;
  }

  public QuickSearchPeriod getPeriod() {
    return period;
  }

  public void setPeriod(QuickSearchPeriod period) {
    this.period = period;
  }

  @JsonProperty("isCaseQuery")
  public boolean isCaseQuery(){
    return !isPatientQuery;
  }
  
  @JsonProperty("isCaseQuery")
  public void setIsCaseQuery(boolean aValue){
    isPatientQuery = false;
  }
  
  public boolean isPatientQuery() {
    return isPatientQuery;
  }

  public void setPatientQuery(boolean isPatientQuery) {
    this.isPatientQuery = isPatientQuery;
  }

  @JsonProperty("isDistribution")
  public boolean isDistributionQuery() {
    return isDistributionQuery;
  }

  @JsonProperty("isDistribution")
  public void setDistributionQuery(boolean isDistributionQuery) {
    this.isDistributionQuery = isDistributionQuery;
  }

  public void addQueryLine(QuickSearchLine line) {
    this.lines.add(line);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("---QueryQuickSearchRepresentation---\n");
    sb.append("IsPatientQuery: " + isPatientQuery + " isDistributionQuey: " + isDistributionQuery
            + "\n");
    sb.append("Query Lines:\n");
    for (QuickSearchLine line : lines) {
      sb.append(line + "\n");
    }
    sb.append(period + "\n");
    sb.append("-----------------------------------");
    return sb.toString();
  }

  public void setPreviewRows(int previewRows) {
    this.previewRows = previewRows;
  }

  public int getPreviewRows() {
    return previewRows;
  }

}
