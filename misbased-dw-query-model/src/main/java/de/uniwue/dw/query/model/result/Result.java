package de.uniwue.dw.query.model.result;

import de.uniwue.misc.util.StringUtilsUniWue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Result {

  private List<String> header = new ArrayList<String>();

  private ArrayList<Row> rows = new ArrayList<>();

  private boolean isExcelExport = false;

  private long docsFound;

  private long queryTime;

  private String[][] correlationWithPValues;

  public Result() {
  }

  public void addColumn(String columnName) {
    header.add(columnName);
  }

  public Row createNewRow() {
    Row row = new Row(this, rows.size());
    rows.add(row);
    return row;
  }

  public Row addRow(Row row) {
    row.setResult(this);
    row.setRowNumber(rows.size());
    rows.add(row);
    return row;
  }

  public List<String> getHeader() {
    return header;
  }

  public void setHeader(List<String> header) {
    this.header = header;
  }

  public List<Row> getRows() {
    return rows;
  }

  public Row getRow(int index) {
    return rows.get(index);
  }

  public Cell getCell(int row, int column) {
    return getRow(row).getCell(column);
  }

  public boolean isExcelExoport() {
    return isExcelExport;
  }

  public void setDocsFound(long docsFound) {
    this.docsFound = docsFound;
  }

  public void setQueryTime(long timeTakenForQuery) {
    this.queryTime = timeTakenForQuery;
  }

  public long getDocsFound() {
    return docsFound;
  }

  public long getQueryTime() {
    return queryTime;
  }

  public String[][] getCorrelationWithPValues() {
    return correlationWithPValues;
  }

  public void setCorrelationWithPValues(String[][] correlationWithPValues) {
    this.correlationWithPValues = correlationWithPValues;
  }

  public void removeRow(Row row) {
    rows.remove(row);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(StringUtilsUniWue.concat(getHeader(), ",") + "\n");
    sb.append(rows.stream().map(Row::toString).collect(Collectors.joining("\n")));
    return sb.toString();
  }

}
