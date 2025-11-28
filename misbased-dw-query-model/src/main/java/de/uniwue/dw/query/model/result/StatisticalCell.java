package de.uniwue.dw.query.model.result;

import de.uniwue.dw.query.model.lang.QueryRoot;

public class StatisticalCell extends Cell {

  public StatisticalQueryType resultType;

  public QueryRoot queryRoot;

  public enum StatisticalQueryType {
    SUM, QUERY_ROOT, SUM_MINUS_CASE_UNION, COUNT
  }

  public StatisticalCell(Row row, int columnNumber) {
    super(row, columnNumber);
    cellType = CellType.Statistical;
  }

}
