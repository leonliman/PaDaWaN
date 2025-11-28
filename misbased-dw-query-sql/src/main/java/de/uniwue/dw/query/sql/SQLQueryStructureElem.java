package de.uniwue.dw.query.sql;

import java.util.List;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryIDFilter;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;
import de.uniwue.dw.query.model.lang.QueryStructureElem;
import de.uniwue.dw.query.sql.util.QuerySQLGenHelper;

public abstract class SQLQueryStructureElem extends SQLQueryElem {

  public SQLQueryStructureElem(QueryStructureElem anElem) {
    super(anElem);
  }

  @Override
  public QueryStructureElem getMyQueryElem() {
    return (QueryStructureElem) queryElem;
  }

  public void createColumnsForOuterSelect(QuerySQLGenHelper genHelper, boolean createTable,
          boolean finalSelect) {
  }

  // adds all columns that this elements or all containing elements need for the result table
  // (value, time, etc.)
  public void createColumnsForInnerSelect(QuerySQLGenHelper genHelper) {
  }

  public void generateSQLQueryTable(QuerySQLGenHelper genHelper, String tableSuffix)
          throws QueryException {
    StringBuilder builder = genHelper.sqlQuery;
    builder.append("(");
    generateSQL(genHelper);
    String tableName = genHelper.getTableName(this);
    if ((tableSuffix != null) && !tableSuffix.isEmpty()) {
      tableName += "_" + tableSuffix;
    }
    builder.append(") " + tableName);
  }

  // wraps the actual sql code for the element so that it represents an encapsulated table
  public void generateSQLQueryTable(QuerySQLGenHelper genHelper) throws QueryException {
    generateSQLQueryTable(genHelper, "");
  }

  public String getReferencedTableForJoin(QuerySQLGenHelper genHelper) {
    return "";
  }

  public String getCountGroupColumn(QuerySQLGenHelper genHelper) {
    return "";
  }

  protected boolean createTimeColumn() {
    List<QueryIDFilter> someFilters = getAncestorIDFilters();
    for (QueryIDFilter filter : someFilters) {
      if (filter.hasTimeRestrictions()) {
        return true;
      }
    }
    return false;
  }

  protected boolean createDocIDColumn() {
    List<QueryIDFilter> someFilters = getAncestorIDFilters();
    for (QueryIDFilter filter : someFilters) {
      if (filter.getFilterIDType() == FilterIDType.DocID) {
        return true;
      }
    }
    return false;
  }

  protected boolean createYearColumn() {
    List<QueryIDFilter> someFilters = getAncestorIDFilters();
    for (QueryIDFilter filter : someFilters) {
      if (filter.getFilterIDType() == FilterIDType.Year) {
        return true;
      }
    }
    return false;
  }

  protected boolean createCaseIDColumn() {
    // return getMyQueryElem().createCaseIDColumn();
    for (QueryIDFilter aFilter : getAncestorIDFilters()) {
      if (aFilter.getFilterIDType() == FilterIDType.CaseID) {
        return true;
      }
    }
    return false;
  }

  public List<QueryIDFilter> getAncestorIDFilters() {
    return getMyQueryElem().getAncestorIDFilters();
  }

  public String getXMLName() {
    return getMyQueryElem().getXMLName();
  }

}
