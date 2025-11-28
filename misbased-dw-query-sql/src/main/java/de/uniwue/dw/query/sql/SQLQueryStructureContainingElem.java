package de.uniwue.dw.query.sql;

import java.util.ArrayList;
import java.util.List;

import de.uniwue.dw.query.model.lang.QueryIDFilter;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;
import de.uniwue.dw.query.model.lang.QueryStructureContainingElem;
import de.uniwue.dw.query.model.lang.QueryStructureElem;
import de.uniwue.dw.query.sql.util.QuerySQLGenHelper;

public abstract class SQLQueryStructureContainingElem extends SQLQueryStructureElem {

  public SQLQueryStructureContainingElem(QueryStructureContainingElem anElem) {
    super(anElem);
  }

  @Override
  public QueryStructureContainingElem getMyQueryElem() {
    return (QueryStructureContainingElem) queryElem;
  }

  public void createColumnsForOuterSelect(QuerySQLGenHelper genHelper, boolean createTable,
          boolean finalSelect) {
    for (QueryStructureElem anElem : getMyQueryElem().getChildren()) {
      SQLQueryStructureElem sqlElem = SQLQueryElemFactory.generateSQLElem(anElem);
      sqlElem.createColumnsForOuterSelect(genHelper, createTable, finalSelect);
    }
  }

  public void generateSQLIDFilter(QuerySQLGenHelper genHelper) {
    StringBuilder builder = genHelper.sqlQuery;
    List<QueryIDFilter> someFilters = getAncestorIDFilters();
    for (QueryIDFilter filter : someFilters) {
      if (!filter.hasIDRestrictions()) {
        continue;
      }
      SQLQueryIDFilter sqlFilter = (SQLQueryIDFilter) SQLQueryElemFactory.generateSQLElem(filter);
      String filterTable = genHelper.getTmpTableName(sqlFilter);
      String filterIDColumn = sqlFilter.getIDColumnForGivenIDs(genHelper);
      builder.append("JOIN " + filterTable + " ON ");
      boolean first = true;
      for (QueryStructureElem aChild : getMyQueryElem().getChildren()) {
        if (first) {
          first = false;
        } else {
          builder.append("AND ");
        }
        List<String> joinColumns = getElemJoinColumns(aChild, genHelper);
        for (String aJoinColumn : joinColumns) {
          //
          builder.append("(" + aJoinColumn + " = " + filterIDColumn + " ");
          builder.append("OR " + aJoinColumn + " IS NULL)");
        }
        sqlFilter.generateSQLTimeRestriction(genHelper, aChild);
      }
    }
  }

  public List<String> getElemJoinColumns(QueryStructureElem anElem, QuerySQLGenHelper genHelper) {
    QueryIDFilter filter = getMyQueryElem().getMostRestrictingAncestorFilters();
    List<String> result = new ArrayList<String>();

    SQLQueryStructureElem sqlElem = SQLQueryElemFactory.generateSQLElem(anElem);
    if ((filter == null) || (filter.getFilterIDType() == FilterIDType.PID)) {
      result.add(genHelper.getPIDColumnName(sqlElem));
    } else if (filter.getFilterIDType() == FilterIDType.CaseID) {
      result.add(genHelper.getCaseIDColumnName(sqlElem));
    } else if (filter.getFilterIDType() == FilterIDType.DocID) {
      result.add(genHelper.getDocIDColumnName(sqlElem));
    } else if (filter.getFilterIDType() == FilterIDType.Year) {
      result.add(genHelper.getPIDColumnName(sqlElem));
      result.add(genHelper.getYearColumnName(sqlElem));
    } else {
      @SuppressWarnings("unused") int x = 1 / 0;
    }
    return result;
  }

  public void createPIDColumnForSelect(QuerySQLGenHelper genHelper) {
    StringBuilder builder = genHelper.sqlQuery;
    String pidColumn = genHelper.getPIDColumnName(this);
    builder.append(pidColumn + ", ");
  }

  public void createCaseIDColumnForSelect(QuerySQLGenHelper genHelper) {
    StringBuilder builder = genHelper.sqlQuery;
    if (createCaseIDColumn()) {
      String caseIDColumn = genHelper.getCaseIDColumnName(this);
      builder.append(caseIDColumn + ", ");
    }
  }

  public void createDocIDColumnForSelect(QuerySQLGenHelper genHelper) {
    StringBuilder builder = genHelper.sqlQuery;
    if (createDocIDColumn()) {
      String docIDColumn = genHelper.getDocIDColumnName(this);
      builder.append(docIDColumn + ", ");
    }
  }

  public void createTimeColumnForSelect(QuerySQLGenHelper genHelper) {
    StringBuilder builder = genHelper.sqlQuery;
    if (createTimeColumn()) {
      String timeColumn = genHelper.getMeasureTimeColumnName(this);
      builder.append(timeColumn + ", ");
    }
  }

  public void createYearColumnForSelect(QuerySQLGenHelper genHelper) {
    StringBuilder builder = genHelper.sqlQuery;
    if (createYearColumn()) {
      String timeColumn = genHelper.getYearColumnName(this);
      builder.append(timeColumn + ", ");
    }
  }

  @Override
  public void createColumnsForInnerSelect(QuerySQLGenHelper genHelper) {
    StringBuilder builder = genHelper.sqlQuery;
    createPIDColumnForSelect(genHelper);
    createCaseIDColumnForSelect(genHelper);
    createDocIDColumnForSelect(genHelper);
    createTimeColumnForSelect(genHelper);
    createYearColumnForSelect(genHelper);
    for (QueryStructureElem anElem : getMyQueryElem().getChildren()) {
      SQLQueryStructureElem sqlElem = SQLQueryElemFactory.generateSQLElem(anElem);
      sqlElem.createColumnsForOuterSelect(genHelper, false, false);
    }
    builder.replace(builder.length() - 2, builder.length(), "");
    builder.append(" ");
  }

  protected List<QueryStructureElem> getChildren() {
    return getMyQueryElem().getChildren();
  }

}
