package de.uniwue.dw.query.sql;

import java.util.Date;
import java.util.HashSet;
import java.util.List;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryAnd;
import de.uniwue.dw.query.model.lang.QueryIDFilter;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;
import de.uniwue.dw.query.model.lang.QueryStructureElem;
import de.uniwue.dw.query.sql.util.QuerySQLGenHelper;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.sql.SQLTypes;
import de.uniwue.misc.util.TimeUtil;

public class SQLQueryIDFilter extends SQLQueryAnd {

  public SQLQueryIDFilter(QueryAnd anElem) {
    super(anElem);
  }

  @Override
  public QueryIDFilter getMyQueryElem() {
    return (QueryIDFilter) queryElem;
  }

  private FilterIDType getFilterIDType() {
    return getMyQueryElem().getFilterIDType();
  }

  private HashSet<Long> getIDs() {
    return getMyQueryElem().getIds();
  }

  public boolean hasIDRestrictions() {
    return getMyQueryElem().hasIDRestrictions();
  }

  private boolean hasGivenTimeRestrictions() {
    return getMyQueryElem().hasGivenTimeRestrictions();
  }

  private boolean hasTimeRestrictions() {
    return getMyQueryElem().hasTimeRestrictions();
  }

  @Override
  public void createPIDColumnForSelect(QuerySQLGenHelper genHelper) {
    if ((getFilterIDType() == FilterIDType.PID) && !getIDs().isEmpty()) {
      StringBuilder builder = genHelper.sqlQuery;
      String idColumn = getIDColumnForGivenIDs(genHelper);
      String myPIDColumn = genHelper.getPIDColumnName(this);
      builder.append(idColumn + " AS " + myPIDColumn + ", ");
    } else {
      super.createPIDColumnForSelect(genHelper);
    }
  }

  @Override
  public void createCaseIDColumnForSelect(QuerySQLGenHelper genHelper) {
    if ((getFilterIDType() == FilterIDType.CaseID) && !getIDs().isEmpty()) {
      StringBuilder builder = genHelper.sqlQuery;
      String idColumn = getIDColumnForGivenIDs(genHelper);
      String myCaseIDColumn = genHelper.getCaseIDColumnName(this);
      builder.append(idColumn + " AS " + myCaseIDColumn + ", ");
    } else {
      super.createCaseIDColumnForSelect(genHelper);
    }
  }

  @Override
  public void createDocIDColumnForSelect(QuerySQLGenHelper genHelper) {
    if ((getFilterIDType() == FilterIDType.DocID) && !getIDs().isEmpty()) {
      StringBuilder builder = genHelper.sqlQuery;
      String idColumn = getIDColumnForGivenIDs(genHelper);
      String myDocIDColumn = genHelper.getDocIDColumnName(this);
      builder.append(idColumn + " AS " + myDocIDColumn + ", ");
    } else {
      super.createDocIDColumnForSelect(genHelper);
    }
  }

  @Override
  public void createColumnsForInnerSelect(QuerySQLGenHelper genHelper) {
    StringBuilder builder = genHelper.sqlQuery;
    if (hasIDRestrictions()) {
      String filterID = getIDColumnForGivenIDs(genHelper);
      builder.append(filterID + ", ");
    }
    if (hasGivenTimeRestrictions()) {
      String timeColumn = getTimeColumnName();
      builder.append(timeColumn + ", ");
    }
    super.createColumnsForInnerSelect(genHelper);
  }

  public void generateSQLTimeRestriction(QuerySQLGenHelper genHelper, QueryStructureElem anotherElem) {
    if (hasTimeRestrictions()) {
      StringBuilder builder = genHelper.sqlQuery;
      SQLQueryStructureElem sqlAnotherElem = SQLQueryElemFactory.generateSQLElem(anotherElem);
      String elemTimeColumn = genHelper.getMeasureTimeColumnName(sqlAnotherElem);
      builder.append("AND\n (");
      if (getMyQueryElem().isRestrictingTimesAreDates()) {
        builder.append("DATEDIFF(");
        if (genHelper.sqlManager.getDBType() == DBType.MSSQL) {
          builder.append("d, ");
        }
        builder.append(elemTimeColumn + ", " + getTimeColumnName() + ") = 0 ");
      } else {
        builder.append(elemTimeColumn + " = " + getTimeColumnName() + " ");
      }
      builder.append(" OR " + elemTimeColumn + " IS NULL)");
    }
  }

  @Override
  public String getCountGroupColumn(QuerySQLGenHelper genHelper) {
    return getIDColumn(genHelper);
  }

  @Override
  public void createColumnsForOuterSelect(QuerySQLGenHelper genHelper, boolean createTable,
          boolean finalSelect) {
    super.createColumnsForOuterSelect(genHelper, createTable, finalSelect);
    StringBuilder builder = genHelper.sqlQuery;
    boolean displayTimeColumnInFinalSelect = false;
    List<QueryIDFilter> someFilters = getAncestorIDFilters();
    // don't create columns for the outer select that originated from myself
    if (finalSelect) {
      return;
    }
    for (QueryIDFilter filter : someFilters) {
      if (filter.hasTimeRestrictions()) {
        displayTimeColumnInFinalSelect = true;
      }
    }
    if (displayTimeColumnInFinalSelect) {
      builder.append(genHelper.getMeasureTimeColumnName(this));
      if (createTable) {
        builder.append(" " + genHelper.getColumnTimeDataType());
      }
      builder.append(", ");
    }
    boolean displayCaseIDInFinalSelect = false;
    for (QueryIDFilter filter : someFilters) {
      if (filter.getFilterIDType() == FilterIDType.CaseID) {
        displayCaseIDInFinalSelect = true;
      }
    }
    if (displayCaseIDInFinalSelect) {
      builder.append(genHelper.getCaseIDColumnName(this));
      if (createTable) {
        builder.append(" BIGINT");
      }
      builder.append(", ");
    }
    boolean displayDocIDInFinalSelect = false;
    for (QueryIDFilter filter : someFilters) {
      if (filter.getFilterIDType() == FilterIDType.DocID) {
        displayDocIDInFinalSelect = true;
      }
    }
    if (displayDocIDInFinalSelect) {
      builder.append(genHelper.getDocIDColumnName(this));
      if (createTable) {
        builder.append(" BIGINT");
      }
      builder.append(", ");
    }
    boolean displayYearInFinalSelect = false;
    for (QueryIDFilter filter : someFilters) {
      if (filter.getFilterIDType() == FilterIDType.Year) {
        displayYearInFinalSelect = true;
      }
    }
    if (displayYearInFinalSelect) {
      String valueColumn = genHelper.getYearColumnName(this);
      builder.append(valueColumn);
      if (createTable) {
        builder.append(" INT");
      }
      builder.append(", ");
    }
  }

  
  @Override
  public void generateSQL(QuerySQLGenHelper genHelper) throws QueryException {
    if (!hasIDRestrictions()) {
      super.generateSQL(genHelper);
      return;
    }
    // the stuff below is only executed when the filter has IDRestrictions !!!
    StringBuilder builder = genHelper.sqlQuery;
    builder.append(" -- begin " + getMyQueryElem().getXMLName() + "\n");
    builder.append("SELECT ");
    createColumnsForInnerSelect(genHelper);
    builder.append(" FROM\n");

    String tableName = genHelper.getTmpTableName(this);
    builder.append(tableName + "\n");
    // this cannot happen for year filter, so no problem here
    String idJoinColumn = getIDColumnForGivenIDs(genHelper);
    for (QueryStructureElem aChild : getChildren()) {
      if (aChild.isOptional()) {
        builder.append("LEFT ");
      }
      builder.append(" JOIN \n");
      SQLQueryStructureElem sqlChild = SQLQueryElemFactory.generateSQLElem(aChild);
      sqlChild.generateSQLQueryTable(genHelper);
      List<String> childIDColumns = getElemJoinColumns(aChild, genHelper);
      builder.append("\n ON ");
      String childIDColumn = childIDColumns.get(0);
      builder.append(idJoinColumn + " = " + childIDColumn + " \n");
      generateSQLTimeRestriction(genHelper, aChild);
    }
    builder.append(" -- end " + getXMLName() + "\n");
  }

  public String getTimeColumnName() {
    String result = "IDFilterTime";
    return result;
  }

  // this is the column which is used from the outside to join with other elements
  public String getIDColumn(QuerySQLGenHelper genHelper) {
    if (getFilterIDType() == FilterIDType.PID) {
      return genHelper.getPIDColumnName(this);
    } else if (getFilterIDType() == FilterIDType.CaseID) {
      return genHelper.getCaseIDColumnName(this);
    } else if (getFilterIDType() == FilterIDType.DocID) {
      return genHelper.getDocIDColumnName(this);
    } else if (getFilterIDType() == FilterIDType.Year) {
      return genHelper.getYearColumnName(this);
    }
    return null;
  }

  // this is an identifying column for a pre given patientID, caseID or documentID
  public String getIDColumnForGivenIDs(QuerySQLGenHelper genHelper) {
    String tableName = genHelper.getTmpTableName(this);
    return "id_" + tableName;
  }

  // this is an identifying column for an entry in this filter
  public String getFilterIDColumnForGivenIDs(QuerySQLGenHelper genHelper) {
    return genHelper.getMeasureTimeColumnName(this);
  }

  public void createIDFilterTmpTable(QuerySQLGenHelper genHelper) {
    if (!hasIDRestrictions()) {
      return;
    }
    StringBuilder builder = genHelper.sqlQuery;
    String tableName = genHelper.getTmpTableName(this);
    String filterIDColumn = getFilterIDColumnForGivenIDs(genHelper);
    String idColumn = getIDColumnForGivenIDs(genHelper);
    String timeColumn = getTimeColumnName();
    builder.append("CREATE TABLE " + tableName + " (\n");
    builder.append("  " + filterIDColumn + " BIGINT "
            + SQLTypes.incrementFlagStartingWith1(genHelper.sqlManager.config)
            + " NOT NULL PRIMARY KEY,\n");
    builder.append("  " + idColumn + " BIGINT NOT NULL");
    if (hasTimeRestrictions()) {
      builder.append(",\n  " + timeColumn + " " + genHelper.getColumnTimeDataType());
    }
    if (genHelper.sqlManager.getDBType() == DBType.MSSQL) {
      builder.append(");\n" + "CREATE INDEX " + tableName + "_id ON " + tableName + " (" + idColumn
              + ");\n");
      if (hasTimeRestrictions()) {
        builder.append("CREATE INDEX " + tableName + "_time ON " + tableName + " (" + timeColumn
                + ");\n" + "\n");
      }
    } else {
      builder.append(",\n" + "INDEX " + tableName + "_id (" + idColumn + "), \n" + "INDEX "
              + tableName + "_time (" + timeColumn + ") \n" + ");\n");
    }
    for (Long anID : getIDs()) {
      if (getMyQueryElem().getRestrictedTimes().containsKey(anID)) {
        HashSet<Date> dates = getMyQueryElem().getRestrictedTimes().get(anID);
        for (Date aDate : dates) {
          builder.append("INSERT INTO " + tableName + " VALUES (" + anID);
          builder.append(", '" + TimeUtil.getSdfWithTimeSQLTimestamp().format(aDate) + "'");
          builder.append(");\n");
        }
      } else {
        builder.append("INSERT INTO " + tableName + " VALUES (" + anID + ");\n");
      }
    }
  }

  public void generateSQLForSubAttribute(QuerySQLGenHelper genHelper) {
    StringBuilder builder = genHelper.sqlQuery;

    builder.append(" -- begin " + getXMLName() + "\n");

    String tableName = genHelper.getTableName(this);
    String pidColumnName = genHelper.getPIDColumnName(this);

    builder.append("SELECT " + pidColumnName);

    if (hasTimeRestrictions()) {
      String timeColumn = getTimeColumnName();
      builder.append(", " + timeColumn);
    }

    builder.append(" FROM " + tableName);
    builder.append(" -- end " + getXMLName() + "\n");
  }

}
