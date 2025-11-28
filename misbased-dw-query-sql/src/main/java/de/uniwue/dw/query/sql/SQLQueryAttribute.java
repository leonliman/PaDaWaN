package de.uniwue.dw.query.sql;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.sql.IDwSqlSchemaConstant;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.*;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;
import de.uniwue.dw.query.sql.util.QuerySQLGenHelper;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;

import java.sql.SQLException;
import java.util.List;

public class SQLQueryAttribute extends SQLQueryStructureElem implements IDwSqlSchemaConstant {

  public SQLQueryAttribute(QueryAttribute anElem) {
    super(anElem);
  }

  @Override
  public QueryAttribute getMyQueryElem() {
    return (QueryAttribute) queryElem;
  }

  private boolean createValueColumn() {
    return getMyQueryElem().displayValue();
  }

  private boolean displayInfoDate() {
    return getMyQueryElem().displayInfoDate();
  }

  private boolean displayCaseID() {
    return getMyQueryElem().displayCaseID();
  }

  private boolean displayDocID() {
    return getMyQueryElem().displayDocID();
  }

  private CatalogEntry getCatalogEntry() {
    return getMyQueryElem().getCatalogEntry();
  }

  private String getDesiredContent() {
    return getMyQueryElem().getDesiredContent();
  }

  private ContentOperator getContentOperator() {
    return getMyQueryElem().getContentOperator();
  }

  private ReductionOperator getReductionOperator() {
    if ((getMyQueryElem().getRoot().getAttributesRecursive().size() > 1)
            && (getMyQueryElem().getReductionOperator() == ReductionOperator.NONE)) {
      return ReductionOperator.EARLIEST;
    } else {
      return getMyQueryElem().getReductionOperator();
    }
  }

  // this join is done directly in the Attribute's SQL-Code. As the columns are not yet aliased to
  // the normalized names (which is returned by "getElemJoinColumn") we have to use their original
  // names
  public String getElemInnerstJoinColumn(QueryIDFilter filter, QuerySQLGenHelper genHelper) {
    if (filter.getFilterIDType() == FilterIDType.PID) {
      return "PID";
    } else if (filter.getFilterIDType() == FilterIDType.CaseID) {
      return "CaseID";
    } else if (filter.getFilterIDType() == FilterIDType.DocID) {
      return "ref";
    } else {
      return "";
    }
  }

  @Override
  public void createColumnsForInnerSelect(QuerySQLGenHelper genHelper) {
    StringBuilder builder = genHelper.sqlQuery;
    if (createValueColumn()) {
      builder.append(genHelper.getValueColumnName(this) + ", ");
    }
    if (createTimeColumn() || (getMyQueryElem().getReferencingTempRelOps().size() > 0)
            || (getMyQueryElem().getTemporalOpsRel().size() > 0)) {
      builder.append(genHelper.getMeasureTimeColumnName(this) + ", ");
    }
    if (createCaseIDColumn()) {
      builder.append(genHelper.getCaseIDColumnName(this) + ", ");
    }
    if (createDocIDColumn()) {
      builder.append(genHelper.getDocIDColumnName(this) + ", ");
    }
    if (createYearColumn()) {
      String timeColumn = genHelper.getMeasureTimeColumnName(this);
      String valueColumn = genHelper.getYearColumnName(this);
      String columnExpression = "DATEPART(yy, " + timeColumn + ") AS " + valueColumn;
      builder.append(columnExpression + ", ");
    }
  }

  @Override
  public void createColumnsForOuterSelect(QuerySQLGenHelper genHelper, boolean createTable,
          boolean finalSelect) {
    StringBuilder builder = genHelper.sqlQuery;
    if (createValueColumn()) {
      builder.append(genHelper.getValueColumnName(this));
      if (createTable) {
        builder.append(" " + genHelper.getColumnDataType(this));
      }
      builder.append(", ");
    }
    if (displayInfoDate()) {
      builder.append(genHelper.getMeasureTimeColumnName(this));
      if (createTable) {
        builder.append(" " + genHelper.getColumnTimeDataType());
      }
      builder.append(", ");
    }
    if (displayCaseID()) {
      builder.append(genHelper.getCaseIDColumnName(this));
      if (createTable) {
        builder.append(" BIGINT");
      }
      builder.append(", ");
    }
    if (displayDocID()) {
      builder.append(genHelper.getDocIDColumnName(this));
      if (createTable) {
        builder.append(" BIGINT");
      }
      builder.append(", ");
    }
  }

  public void generateFilter(QuerySQLGenHelper genHelper, String tableName)
          throws NumberFormatException, QueryException {
    StringBuilder builder = genHelper.sqlQuery;
    if ((getCatalogEntry().getDataType() == CatalogEntryType.Bool)
            && (getCatalogEntry().getChildren().size() > 0)) {
      String attrList = "";
      attrList += getCatalogEntry().getAttrId();
      int i = 0;
      for (CatalogEntry aSibling : getCatalogEntry().getDescendants()) {
        if (aSibling.getDataType() != CatalogEntryType.Structure) {
          if (!attrList.isEmpty()) {
            attrList += ", ";
          }
          attrList += aSibling.getAttrId();
          i++;
          if (i > 20) {
            attrList += "\n";
            i = 0;
          }
        }
      }
      builder.append(tableName + ".attrId IN (" + attrList + ")");
    } else {
      builder.append(tableName + ".attrId = " + getCatalogEntry().getAttrId());
    }
    try {
      if (SQLPropertiesConfiguration.getInstance().getSQLConfig().dbType.equals(DBType.MySQL))
        builder.append(" AND (storno = 0 OR storno is null)");
      else
        builder.append(" AND storno = 0");
    } catch (SQLException e) {
      e.printStackTrace();
      builder.append(" AND storno = 0");
    }
    if (getContentOperator() != ContentOperator.EXISTS) {
      builder.append(" AND \n");
      if (getContentOperator() == ContentOperator.EQUALS) {
        if ((getCatalogEntry().getDataType() == CatalogEntryType.Bool)
                || (getCatalogEntry().getDataType() == CatalogEntryType.SingleChoice)) {
          builder.append(
                  tableName + ".valueShort = '" + getDesiredContent().replaceAll("'", "''") + "'");
        } else {
          builder.append(tableName + ".valueDec = " + getDesiredContent());
        }
      } else if (getContentOperator() == ContentOperator.LESS) {
        builder.append(tableName + ".valueDec < " + getDesiredContent());
      } else if (getContentOperator() == ContentOperator.LESS_OR_EQUAL) {
        builder.append(tableName + ".valueDec <= " + getDesiredContent());
      } else if (getContentOperator() == ContentOperator.MORE) {
        builder.append(tableName + ".valueDec > " + getDesiredContent());
      } else if (getContentOperator() == ContentOperator.MORE_OR_EQUAL) {
        builder.append(tableName + ".valueDec >= " + getDesiredContent());
      } else if (getContentOperator() == ContentOperator.BETWEEN) {
        builder.append(tableName + ".valueDec < "
                + getMyQueryElem().getDesiredContentBetweenUpperBoundDouble() + "\n" + " AND "
                + tableName + ".valueDec > "
                + getMyQueryElem().getDesiredContentBetweenLowerBoundDouble());
      } else if (getContentOperator() == ContentOperator.CONTAINS) {
        boolean fulltextEnabled = false;
        try {
          fulltextEnabled = genHelper.sqlManager.fulltextIndexEnabled();
        } catch (SQLException e) {
          throw new QueryException(e);
        }
        if ((genHelper.sqlManager.getDBType() == DBType.MSSQL) && fulltextEnabled
                && DwClientConfiguration.getInstance().useFulltextIndex()) {
          builder.append("CONTAINS(" + tableName + ".value, '\""
                  + getDesiredContent().replaceAll("'", "''") + "\"')");
        } else {
          builder.append(
                  tableName + ".value LIKE '%" + getDesiredContent().replaceAll("'", "''") + "%'");
        }
      }
    }
    boolean first = true;
    if (getMyQueryElem().getTempOpsAbs().size() > 0) {
      builder.append(" AND (\n");
    }
    for (QueryTempOpAbs aTempOp : getMyQueryElem().getTempOpsAbs()) {
      String tempOpSQLString = "(";
      if (aTempOp.absMinDate != null) {
        tempOpSQLString += tableName + ".measuretime >= '" + aTempOp.absMinDate + "'";
      }
      if (aTempOp.absMaxDate != null) {
        if (aTempOp.absMinDate != null) {
          tempOpSQLString += " AND ";
        }
        tempOpSQLString += tableName + ".measuretime <= '" + aTempOp.absMaxDate + "'";
      }
      tempOpSQLString += ")";
      if (!first) {
        builder.append(" OR ");
      }
      builder.append(tempOpSQLString);
    }
    if (getMyQueryElem().getTempOpsAbs().size() > 0) {
      builder.append(") ");
    }
  }

  @Override
  public void generateSQL(QuerySQLGenHelper genHelper) throws QueryException {
    StringBuilder builder = genHelper.sqlQuery;
    String valueColumnName = genHelper.getValueColumnName(this);
    builder.append(" -- begin " + getXMLName() + ": " + valueColumnName + "\n");
    builder.append("SELECT ");
    String valueType;
    if ((getCatalogEntry() == null) || (getCatalogEntry().getDataType() == CatalogEntryType.Bool)
            || getMyQueryElem().isOnlyDisplayExistence()) {
      valueType = "'X'";
    } else if (getCatalogEntry().getDataType() == CatalogEntryType.Number) {
      valueType = "valueDec";
    } else if (getMyQueryElem().getValueInFile()) {
      valueType = "value";
    } else {
      valueType = "valueShort";
    }
    String pidColumnName = genHelper.getPIDColumnName(this);
    String timeColumnName = genHelper.getMeasureTimeColumnName(this);
    builder.append(valueType + " AS " + valueColumnName);
    if (createTimeColumn() || (getMyQueryElem().getReferencingTempRelOps().size() > 0)
            || (getMyQueryElem().getTemporalOpsRel().size() > 0)) {
      builder.append(",\n measuretime AS " + timeColumnName);
    }
    if (createYearColumn()) {
      String yearColumnName = genHelper.getYearColumnName(this);
      builder.append(",\n DATEPART(yy, measuretime) AS " + yearColumnName);
    }
    if (createCaseIDColumn()) {
      String caseIDColumn = genHelper.getCaseIDColumnName(this);
      builder.append(",\n caseID AS " + caseIDColumn);
    }
    if (createDocIDColumn()) {
      String docIDColumn = genHelper.getDocIDColumnName(this);
      builder.append(",\n ref AS " + docIDColumn);
    }
    builder.append(",\n pid AS " + pidColumnName);
    String tableName = genHelper.getTableName(this);
    builder.append(" FROM " + T_INFO + " AS " + tableName + " ");

    if (!getMyQueryElem().hasRestrictionsWithOtherAttributes()
            && (getMyQueryElem().getAncestorFilterWithTimes() == null)
            && (getMyQueryElem().getReductionOperator() != ReductionOperator.NONE)) {
      // if the attributes has restrictions other than its own (e.g. same case as another attribute)
      // the reduction of values may not be done here but instead after the outer reduction has been
      // performed
      generateSQLWithPivotOperator(genHelper, valueType);
    }

    generateSQLIDFilter(genHelper, tableName);
    builder.append(" WHERE \n");
    generateFilter(genHelper, tableName);

    builder.append(" -- end " + getMyQueryElem().getXMLName() + "\n");
  }

  private void generateSQLWithPivotOperator(QuerySQLGenHelper genHelper, String valueType)
          throws NumberFormatException, QueryException {
    StringBuilder builder = genHelper.sqlQuery;
    String tableName = genHelper.getTableName(this);

    QueryIDFilter mostRestrictingAncestorFilters = getMyQueryElem()
            .getMostRestrictingAncestorFilters();
    String groupColumn = "pid";
    if (mostRestrictingAncestorFilters != null) {
      if (mostRestrictingAncestorFilters.getFilterIDType() == FilterIDType.CaseID) {
        groupColumn = "caseid";
      } else if (mostRestrictingAncestorFilters.getFilterIDType() == FilterIDType.DocID) {
        groupColumn = "ref";
      }
    }

    String pivotColumn = "";
    if ((getReductionOperator() == ReductionOperator.MAX)
            || (getReductionOperator() == ReductionOperator.MIN)) {
      pivotColumn = valueType;
    } else if ((getReductionOperator() == ReductionOperator.EARLIEST)
            || (getReductionOperator() == ReductionOperator.LATEST)) {
      pivotColumn = "measuretime";
    }
    String pivotDirection = "";
    if ((getReductionOperator() == ReductionOperator.MAX)
            || (getReductionOperator() == ReductionOperator.LATEST)) {
      pivotDirection = "MAX";
    } else if ((getReductionOperator() == ReductionOperator.MIN)
            || (getReductionOperator() == ReductionOperator.EARLIEST)) {
      pivotDirection = "MIN";
    }

    builder.append("JOIN (\n");
    builder.append(
            "  SELECT MAX(infoID) as infoID FROM " + T_INFO + " as " + tableName + "_1, (\n");

    builder.append("    SELECT " + groupColumn + ", " + pivotDirection + "(" + pivotColumn
            + ") AS pivotValue \n");
    builder.append("    FROM " + T_INFO + " AS " + tableName + "_2 WHERE \n");
    generateFilter(genHelper, tableName + "_2");
    builder.append(" GROUP BY " + groupColumn + ") " + tableName + "_3 WHERE \n");

    builder.append("  " + tableName + "_3." + groupColumn + " = " + tableName + "_1." + groupColumn
            + " AND ");
    generateFilter(genHelper, tableName + "_1");
    builder.append(" AND " + tableName + "_1." + pivotColumn + " = pivotValue");
    builder.append(" GROUP BY " + tableName + "_1." + groupColumn + ") " + tableName + "_4 ON \n");
    builder.append(tableName + "_4.infoID = " + tableName + ".infoid");
  }

  public void generateSQLIDFilter(QuerySQLGenHelper genHelper, String tableName) {
    StringBuilder builder = genHelper.sqlQuery;
    List<QueryIDFilter> someFilters = getAncestorIDFilters();
    for (QueryIDFilter filter : someFilters) {
      if (!filter.hasIDRestrictions()) {
        continue;
      }
      SQLQueryIDFilter sqlFilter = (SQLQueryIDFilter) SQLQueryElemFactory.generateSQLElem(filter);
      String filterTable = genHelper.getTmpTableName(sqlFilter);
      String filterIDColumn = sqlFilter.getIDColumnForGivenIDs(genHelper);
      String joinColumn = getElemInnerstJoinColumn(filter, genHelper);
      // Better do the join already in the attribute table as it prevents wrong joins in the outer
      // elements
      builder.append("\nJOIN " + filterTable + " ON " + tableName + "." + joinColumn + " = "
              + filterTable + "." + filterIDColumn + " ");
      if (filter.hasTimeRestrictions()) {
        builder.append("AND\n ");
        if (filter.isRestrictingTimesAreDates()) {
          builder.append("DATEDIFF(");
          if (genHelper.sqlManager.getDBType() == DBType.MSSQL) {
            builder.append("d, ");
          }
          builder.append(tableName + ".measureTime, " + filterTable + "."
                  + sqlFilter.getTimeColumnName() + ") = 0");
        } else {
          builder.append(tableName + ".measureTime = " + filterTable + "."
                  + sqlFilter.getTimeColumnName());
        }
      }
    }
  }

}
