package de.uniwue.dw.query.sql;

import java.util.List;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryIDFilter;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;
import de.uniwue.dw.query.model.lang.QueryOr;
import de.uniwue.dw.query.model.lang.QueryStructureElem;
import de.uniwue.dw.query.sql.util.QuerySQLGenHelper;

// TODO: wenn von den geOrten Atttributen eine Case-ID zurückgegeben werden soll wird im Moment
// noch die CaseID des 1. Attributs genommen. Falls dieses nicht vorhanden war ist die CaseID
// leer. Wie kann man eine CaseID von einem beliebigen gefüllten Attribut nehmen ?

public class SQLQueryOr extends SQLQueryStructureContainingElem {

  public SQLQueryOr(QueryOr anElem) {
    super(anElem);
  }

  @Override
  public QueryOr getMyQueryElem() {
    return (QueryOr) queryElem;
  }

  @Override
  public void generateSQL(QuerySQLGenHelper genHelper) throws QueryException {
    List<QueryStructureElem> children = getChildren();
    String myTableName = genHelper.getTableName(this);

    List<String> myJoinIDColumns = getElemJoinColumns(getMyQueryElem(), genHelper);

    StringBuilder builder = genHelper.sqlQuery;
    builder.append(" -- begin " + getXMLName() + "\n");
    builder.append("SELECT ");
    createColumnsForInnerSelect(genHelper);
    builder.append(" FROM (\n");
    builder.append(" SELECT -- begin " + getMyQueryElem().getXMLName() + ", collect IDs \n");

    boolean createCaseIDColumn = false;
    boolean createDocIDColumn = false;
    boolean createTimeColumn = false;
    QueryIDFilter filter = getMyQueryElem().getMostRestrictingAncestorFilters();
    if (filter != null) {
      if ((filter.getFilterIDType() == FilterIDType.CaseID)
              || (filter.getFilterIDType() == FilterIDType.DocID)) {
        createCaseIDColumn = true;
      }
      if (filter.getFilterIDType() == FilterIDType.DocID) {
        createDocIDColumn = true;
      }
      if (getMyQueryElem().getAncestorFilterWithTimes() != null) {
        createTimeColumn = true;
      }
    }

    boolean first = true;
    for (QueryStructureElem aChild : children) {
      if (aChild.isOptional()) {
        continue;
      }
      SQLQueryStructureElem sqlChild = SQLQueryElemFactory.generateSQLElem(aChild);
      String childPidColumn = genHelper.getPIDColumnName(sqlChild);
      String childCaseIDColumn = genHelper.getCaseIDColumnName(sqlChild);
      String childDocIDColumn = genHelper.getDocIDColumnName(sqlChild);
      String childTimeColumn = genHelper.getMeasureTimeColumnName(sqlChild);
      if (first) {
        String myPIDColumn = genHelper.getPIDColumnName(this);
        String myCaseIDColumn = genHelper.getCaseIDColumnName(this);
        String myDocIDColumn = genHelper.getDocIDColumnName(this);
        String myTimeColumn = genHelper.getMeasureTimeColumnName(this);
        builder.append(childPidColumn + " AS " + myPIDColumn);
        if (createCaseIDColumn) {
          builder.append(", " + childCaseIDColumn + " AS " + myCaseIDColumn);
        }
        if (createDocIDColumn) {
          builder.append(", " + childDocIDColumn + " AS " + myDocIDColumn);
        }
        if (createTimeColumn) {
          builder.append(", " + childTimeColumn + " AS " + myTimeColumn);
        }
        builder.append(" FROM ");
        first = false;
      } else {
        builder.append("\n UNION SELECT " + childPidColumn);
        if (createCaseIDColumn) {
          builder.append(", " + childCaseIDColumn);
        }
        if (createDocIDColumn) {
          builder.append(", " + childDocIDColumn);
        }
        if (createTimeColumn) {
          builder.append(", " + childTimeColumn);
        }
        builder.append(" FROM ");
      }
      sqlChild.generateSQLQueryTable(genHelper);
    }
    builder.append("\n) " + myTableName + "_pids ");
    builder.append(" -- end " + getXMLName() + ", collect IDs \n");
    for (QueryStructureElem aChild : children) {
      List<String> childIDColumnNames = getElemJoinColumns(aChild, genHelper);
      builder.append(" LEFT JOIN ");
      SQLQueryStructureElem sqlChild = SQLQueryElemFactory.generateSQLElem(aChild);
      sqlChild.generateSQLQueryTable(genHelper);
      builder.append("\n");
      builder.append(" ON ");
      boolean firstChildColumn = true;
      for (int i = 0; i < childIDColumnNames.size(); i++) {
        if (!firstChildColumn) {
          builder.append(" AND ");
        }
        String myJoinIDColumn = myJoinIDColumns.get(i);
        String childIDColumnName = childIDColumnNames.get(i);
        builder.append(myJoinIDColumn + " = " + childIDColumnName + "\n");
        if (firstChildColumn) {
          firstChildColumn = false;
        }
      }
    }
    generateSQLIDFilter(genHelper);
    builder.append(" -- end " + getXMLName() + "\n");
  }

}
