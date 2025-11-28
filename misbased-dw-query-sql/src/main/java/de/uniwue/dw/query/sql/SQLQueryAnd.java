package de.uniwue.dw.query.sql;

import java.util.List;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryAnd;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryNot;
import de.uniwue.dw.query.model.lang.QueryStructureElem;
import de.uniwue.dw.query.model.lang.QueryTempOpRel;
import de.uniwue.dw.query.sql.util.QuerySQLGenHelper;

public class SQLQueryAnd extends SQLQueryStructureContainingElem {

  public SQLQueryAnd(QueryAnd anElem) {
    super(anElem);
  }

  @Override
  public QueryAnd getMyQueryElem() {
    return (QueryAnd) queryElem;
  }

  @Override
  public void createPIDColumnForSelect(QuerySQLGenHelper genHelper) {
    StringBuilder builder = genHelper.sqlQuery;
    QueryStructureElem firstDisplayedChild = getChildren().get(0);
    SQLQueryStructureElem sqlChild = SQLQueryElemFactory.generateSQLElem(firstDisplayedChild);
    String firstChildPIDColumn = genHelper.getPIDColumnName(sqlChild);
    String myPIDColumn = genHelper.getPIDColumnName(this);
    builder.append(firstChildPIDColumn + " AS " + myPIDColumn + ", ");
  }

  @Override
  public void createCaseIDColumnForSelect(QuerySQLGenHelper genHelper) {
    StringBuilder builder = genHelper.sqlQuery;
    if (createCaseIDColumn()) {
      QueryStructureElem firstDisplayedChild = getChildren().get(0);
      SQLQueryStructureElem sqlChild = SQLQueryElemFactory.generateSQLElem(firstDisplayedChild);
      String firstChildCaseIDColumn = genHelper.getCaseIDColumnName(sqlChild);
      String caseIDColumn = genHelper.getCaseIDColumnName(this);
      builder.append(firstChildCaseIDColumn + " AS " + caseIDColumn + ", ");
    }
  }

  @Override
  public void createDocIDColumnForSelect(QuerySQLGenHelper genHelper) {
    StringBuilder builder = genHelper.sqlQuery;
    if (createDocIDColumn()) {
      QueryStructureElem firstDisplayedChild = getChildren().get(0);
      SQLQueryStructureElem sqlChild = SQLQueryElemFactory.generateSQLElem(firstDisplayedChild);
      String firstChildDocIDColumn = genHelper.getDocIDColumnName(sqlChild);
      String docIDColumn = genHelper.getDocIDColumnName(this);
      builder.append(firstChildDocIDColumn + " AS " + docIDColumn + ", ");
    }
  }

  @Override
  public void createTimeColumnForSelect(QuerySQLGenHelper genHelper) {
    StringBuilder builder = genHelper.sqlQuery;
    if (createTimeColumn()) {
      QueryStructureElem firstDisplayedChild = getChildren().get(0);
      SQLQueryStructureElem sqlChild = SQLQueryElemFactory.generateSQLElem(firstDisplayedChild);
      String firstChildTimeColumn = genHelper.getMeasureTimeColumnName(sqlChild);
      String timeColumn = genHelper.getMeasureTimeColumnName(this);
      builder.append(firstChildTimeColumn + " AS " + timeColumn + ", ");
    }
  }

  @Override
  public void createYearColumnForSelect(QuerySQLGenHelper genHelper) {
    StringBuilder builder = genHelper.sqlQuery;
    if (createYearColumn()) {
      QueryStructureElem firstDisplayedChild = getChildren().get(0);
      SQLQueryStructureElem sqlChild = SQLQueryElemFactory.generateSQLElem(firstDisplayedChild);
      String firstChildYearColumn = genHelper.getYearColumnName(sqlChild);
      String yearColumn = genHelper.getYearColumnName(this);
      builder.append(firstChildYearColumn + " AS " + yearColumn + ", ");
    }
  }

  // TODO: relative temporale Abhängigkeiten könnten über Bedingungen in den ON Statements der
  // jeweiligen Attribute gemacht werden
  // Die Lösung hätte die Einschränkung dass nur Attribute, die direkt im AND drin sind relativ
  // zueinander gemacht werden können
  // Und die Attribute müssen vorher so sortiert werden, dass sie direkt hintereinander gejoined
  // werden
  @Override
  public void generateSQL(QuerySQLGenHelper genHelper) throws QueryException {
    StringBuilder builder = genHelper.sqlQuery;
    builder.append(" -- begin " + getXMLName() + "\n");
    builder.append("SELECT ");
    createColumnsForInnerSelect(genHelper);
    builder.append(" FROM\n");

    QueryStructureElem firstChild = getChildren().get(0);
    List<String> firstChildIDColumn = getElemJoinColumns(firstChild, genHelper);

    boolean first = true;
    for (QueryStructureElem aChild : getMyQueryElem().getChildren()) {
      if (aChild instanceof QueryNot) {
        QueryNot aNot = (QueryNot) aChild;
        generateSQLForNot(genHelper, aNot, firstChildIDColumn);
      } else {
        generateSQLForNonNotChild(genHelper, first, aChild, firstChildIDColumn);
      }
      if (first) {
        first = false;
      }
    }
    builder.append(" -- end " + getMyQueryElem().getXMLName() + "\n");
  }

  private void generateSQLForNot(QuerySQLGenHelper genHelper, QueryNot aNot,
          List<String> firstChildPIDColumns) {
    // TODO: implement NOT correctly
    // StringBuilder builder = genHelper.sqlQuery;
    // builder.append(" WHERE " + firstChildPIDColumn + " NOT IN ");
    // aNot.generateSQLQueryTable(genHelper);
  }

  private void generateSQLForNonNotChild(QuerySQLGenHelper genHelper, boolean first,
          QueryStructureElem aChild, List<String> firstChildIDColumns) throws QueryException {
    StringBuilder builder = genHelper.sqlQuery;
    if (!first) {
      if (aChild.isOptional()) {
        builder.append(" LEFT");
      }
      builder.append(" JOIN \n");
    }
    SQLQueryStructureElem sqlChild = SQLQueryElemFactory
            .generateSQLElem(aChild);
    sqlChild.generateSQLQueryTable(genHelper);
    List<String> childIDColumns = getElemJoinColumns(aChild, genHelper);
    if (!first) {
      builder.append("\n ON ");
      boolean firstChild = true;
      for (int i = 0; i < firstChildIDColumns.size(); i++) {
        if (!firstChild) {
          builder.append(" AND ");
        }
        String firstChildIDColumn = firstChildIDColumns.get(i);
        String childIDColumn = childIDColumns.get(i);
        builder.append(firstChildIDColumn + " = " + childIDColumn + " \n");
        if (firstChild) {
          firstChild = false;
        }
      }
      if (aChild instanceof QueryAttribute) {
        QueryAttribute childAttr = (QueryAttribute) aChild;
        generateAdditionalSQLForChildAttribute(genHelper, childAttr);
      }
    } else {
      first = false;
    }
  }

  private void generateAdditionalSQLForChildAttribute(QuerySQLGenHelper genHelper,
          QueryAttribute childAttr) {
    StringBuilder builder = genHelper.sqlQuery;
    for (QueryTempOpRel anOp : childAttr.getTemporalOpsRel()) {
      // this does not work !!
//      SQLQueryStructureElem sqlChild = SQLQueryElemFactory.generateSQLElem(childAttr);
//      SQLQueryStructureElem sqlRefElem = SQLQueryElemFactory.generateSQLElem(anOp.getRefElem());
//      String childTimeColumn = genHelper.getMeasureTimeColumnName(sqlChild);
//      String refChildTimeColumn = genHelper.getMeasureTimeColumnName(sqlRefElem);
//      if ((anOp.dayShiftMin != 0) || (anOp.monthShiftMin != 0) || (anOp.yearShiftMin != 0)) {
//        builder.append(" AND ");
//        if (anOp.dayShiftMin != 0) {
//          builder.append("DATEADD(DAY, " + anOp.dayShiftMin + ", ");
//        }
//        if (anOp.monthShiftMin != 0) {
//          builder.append("DATEADD(MONTH, " + anOp.monthShiftMin + ", ");
//        }
//        if (anOp.yearShiftMin != 0) {
//          builder.append("DATEADD(YEAR, " + anOp.yearShiftMin + ", ");
//        }
//        builder.append(refChildTimeColumn + ") < " + childTimeColumn + " \n");
//      }
//      if ((anOp.dayShiftMax != 0) || (anOp.monthShiftMax != 0) || (anOp.yearShiftMax != 0)) {
//        builder.append(" AND " + refChildTimeColumn);
//        if (anOp.dayShiftMax != 0) {
//          builder.append("DATEADD(DAY, " + anOp.dayShiftMax + ", ");
//        }
//        if (anOp.monthShiftMax != 0) {
//          builder.append("DATEADD(MONTH, " + anOp.monthShiftMax + ", ");
//        }
//        if (anOp.yearShiftMax != 0) {
//          builder.append("DATEADD(YEAR, " + anOp.yearShiftMax + ", ");
//        }
//        builder.append(refChildTimeColumn + ") > " + childTimeColumn + " \n");
//      }
    }
  }

}
