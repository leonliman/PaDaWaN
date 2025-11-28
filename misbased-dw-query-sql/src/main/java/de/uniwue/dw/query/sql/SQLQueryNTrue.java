package de.uniwue.dw.query.sql;

import java.util.List;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryNTrue;
import de.uniwue.dw.query.model.lang.QueryStructureContainingElem;
import de.uniwue.dw.query.model.lang.QueryStructureElem;
import de.uniwue.dw.query.sql.util.QuerySQLGenHelper;

public class SQLQueryNTrue extends SQLQueryStructureContainingElem {

  public SQLQueryNTrue(QueryStructureContainingElem anElem) {
    super(anElem);
  }

  @Override
  public QueryNTrue getMyQueryElem() {
    return (QueryNTrue) queryElem;
  }

  @Override
  public void generateSQL(QuerySQLGenHelper genHelper) throws QueryException {
    List<QueryStructureElem> children = getChildren();
    if (children.size() == 0) {
      return;
    }
    StringBuilder builder = genHelper.sqlQuery;
    String myPidColumn = genHelper.getPIDColumnName(this);
    builder.append(" -- begin NTrue\n");
    builder.append("SELECT " + myPidColumn + ", ");
    createColumnsForInnerSelect(genHelper);
    builder.append(" FROM\n");
    boolean first = true;
    QueryStructureElem firstDisplayedChild = children.get(0);
    SQLQueryStructureElem sqlFirstChild = SQLQueryElemFactory.generateSQLElem(firstDisplayedChild);
    String firstChildPidColumn = genHelper.getPIDColumnName(sqlFirstChild);
    String myTableName = genHelper.getTableName(this);
    builder.append(" -- the ID collection for the NTrue\n");
    builder.append("(SELECT " + firstChildPidColumn + " AS " + myPidColumn + " FROM (\n");
    for (QueryStructureElem aChild : children) {
      if (!aChild.isOptional()) {
        SQLQueryStructureElem sqlChild = SQLQueryElemFactory.generateSQLElem(aChild);
        String childPidColumn = genHelper.getPIDColumnName(sqlChild);
        if (!first) {
          builder.append("\n UNION ALL ");
        }
        builder.append("SELECT DISTINCT(" + childPidColumn + ") FROM ");
        sqlChild.generateSQLQueryTable(genHelper);
        first = false;
      }
    }
    builder.append(") " + myTableName + "_pid_tmp \n" + "GROUP BY " + firstChildPidColumn
            + " HAVING COUNT(*) >= " + getMyQueryElem().n + "\n) " + myTableName + "_pid_table \n");
    builder.append(" -- the joining of the data values for the NTrue\n");
    for (QueryStructureElem aChild : children) {
      SQLQueryStructureElem sqlChild = SQLQueryElemFactory.generateSQLElem(aChild);
      String childPidColumnName = genHelper.getPIDColumnName(sqlChild);
      builder.append(" LEFT JOIN ");
      sqlChild.generateSQLQueryTable(genHelper);
      builder.append("\n");
      builder.append(" ON " + myPidColumn + " = " + childPidColumnName + "\n");
    }
    builder.append(" -- end NTrue\n");
  }

}
