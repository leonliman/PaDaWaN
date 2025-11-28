package de.uniwue.dw.query.sql;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryElem;
import de.uniwue.dw.query.sql.util.QuerySQLGenHelper;
import de.uniwue.misc.sql.DBType;

public abstract class SQLQueryElem {

  protected QueryElem queryElem;

  public SQLQueryElem(QueryElem anElem) {
    queryElem = anElem;
  }

  public QueryElem getMyQueryElem() {
    return queryElem;
  }

  // this method fills the StringBuilder contained in the GenHelper
  public abstract void generateSQL(QuerySQLGenHelper genHelper) throws QueryException;

  protected void addComment(QuerySQLGenHelper genHelper, String comment) {
    StringBuilder builder = genHelper.sqlQuery;
    if (genHelper.sqlManager.getDBType() == DBType.MSSQL) {
      builder.append(" # ");
    } else {
      builder.append(" -- ");
    }
    builder.append(comment);
  }

}
