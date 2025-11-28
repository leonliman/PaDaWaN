package de.uniwue.dw.query.sql;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryNot;
import de.uniwue.dw.query.model.lang.QueryStructureContainingElem;
import de.uniwue.dw.query.sql.util.QuerySQLGenHelper;

public class SQLQueryNot extends SQLQueryStructureContainingElem {

  public SQLQueryNot(QueryStructureContainingElem anElem) {
    super(anElem);
  }

  @Override
  public QueryNot getMyQueryElem() {
    return (QueryNot) queryElem;
  }

  @Override
  public void generateSQL(QuerySQLGenHelper genHelper) throws QueryException {
    if (getMyQueryElem().getChild() != null) {
      SQLQueryStructureElem sqlChild = SQLQueryElemFactory.generateSQLElem(getMyQueryElem()
              .getChild());
      sqlChild.generateSQL(genHelper);
    }
  }

}
