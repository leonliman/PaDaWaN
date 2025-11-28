package de.uniwue.dw.query.sql;

import de.uniwue.dw.query.model.lang.QueryStructureContainingElem;
import de.uniwue.dw.query.sql.util.QuerySQLGenHelper;

public class SQLQueryAggregate extends SQLQueryStructureContainingElem {

  public SQLQueryAggregate(QueryStructureContainingElem anElem) {
    super(anElem);
  }

  @Override
  public void generateSQL(QuerySQLGenHelper genHelper) {
  }

}
