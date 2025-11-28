package de.uniwue.dw.query.sql;

import de.uniwue.dw.query.model.lang.QueryAnd;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryElem;
import de.uniwue.dw.query.model.lang.QueryIDFilter;
import de.uniwue.dw.query.model.lang.QueryNot;
import de.uniwue.dw.query.model.lang.QueryOr;
import de.uniwue.dw.query.model.lang.QueryTempFilter;

public class SQLQueryElemFactory {

  public static SQLQueryStructureElem generateSQLElem(QueryElem anElem) {
    if (anElem instanceof QueryAttribute) {
      return new SQLQueryAttribute((QueryAttribute) anElem);
    } else if (anElem instanceof QueryIDFilter) {
      return new SQLQueryIDFilter((QueryIDFilter) anElem);
    } else if (anElem instanceof QueryNot) {
      return new SQLQueryNot((QueryNot) anElem);
    } else if (anElem instanceof QueryOr) {
      return new SQLQueryOr((QueryOr) anElem);
    } else if (anElem instanceof QueryTempFilter) {
      return new SQLQueryTempFilter((QueryTempFilter) anElem);
    } else if (anElem instanceof QueryAnd) {
      return new SQLQueryAnd((QueryAnd) anElem);
    } else {
      @SuppressWarnings("unused") int x = 1 / 0;
      return null;
    }
  }

}
