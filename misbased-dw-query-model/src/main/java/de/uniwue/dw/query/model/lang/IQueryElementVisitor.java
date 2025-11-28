package de.uniwue.dw.query.model.lang;

import de.uniwue.dw.query.model.exception.QueryException;

public interface IQueryElementVisitor<T> {

  T visit(QueryAttribute queryElem) throws QueryException;

  T visit(QueryTempOpAbs queryElem) throws QueryException;

  T visit(QueryValueCompare queryElem) throws QueryException;

  T visit(QueryTempOpRel queryElem) throws QueryException;

  T visit(QuerySubQuery queryElem) throws QueryException;

  T visit(QueryStatisticColumn queryElem) throws QueryException;

  T visit(QueryStatisticFilter queryElem) throws QueryException;

  T visit(QueryStatisticRow queryElem) throws QueryException;

  T visit(QueryNot queryElem) throws QueryException;

  T visit(QueryNTrue queryElem) throws QueryException;

  T visit(QueryOr queryElem) throws QueryException;

  T visit(QueryTempFilter queryElem) throws QueryException;

  T visit(QueryAnd queryElem) throws QueryException;

  T visit(QueryRoot queryElem) throws QueryException;

  T visit(QueryIDFilter queryElem) throws QueryException;

}
