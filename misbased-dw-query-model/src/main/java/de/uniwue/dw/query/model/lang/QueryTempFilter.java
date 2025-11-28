package de.uniwue.dw.query.model.lang;

import de.uniwue.dw.query.model.exception.QueryException;

public class QueryTempFilter extends QueryStructureContainingElem {

  public QueryTempOpAbs tempOp;

  public QueryTempFilter(QueryStructureContainingElem aContainer) throws QueryException {
    super(aContainer);
  }

  @Override
  public String getXMLName() {
    return "QueryFilter";
  }

  @Override
  public <T> T accept(IQueryElementVisitor<T> visitor) throws QueryException {
    return visitor.visit(this);
  }
}
