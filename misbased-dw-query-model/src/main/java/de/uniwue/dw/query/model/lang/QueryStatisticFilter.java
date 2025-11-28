package de.uniwue.dw.query.model.lang;

import de.uniwue.dw.query.model.exception.QueryException;

public class QueryStatisticFilter extends QueryAnd {

  public QueryStatisticFilter(QueryStructureContainingElem aContainer) {
    super(aContainer);
  }

  @Override
  public String getXMLName() {
    return "DistributionFilter";
  }

  @Override
  public <T> T accept(IQueryElementVisitor<T> visitor) throws QueryException {
    return visitor.visit(this);
  }

}
