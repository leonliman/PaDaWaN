package de.uniwue.dw.query.model.lang;

import de.uniwue.dw.query.model.exception.QueryException;

public class QueryStatisticRow extends QueryStructureContainingElem {

  public QueryStatisticRow(QueryStructureContainingElem aContainer) {
    super(aContainer);
  }

  @Override
  public String getXMLName() {
    return "DistributionRow";
  }

  @Override
  public <T> T accept(IQueryElementVisitor<T> visitor) throws QueryException {
    return visitor.visit(this);
  }

}
