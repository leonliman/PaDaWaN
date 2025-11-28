package de.uniwue.dw.query.model.lang;

import de.uniwue.dw.query.model.exception.QueryException;

public class QueryStatisticColumn extends QueryStructureContainingElem {

  public QueryStatisticColumn(QueryStructureContainingElem aContainer) {
    super(aContainer);
  }

  @Override
  public String getXMLName() {
    return "DistributionColumn";
  }

  @Override
  public <T> T accept(IQueryElementVisitor<T> visitor) throws QueryException {
    return visitor.visit(this);
  }

}
