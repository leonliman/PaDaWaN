package de.uniwue.dw.query.model.lang;

import java.util.ArrayList;
import java.util.List;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.manager.QueryManipulationManager;

public class QueryNot extends QueryStructureContainingElem {

  public QueryNot(QueryStructureContainingElem aContainer) throws QueryException {
    super(aContainer);
  }

  public QueryNot(QueryStructureContainingElem aContainer, int position) throws QueryException {
    super(aContainer, position);
  }

  public QueryStructureElem getChild() {
    if (getChildren().size() > 0) {
      return getChildren().get(0);
    } else {
      return null;
    }
  }

  // Attributes within NOTs are never displayed and therefore are not returned by this recursive
  // fetching
  @Override
  public List<QueryAttribute> getAttributesRecursive() {
    return new ArrayList<QueryAttribute>();
  }

  @Override
  public void addChild(QueryStructureElem aChild) throws QueryException {
    if (getChildren().size() > 0) {
      throw new QueryException("Should not happen. Nots should only have one child.");
    }
    super.addChild(aChild);
  }

  @Override
  public boolean canContainElements() {
    return getChildren().size() < 1;
  }

  @Override
  public String getXMLName() {
    return "Not";
  }

  @Override
  public <T> T accept(IQueryElementVisitor<T> visitor) throws QueryException {
    return visitor.visit(this);
  }

  @Override
  public void shrink(QueryManipulationManager manager) throws QueryException {
    super.shrink(manager);
    if (getChildren().size() == 0) {
      getContainer().removeChild(this);
    }
  }
}
