package de.uniwue.dw.query.model.lang;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.manager.IQueryIOManager;
import de.uniwue.dw.query.model.manager.QueryManipulationManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public abstract class QueryStructureContainingElem extends QueryStructureElem {

  private List<QueryStructureElem> children = new ArrayList<QueryStructureElem>();

  public QueryStructureContainingElem(QueryStructureContainingElem aContainer) {
    super(aContainer);
  }

  public QueryStructureContainingElem(QueryStructureContainingElem aContainer, int position) {
    super(aContainer, position);
  }

  public QueryElem getElem(String aRefId) throws QueryException {
    return getRoot().getElem(aRefId);
  }

  public QueryElem getElem(long anId) throws QueryException {
    return getRoot().getElem(anId);
  }

  @Override
  public List<QueryStructureElem> getChildren() {
    List<QueryStructureElem> result = new ArrayList<QueryStructureElem>();
    result.addAll(children);
    return result;
  }

  @Override
  public List<QueryStructureElem> getSiblings() {
    List<QueryStructureElem> result = new ArrayList<QueryStructureElem>();
    for (QueryStructureElem aChild : children) {
      result.add(aChild);
      List<QueryStructureElem> tmp = aChild.getSiblings();
      result.addAll(tmp);
    }
    return result;
  }

  @Override
  public boolean canContainElements() {
    return true;
  }

  @Override
  public boolean displayColumns() {
    for (QueryStructureElem aChild : getChildren()) {
      if (aChild.displayColumns()) {
        return true;
      }
    }
    return super.displayColumns();
  }

  @Override
  public List<QueryAttribute> getAttributesRecursive() {
    List<QueryAttribute> result = new ArrayList<QueryAttribute>();
    for (QueryStructureElem aChild : children) {
      List<QueryAttribute> tmp = aChild.getAttributesRecursive();
      result.addAll(tmp);
    }
    return result;
  }

  @Override
  public List<QueryStructureElem> getReducingElemsRecursive() {
    List<QueryStructureElem> result = new ArrayList<QueryStructureElem>();
    for (QueryStructureElem aChild : children) {
      List<QueryStructureElem> tmp = aChild.getReducingElemsRecursive();
      result.addAll(tmp);
    }
    return result;
  }

  public List<QueryAttribute> getAttributes() {
    List<QueryAttribute> result = new ArrayList<QueryAttribute>();
    for (QueryStructureElem aChild : children) {
      if (aChild instanceof QueryAttribute) {
        result.add((QueryAttribute) aChild);
      }
    }
    return result;
  }

  public List<QueryOr> getOrs() {
    List<QueryOr> result = new ArrayList<QueryOr>();
    for (QueryStructureElem aChild : children) {
      if (aChild instanceof QueryOr) {
        result.add((QueryOr) aChild);
      }
    }
    return result;
  }

  public List<QueryOr> getOrsRecursive() {
    List<QueryOr> result = new ArrayList<QueryOr>();
    for (QueryStructureElem aChild : children) {
      if (aChild instanceof QueryOr) {
        result.add((QueryOr) aChild);
      }
      if (aChild instanceof QueryStructureContainingElem) {
        result.addAll(((QueryStructureContainingElem) aChild).getOrsRecursive());
      }
    }
    return result;
  }

  public List<QueryIDFilter> getIDFilter() {
    List<QueryIDFilter> result = new ArrayList<QueryIDFilter>();
    for (QueryStructureElem aChild : children) {
      if (aChild instanceof QueryIDFilter) {
        result.add((QueryIDFilter) aChild);
      }
    }
    return result;
  }

  @Override
  public List<QueryIDFilter> getIDFilterRecursive() {
    List<QueryIDFilter> result = new ArrayList<QueryIDFilter>();
    for (QueryStructureElem aChild : children) {
      List<QueryIDFilter> tmp = aChild.getIDFilterRecursive();
      result.addAll(tmp);
    }
    return result;
  }

  public void addAllChildren(List<QueryStructureElem> children) throws QueryException {
    for (QueryStructureElem child : children)
      addChild(child);
  }

  public void addChild(QueryStructureElem aChild) throws QueryException {
    if (aChild != null) {
      children.add(aChild);
      aChild.setParent(this);
      aChild.setContainer(this);
    }
  }

  public void addChild(QueryStructureElem aChild, int position) {
    children.add(position, aChild);
    aChild.setParent(this);
    aChild.setContainer(this);
  }

  public void removeChild(QueryStructureElem aChild) {
    children.remove(aChild);
    aChild.setParent(null);
    aChild.setContainer(null);
  }

  public void removeAllChildren() {
    for (QueryStructureElem child : children) {
      child.setParent(null);
      child.setContainer(null);
    }
    children.clear();
  }

  @Override
  public void shrink(QueryManipulationManager manager) throws QueryException {
    super.shrink(manager);
    for (QueryStructureElem aChild : getChildren()) {
      aChild.shrink(manager);
    }
  }

  @Override
  public void removeInactiveChildren() {
    List<QueryStructureElem> childrenToRemove = new ArrayList<>();
    for (QueryStructureElem aChild : children) {
      if (!aChild.active) {
        childrenToRemove.add(aChild);
      } else {
        aChild.removeInactiveChildren();
      }
    }
    for (QueryStructureElem aChild : childrenToRemove)
      removeChild(aChild);
  }

  // QueryAttributes have to appear before logical operators,
  // QueryAttributes which are referenced with relative temporal relations
  // by other QueryAttributes have to appear before those others,
  // optional QueryAttributes have to appear behind non optional ones
  @Override
  public void sortChildrenForQuery() {
    Collections.sort(children, new Comparator<QueryStructureElem>() {
      @Override
      public int compare(QueryStructureElem o1, QueryStructureElem o2) {
        if ((o1 instanceof QueryAttribute) && !(o2 instanceof QueryAttribute)) {
          return -1;
        }
        if (!(o1 instanceof QueryAttribute) && (o2 instanceof QueryAttribute)) {
          return 1;
        }
        if ((o1 instanceof QueryAttribute) && (o2 instanceof QueryAttribute)) {
          QueryAttribute attr1 = (QueryAttribute) o1;
          QueryAttribute attr2 = (QueryAttribute) o2;
          // the referenced elements have to appear before the referencing elements
          for (QueryTempOpRel anOp : attr2.getTemporalOpsRel()) {
            if (anOp.getRefElem() == attr1) {
              return -1;
            }
          }
          for (QueryTempOpRel anOp : attr1.getTemporalOpsRel()) {
            if (anOp.getRefElem() == attr2) {
              return 1;
            }
          }
          // non optional attributes have to appear before optional ones
          if (!attr1.optional && attr2.optional) {
            return -1;
          }
          if (attr1.optional && !attr2.optional) {
            return 1;
          }
        }
        return 0;
      }
    });
    for (QueryStructureElem aChild : children) {
      aChild.sortChildrenForQuery();
    }
  }

  @Override
  public void initialize() {
    for (QueryStructureElem aChild : getChildren()) {
      aChild.initialize();
    }
  }

  @Override
  public void expandSubQueries(QueryManipulationManager manager, IQueryIOManager allQueriesManager)
          throws QueryException {
    for (QueryStructureElem aChild : getChildren()) {
      aChild.expandSubQueries(manager, allQueriesManager);
    }
  }

}
