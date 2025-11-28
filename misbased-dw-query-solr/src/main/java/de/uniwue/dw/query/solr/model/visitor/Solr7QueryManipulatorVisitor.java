package de.uniwue.dw.query.solr.model.visitor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.exception.QueryStructureException;
import de.uniwue.dw.query.model.lang.IQueryElementVisitor;
import de.uniwue.dw.query.model.lang.QueryAnd;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryElem;
import de.uniwue.dw.query.model.lang.QueryIDFilter;
import de.uniwue.dw.query.model.lang.QueryNTrue;
import de.uniwue.dw.query.model.lang.QueryNot;
import de.uniwue.dw.query.model.lang.QueryOr;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.lang.QueryStatisticColumn;
import de.uniwue.dw.query.model.lang.QueryStatisticFilter;
import de.uniwue.dw.query.model.lang.QueryStatisticRow;
import de.uniwue.dw.query.model.lang.QueryStructureContainingElem;
import de.uniwue.dw.query.model.lang.QueryStructureElem;
import de.uniwue.dw.query.model.lang.QuerySubQuery;
import de.uniwue.dw.query.model.lang.QueryTempFilter;
import de.uniwue.dw.query.model.lang.QueryTempOpAbs;
import de.uniwue.dw.query.model.lang.QueryTempOpRel;
import de.uniwue.dw.query.model.lang.QueryValueCompare;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;

public class Solr7QueryManipulatorVisitor implements IQueryElementVisitor<Void> {

  protected Void visitChilds(QueryStructureContainingElem queryElem) throws QueryException {
    for (QueryStructureElem child : queryElem.getChildren()) {
      child.accept(this);
    }
    return null;
  }

  private static boolean ancestorIsGroupFilter(QueryStructureElem queryElem) {
    return queryElem.getAncestorIDFilters().stream().anyMatch(n -> n.getFilterIDType() == FilterIDType.GROUP);
  }

  @Override
  public Void visit(QueryAttribute queryElem) throws QueryException {
    return null;
  }

  @Override
  public Void visit(QueryTempOpAbs queryElem) throws QueryException {
    return null;
  }

  @Override
  public Void visit(QueryValueCompare queryElem) throws QueryException {
    return null;
  }

  @Override
  public Void visit(QueryTempOpRel queryElem) throws QueryException {
    return null;
  }

  @Override
  public Void visit(QuerySubQuery queryElem) throws QueryException {
    return null;
  }

  @Override
  public Void visit(QueryStatisticColumn queryElem) throws QueryException {
    return visitChilds(queryElem);
  }

  @Override
  public Void visit(QueryStatisticFilter queryElem) throws QueryException {
    return visitChilds(queryElem);
  }

  @Override
  public Void visit(QueryStatisticRow queryElem) throws QueryException {
    return visitChilds(queryElem);
  }

  @Override
  public Void visit(QueryNot queryElem) throws QueryException {
    return visitChilds(queryElem);
  }

  @Override
  public Void visit(QueryNTrue queryElem) throws QueryException {
    return visitChilds(queryElem);
  }

  @Override
  public Void visit(QueryOr queryElem) throws QueryException {
    return visitChilds(queryElem);
  }

  @Override
  public Void visit(QueryTempFilter queryElem) throws QueryException {
    return visitChilds(queryElem);
  }

  @Override
  public Void visit(QueryAnd and) throws QueryException {
    if (!ancestorIsGroupFilter(and)) {
      QueryStructureContainingElem parent = (QueryStructureContainingElem) and.getParent();
      List<QueryStructureElem> children = and.getChildren();
      parent.removeChild(and);
      QueryOr or = new QueryOr(parent);
      for (QueryStructureElem child : children) {
        and.removeChild(child);
        or.addChild(child);
        child.setParent(or);
      }
      return visitChilds(or);
    }
    return visitChilds(and);
  }

  @Override
  public Void visit(QueryRoot queryElem) throws QueryException {
    return visitChilds(queryElem);
  }

  @Override
  public Void visit(QueryIDFilter queryElem) throws QueryException {
    return visitChilds(queryElem);
  }

}
