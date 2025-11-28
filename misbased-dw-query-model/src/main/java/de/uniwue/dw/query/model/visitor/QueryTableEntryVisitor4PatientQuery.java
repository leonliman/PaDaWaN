package de.uniwue.dw.query.model.visitor;

import java.util.ArrayList;
import java.util.List;

import de.uniwue.dw.query.model.data.RawQuery;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.IQueryElementVisitor;
import de.uniwue.dw.query.model.lang.QueryAnd;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryIDFilter;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;
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
import de.uniwue.dw.query.model.manager.IQueryIOManager;
import de.uniwue.dw.query.model.table.QueryTableEntry;
import de.uniwue.dw.query.model.table.QueryTableEntry.Position;

public class QueryTableEntryVisitor4PatientQuery
        implements IQueryElementVisitor<List<QueryTableEntry>> {

  private int group = 1;

  private IQueryIOManager queryIOManager;

  public QueryTableEntryVisitor4PatientQuery(IQueryIOManager queryIOManager) {
    this.queryIOManager = queryIOManager;
  }

  protected List<QueryTableEntry> visitChilds(QueryStructureContainingElem queryElem)
          throws QueryException {
    List<QueryTableEntry> result = new ArrayList<>();
    for (QueryStructureElem child : queryElem.getChildren()) {
      List<QueryTableEntry> tableEntries = child.accept(this);
      result.addAll(tableEntries);
    }
    return result;
  }

  private QueryTableEntry createQueryViewEntry(QueryStructureElem elem) {
    if (elem instanceof QueryAttribute)
      return new QueryTableEntry((QueryAttribute) elem);
    else if (elem instanceof QuerySubQuery) {
      QuerySubQuery a = (QuerySubQuery) elem;
      RawQuery rawQuery = queryIOManager.getQuery(a.name);
      return new QueryTableEntry(rawQuery);
    }
    return null;
  }

  @Override
  public List<QueryTableEntry> visit(QueryAttribute queryElem) {
    List<QueryTableEntry> entries = new ArrayList<>();
    QueryTableEntry entry = createQueryViewEntry(queryElem);
    entries.add(entry);
    return entries;
  }

  @Override
  public List<QueryTableEntry> visit(QuerySubQuery queryElem) {
    List<QueryTableEntry> entries = new ArrayList<>();
    QueryTableEntry entry = createQueryViewEntry(queryElem);
    entries.add(entry);
    return entries;
  }

  @Override
  public List<QueryTableEntry> visit(QueryStatisticColumn queryElem) throws QueryException {
    List<QueryTableEntry> childs = visitChilds(queryElem);
    for (QueryTableEntry child : childs) {
      child.setPosition(Position.column);
    }
    return childs;
  }

  @Override
  public List<QueryTableEntry> visit(QueryStatisticFilter queryElem) throws QueryException {
    List<QueryTableEntry> childs = visitChilds(queryElem);
    for (QueryTableEntry child : childs) {
      child.setPosition(Position.filter);
    }
    return childs;
  }

  @Override
  public List<QueryTableEntry> visit(QueryStatisticRow queryElem) throws QueryException {
    List<QueryTableEntry> childs = visitChilds(queryElem);
    for (QueryTableEntry child : childs) {
      child.setPosition(Position.row);
    }
    return childs;
  }

  @Override
  public List<QueryTableEntry> visit(QueryNot queryElem) {
    throw new IllegalStateException("Unsupported Operation");
  }

  @Override
  public List<QueryTableEntry> visit(QueryNTrue queryElem) {
    throw new IllegalStateException("Unsupported Operation");
  }

  @Override
  public List<QueryTableEntry> visit(QueryOr queryElem) throws QueryException {
    List<QueryTableEntry> childs = visitChilds(queryElem);
    for (QueryTableEntry child : childs) {
      child.setOrGroup(group + "");
    }
    group++;
    return childs;
  }

  @Override
  public List<QueryTableEntry> visit(QueryTempFilter queryElem) {
    throw new IllegalStateException("Unsupported Operation");
  }

  @Override
  public List<QueryTableEntry> visit(QueryAnd queryElem) throws QueryException {
    List<QueryTableEntry> childs = visitChilds(queryElem);
    return childs;
  }

  @Override
  public List<QueryTableEntry> visit(QueryRoot queryElem) throws QueryException {
    return visitChilds(queryElem);
  }

  @Override
  public List<QueryTableEntry> visit(QueryIDFilter queryElem) throws QueryException {
    List<QueryTableEntry> childs = visitChilds(queryElem);
    for (QueryTableEntry child : childs) {
      // if (queryElem.getFilterIDType() == FilterIDType.CaseID) {
      // if (child.getCaseGroup().isEmpty()) {
      // child.setCaseGroup(group + "");
      // }
      // } else
      if (queryElem.getFilterIDType() == FilterIDType.DocID) {
        if (child.getDocGroup().isEmpty()) {
          child.setDocGroup(group + "");
        }
      }
    }
    group++;
    return childs;
  }

  @Override
  public List<QueryTableEntry> visit(QueryTempOpAbs queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<QueryTableEntry> visit(QueryTempOpRel queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<QueryTableEntry> visit(QueryValueCompare queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

}