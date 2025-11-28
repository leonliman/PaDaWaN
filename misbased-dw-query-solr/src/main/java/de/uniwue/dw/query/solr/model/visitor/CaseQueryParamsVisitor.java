package de.uniwue.dw.query.solr.model.visitor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.ContentOperator;
import de.uniwue.dw.query.model.lang.IQueryElementVisitor;
import de.uniwue.dw.query.model.lang.QueryAnd;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryElem;
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
import de.uniwue.dw.query.model.lang.QuerySubQuery;
import de.uniwue.dw.query.model.lang.QueryTempFilter;
import de.uniwue.dw.query.model.lang.QueryTempOpAbs;
import de.uniwue.dw.query.model.lang.QueryTempOpRel;
import de.uniwue.dw.query.model.lang.QueryValueCompare;
import de.uniwue.dw.query.model.lang.ReductionOperator;
import de.uniwue.dw.query.model.result.Cell.CellType;
import de.uniwue.dw.query.solr.SolrUtil;
import de.uniwue.dw.query.solr.client.ISolrConstants;

public class CaseQueryParamsVisitor implements IQueryElementVisitor<Void> {

  private static final Logger logger = LogManager.getLogger(CaseQueryParamsVisitor.class);

  protected SolrQuery query;

  public CaseQueryParamsVisitor() {
    this(new SolrQuery());
  }

  public CaseQueryParamsVisitor(String queryString) {
    this(new SolrQuery(queryString));
  }

  public CaseQueryParamsVisitor(SolrQuery query) {
    this.query = query;
  }

  private void addField(QueryAttribute queryElem, CatalogEntry anEntry,
          ContentOperator contentOperator, boolean hasDocIDFilter) {
    String solrDisplayFieldName;
    if (queryElem.displayCaseID() || queryElem.displayDocID() || queryElem.displayInfoDate()
            || hasDocIDFilter) {
      solrDisplayFieldName = SolrUtil.getSolrFieldName(anEntry);
    } else {
      solrDisplayFieldName = SolrUtil.getSolrDisplayFieldName(queryElem);
    }
    query.addField(solrDisplayFieldName);
    if (queryElem.displayDocID() || hasDocIDFilter) {
      String solrDocIDFieldName = SolrUtil.getSolrFieldName(anEntry, CellType.DocID);
      query.addField(solrDocIDFieldName);
    }
    if (queryElem.displayInfoDate() || (queryElem.getReferencingTempRelOps().size() > 0)
            || (queryElem.getReductionOperator() == ReductionOperator.EARLIEST)
            || (queryElem.getReductionOperator() == ReductionOperator.LATEST)) {
      String solrMeasureTimeFieldName = SolrUtil.getSolrFieldName(anEntry, CellType.MeasureTime);
      query.addField(solrMeasureTimeFieldName);
    }
    if (anEntry.getDataType() == CatalogEntryType.Text && contentOperator.isQueryWordOperator()) {
      if (contentOperator == ContentOperator.CONTAINS_POSITIVE)
        query.addField(SolrUtil.getSolrFieldNameForPositiveTextSegments(anEntry));
    }
  }

  @Override
  public Void visit(QueryAttribute queryElem) throws QueryException {
    if (queryElem.displayValue() || (queryElem.getReferencingTempRelOps().size() > 0)
            || (queryElem.getTemporalOpsRel().size() > 0)) {
      boolean hasDocIDFilter = false;
      for (QueryIDFilter aFilter : queryElem.getAncestorIDFilters()) {
        if (aFilter.getFilterIDType() == FilterIDType.DocID) {
          hasDocIDFilter = true;
        }
      }
      addField(queryElem, queryElem.getCatalogEntry(), queryElem.getContentOperator(),
              hasDocIDFilter);
      if (queryElem.getCatalogEntry().getDataType() == CatalogEntryType.Bool
              || queryElem.getCatalogEntry().getDataType() == CatalogEntryType.Structure) {
        addField(queryElem, queryElem.getCatalogEntry(), queryElem.getContentOperator(),
                hasDocIDFilter);
        if (queryElem.displayCaseID() || queryElem.displayDocID() || queryElem.displayInfoDate()
                || hasDocIDFilter) {
          for (CatalogEntry aSibling : queryElem.getCatalogEntry().getDescendants()) {
            addField(queryElem, aSibling, queryElem.getContentOperator(), hasDocIDFilter);
          }
        }
      }
    }
    for (QueryTempOpAbs anOp : queryElem.getTempOpsAbs()) {
      visit(anOp);
    }
    for (QueryTempOpRel anOp : queryElem.getTemporalOpsRel()) {
      visit(anOp);
    }
    for (QueryValueCompare anOp : queryElem.getValueCompares()) {
      visit(anOp);
    }
    return null;
  }

  @Override
  public Void visit(QuerySubQuery queryElem) {
    return null;
  }

  @Override
  public Void visit(QueryStatisticColumn queryElem) throws QueryException {
    visitChilds(queryElem);
    return null;
  }

  @Override
  public Void visit(QueryStatisticFilter queryElem) throws QueryException {
    visitChilds(queryElem);
    return null;
  }

  @Override
  public Void visit(QueryStatisticRow queryElem) throws QueryException {
    visitChilds(queryElem);
    return null;
  }

  @Override
  public Void visit(QueryNot queryElem) throws QueryException {
    visitChilds(queryElem);
    return null;
  }

  @Override
  public Void visit(QueryNTrue queryElem) throws QueryException {
    visitChilds(queryElem);
    return null;
  }

  @Override
  public Void visit(QueryOr queryElem) throws QueryException {
    visitChilds(queryElem);
    return null;
  }

  @Override
  public Void visit(QueryTempFilter queryElem) throws QueryException {
    visitChilds(queryElem);
    return null;
  }

  @Override
  public Void visit(QueryAnd queryElem) throws QueryException {
    visitChilds(queryElem);
    return null;
  }

  @Override
  public Void visit(QueryRoot queryElem) throws QueryException {
    query.setParam("q.op", "AND");
    if (queryElem.getLimitResult() == 0) {
      query.setRows(ISolrConstants.MAX_ROWS);
    } else {
      query.setRows(queryElem.getLimitResult());
    }
    query.addField("id");
    query.addField("patient");
    visitChilds(queryElem);
    return null;
  }

  @Override
  public Void visit(QueryIDFilter queryElem) throws QueryException {
    visitChilds(queryElem);
    return null;
  }

  private void visitChilds(QueryStructureContainingElem queryElem) throws QueryException {
    for (QueryElem child : queryElem.getChildren())
      child.accept(this);
  }

  public SolrQuery getSolrQuery() {
    return query;
  }

  @Override
  public Void visit(QueryTempOpAbs queryElem) throws QueryException {
    QueryAttribute attr = (QueryAttribute) queryElem.getParent();
    String solrRefIDFieldName = SolrUtil.getSolrFieldName(attr.getCatalogEntry(),
            CellType.MeasureTime);
    query.addField(solrRefIDFieldName);
    return null;
  }

  @Override
  public Void visit(QueryTempOpRel queryElem) throws QueryException {
    QueryAttribute attr = (QueryAttribute) queryElem.getParent();
    String solrRefIDFieldName = SolrUtil.getSolrFieldName(attr.getCatalogEntry(),
            CellType.MeasureTime);
    query.addField(solrRefIDFieldName);
    return null;
  }

  @Override
  public Void visit(QueryValueCompare queryElem) throws QueryException {
    QueryAttribute attr = (QueryAttribute) queryElem.getParent();
    String solrRefIDFieldName = SolrUtil.getSolrFieldName(attr.getCatalogEntry(), CellType.Value);
    query.addField(solrRefIDFieldName);
    return null;
  }

}
