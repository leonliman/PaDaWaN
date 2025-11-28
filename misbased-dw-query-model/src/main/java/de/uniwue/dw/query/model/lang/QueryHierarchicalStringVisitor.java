package de.uniwue.dw.query.model.lang;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;

public class QueryHierarchicalStringVisitor implements IQueryElementVisitor<String> {

  @Override
  public String visit(QueryAttribute queryElem) {
    String result = queryElem.toString();
    StringBuilder sb = new StringBuilder();
    sb.append(QueryAttribute.class.getSimpleName()+" ");
    sb.append(result);
    for (QueryTempOpAbs aTempOp : queryElem.getTempOpsAbs()) {
      sb.append(aTempOp.toString() + " ");
    }
    for (QueryTempOpRel aTempOp : queryElem.getTemporalOpsRel()) {
      sb.append(aTempOp.toString() + " ");
    }
    for (QueryValueCompare anOp : queryElem.getValueCompares()) {
      sb.append(anOp.toString() + " ");
    }
    return sb.toString();
  }

  @Override
  public String visit(QuerySubQuery queryElem) {
    return queryElem.toString();

  }

  @Override
  public String visit(QueryStatisticColumn queryElem) throws QueryException {
    StringBuilder sb = new StringBuilder();
    sb.append("QueryDistributionColumn\n");
    sb.append(handleQueryStructureContainingElem(queryElem));
    return sb.toString();
  }

  @Override
  public String visit(QueryStatisticFilter queryElem) throws QueryException {
    StringBuilder sb = new StringBuilder();
    sb.append("QueryDistributionFilter\n");
    sb.append(handleQueryStructureContainingElem(queryElem));
    return sb.toString();
  }

  @Override
  public String visit(QueryStatisticRow queryElem) throws QueryException {
    StringBuilder sb = new StringBuilder();
    sb.append("QueryDistributionRow\n");
    sb.append(handleQueryStructureContainingElem(queryElem));
    return sb.toString();
  }

  @Override
  public String visit(QueryNot queryElem) throws QueryException {
    StringBuilder sb = new StringBuilder();
    sb.append("QueryNot\n");
    sb.append(handleQueryStructureContainingElem(queryElem));
    return sb.toString();
  }

  @Override
  public String visit(QueryNTrue queryElem) throws QueryException {
    StringBuilder sb = new StringBuilder();
    sb.append("QueryNTrue\n");
    sb.append(handleQueryStructureContainingElem(queryElem));
    return sb.toString();
  }

  @Override
  public String visit(QueryOr queryElem) throws QueryException {
    StringBuilder sb = new StringBuilder();
    sb.append("QueryOr\n");
    sb.append(handleQueryStructureContainingElem(queryElem));
    return sb.toString();
  }

  @Override
  public String visit(QueryTempFilter queryElem) throws QueryException {
    StringBuilder sb = new StringBuilder();
    sb.append("QueryTempFilter\n");
    sb.append(handleQueryStructureContainingElem(queryElem));
    return sb.toString();
  }

  @Override
  public String visit(QueryAnd queryElem) throws QueryException {
    StringBuilder sb = new StringBuilder();
    sb.append("QueryAnd\n");
    sb.append(handleQueryStructureContainingElem(queryElem));
    return sb.toString();
  }

  @Override
  public String visit(QueryRoot queryElem) throws QueryException {
    StringBuilder sb = new StringBuilder();
    sb.append("QueryRoot\n");
    sb.append(handleQueryStructureContainingElem(queryElem));
    return sb.toString();
  }

  private String handleQueryStructureContainingElem(QueryStructureContainingElem queryElem)
          throws QueryException {
    StringBuilder sb = new StringBuilder();
    for (QueryStructureElem child : queryElem.getChildren()) {
      String childText = child.accept(this);
      if (childText != null) {
        String[] split = childText.split("\n");
        for (String line : split)
          sb.append("  " + line + "\n");
      }
    }
    return sb.toString();
  }

  @Override
  public String visit(QueryIDFilter queryElem) throws QueryException {
    StringBuilder sb = new StringBuilder();
    FilterIDType filterIDType = queryElem.getFilterIDType();
    String line = QueryIDFilter.class.getSimpleName() + " " + filterIDType;
    sb.append(line + "\n");
    sb.append(handleQueryStructureContainingElem(queryElem));
    return sb.toString();
  }

  @Override
  public String visit(QueryTempOpAbs queryElem) throws QueryException {
    StringBuilder sb = new StringBuilder();
    sb.append("QueryTempOpAbs\n");
    return sb.toString();
  }

  @Override
  public String visit(QueryTempOpRel queryElem) throws QueryException {
    StringBuilder sb = new StringBuilder();
    sb.append("QueryTempOpRel\n");
    return sb.toString();
  }

  @Override
  public String visit(QueryValueCompare queryElem) throws QueryException {
    StringBuilder sb = new StringBuilder();
    sb.append("QueryValueCompare\n");
    return sb.toString();
  }

}
