package de.uniwue.dw.query.solr.model.visitor;

import java.util.ArrayList;
import java.util.List;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryAnd;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryIDFilter;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.lang.QueryStructureElem;
import de.uniwue.misc.util.StringUtilsUniWue;

public class Solr7ChildQueryStringVisitor extends Solr7ParentQueryStringVisitor {

  public Solr7ChildQueryStringVisitor(DOC_TYPE docType) {
    super(docType);
  }

  @Override
  public String visit(QueryAttribute queryElem) throws QueryException {
    String queryString = createAttributeQueryString(queryElem, true, false);
    return queryString;
  }

  @Override
  public String visit(QueryIDFilter queryElem) throws QueryException {
    buildIDFilterString(queryElem);

    if (queryElem.getFilterIDType() == FilterIDType.GROUP)
      return visitQueryAnd(queryElem);
    else
      return visitQueryOr(queryElem);
  }

  @Override
  public String visit(QueryAnd queryElem) throws QueryException {
    if (queryElem.isOptional())
      return "";
    if (ancestorIsGroupFilter(queryElem))
      return visitQueryAnd(queryElem);
    else
      return visitQueryOr(queryElem);
  }

  @Override
  public String visit(QueryRoot queryElem) throws QueryException {
    return visitQueryOr(queryElem);
  }

  @Override
  protected String handleOptionalQueryAttribute(QueryAttribute queryElem) throws QueryException {
    return createAttributeQueryString(queryElem, true, false);
  }

  @Override
  protected String visitQueryAnd(QueryAnd queryElem) throws QueryException {
    List<String> list = new ArrayList<>();
    for (QueryStructureElem qElem : queryElem.getChildren()) {
      String subQueryText = qElem.accept(this);
      if (subQueryText != null && !subQueryText.isEmpty()) {
        list.add(subQueryText);
      }
    }
    String result;
    if (queryElem.getAncestorIDFiltersWithType(FilterIDType.GROUP).size() > 0) {
      result = StringUtilsUniWue.concat(list, " AND ");
    } else {
      result = StringUtilsUniWue.concat(list, " OR ");
    }
    if (!result.isEmpty())
      result = "(" + result + ")";
    return result;
  }

}
