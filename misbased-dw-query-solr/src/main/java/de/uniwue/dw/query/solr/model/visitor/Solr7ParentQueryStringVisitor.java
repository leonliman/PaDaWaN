package de.uniwue.dw.query.solr.model.visitor;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.exception.QueryStructureException;
import de.uniwue.dw.query.model.exception.QueryStructureException.QueryStructureExceptionType;
import de.uniwue.dw.query.model.lang.*;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;
import de.uniwue.dw.query.solr.SolrUtil;
import de.uniwue.dw.query.solr.client.ISolrConstants;
import de.uniwue.dw.query.solr.model.solrText.SolrQueryParser;
import de.uniwue.misc.util.StringUtilsUniWue;
import org.antlr.v4.runtime.tree.ParseTree;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;

import static de.uniwue.dw.core.model.data.CatalogEntryType.Number;
import static de.uniwue.dw.core.model.data.CatalogEntryType.*;
import static de.uniwue.dw.query.model.lang.ContentOperator.*;

public class Solr7ParentQueryStringVisitor implements IQueryElementVisitor<String>, ISolrConstants {
  public static final BigDecimal SMALL_VALUE_FOR_INTERVAL_BOUNDARY = new BigDecimal("0.0001");

  private final Map<String, String> params = new TreeMap<>();

  private final DOC_TYPE docType;

  private int paramIndex = 1;

  private String idFilterString;

  private Map<Long, String> idToTimeFilterStringMap;

  private boolean useIntermediateLayer;

  public Solr7ParentQueryStringVisitor(DOC_TYPE docType) {
    this.docType = docType;
  }

  protected static boolean ancestorIsGroupFilter(QueryStructureElem queryElem) {
    return queryElem.getAncestorIDFilters().stream()
            .anyMatch(n -> n.getFilterIDType() == FilterIDType.GROUP);
  }

  private static String getParentLayerQuery(boolean useIntermediateLayer) {
    return SOLR_FIELD_PARENT_CHILD_LAYER + ":" + (useIntermediateLayer ? PARENT_INTERMEDIATE_LAYER : PARENT_LAYER);
  }

  private static String getChildLayerQuery() {
    return SOLR_FIELD_PARENT_CHILD_LAYER + ":" + CHILD_LAYER;
  }

  public static String excludeIntervalBoundary(String value, boolean increment) {
    try {
      if (increment)
        return new BigDecimal(value).add(SMALL_VALUE_FOR_INTERVAL_BOUNDARY).toString();
      else
        return new BigDecimal(value).subtract(SMALL_VALUE_FOR_INTERVAL_BOUNDARY).toString();
    } catch (NumberFormatException e) {
      // TODO: bad hack
      return value;
    }
  }

  private static String getSolrFieldName(QueryAttribute queryElem) {
    return SolrUtil.getSolrFieldName(queryElem);
  }

  private boolean childElementIsNeeded(QueryAttribute anAttr) {
    if (anAttr.getRoot().isOnlyCount() && (anAttr.getContentOperator() == ContentOperator.EXISTS)
            && (anAttr.getReductionOperator() == ReductionOperator.NONE)) {
      return false;
    }
    return (anAttr.getContentOperator() != ContentOperator.EXISTS) || anAttr.displayCaseID()
            || anAttr.displayDocID() || anAttr.displayInfoDate() || anAttr.displayValue()
            || anAttr.getRoot().isDisplayPID();
  }

  @Override
  public String visit(QueryAttribute queryElem) throws QueryException {
    boolean ancestorIsGroupFilter = ancestorIsGroupFilter(queryElem);
    ContentOperator operator = queryElem.getContentOperator();
    boolean exclusion = operator == NOT_EXISTS || operator == ContentOperator.CONTAINS_NOT
            || operator == CONTAINS_NOT_POSITIVE;
    boolean ignoreNegated = !ancestorIsGroupFilter && exclusion;

    String queryString = createAttributeQueryString(queryElem, false, ignoreNegated);
    if (queryString == null || queryString.trim().isEmpty())
      return "";
    // when the attribute has to constraint its existence is as well indexed at the parent document,
    // so it does not need to be wrapped to the child layer
    if (childElementIsNeeded(queryElem)) {
      queryString = buildChildQuery(queryString, exclusion,
              queryElem.getParent().getClass().equals(QueryIDFilter.class) &&
                      ((QueryIDFilter) queryElem.getParent()).getFilterIDType()
                              .equals(FilterIDType.GROUP), useIntermediateLayer);
      if (exclusion) {
        String allUnits = buildChildQuery("", false, false, useIntermediateLayer);
        queryString = "(" + allUnits + " AND " + queryString + ")";
      }
    }
    return queryString;
  }

  protected String createAttributeQueryString(QueryAttribute queryElem, boolean forceNonOptional,
          boolean ignoreNegated) throws QueryException {
    String attrQuery;
    // handle optional flag
    if (queryElem.isOptional() && !forceNonOptional) {
      attrQuery = handleOptionalQueryAttribute(queryElem);
      // handle just-display-parent_shell
    } else if (queryElem.getContentOperator() == EMPTY) {
      attrQuery = "*:*";
      // handle filter unknown flag
    } else if (!queryElem.isFilterUnkown()) {
      attrQuery = handleFilterUnknownAttributesNotSet(queryElem);
      // handle text queries
    } else if (queryElem.getContentOperator() == CONTAINS
            || queryElem.getContentOperator() == CONTAINS_POSITIVE) {
      ParseTree desiredContentAsParseTree = queryElem.getDesiredContentAsParseTree();
      attrQuery = SolrQueryParser.parse(desiredContentAsParseTree, queryElem);
    } else if (queryElem.getContentOperator() == CONTAINS_NOT
            || queryElem.getContentOperator() == CONTAINS_NOT_POSITIVE) {
      if (ignoreNegated) {
        ParseTree desiredContentAsParseTree = queryElem.getDesiredContentAsParseTree();
        attrQuery = SolrQueryParser.parse(desiredContentAsParseTree, queryElem);
      } else {
        String fieldExistence = SolrUtil.FIELD_EXISTANCE_FIELD + ":"
                + SolrUtil.getSolrID(queryElem.getCatalogEntry());
        String tokenQuery = SolrQueryParser.parse(queryElem.getDesiredContentAsParseTree(),
                queryElem);
        attrQuery = "(" + fieldExistence + " AND -" + tokenQuery + ")";
      }
      // handle existence queries
    } else if (queryElem.getContentOperator() == EXISTS) {
      attrQuery = SolrUtil.FIELD_EXISTANCE_FIELD + ":"
              + SolrUtil.getSolrID(queryElem.getCatalogEntry());
    } else if (queryElem.getContentOperator() == NOT_EXISTS) {
      if (ignoreNegated) {
        attrQuery = SolrUtil.FIELD_EXISTANCE_FIELD + ":" +
                SolrUtil.getSolrID(queryElem.getCatalogEntry());
      } else {
        attrQuery = "(*:* !" + SolrUtil.FIELD_EXISTANCE_FIELD + ":" +
                SolrUtil.getSolrID(queryElem.getCatalogEntry())
                + ")";
      }
    } else {
      attrQuery = getSolrFieldName(queryElem) + ":" + getSolrFieldValue(queryElem);
    }
    if (!attrQuery.isEmpty() && idToTimeFilterStringMap == null) {
      attrQuery = addReductionOperator(queryElem, attrQuery);
    }
    return attrQuery;
  }

  private String addReductionOperator(QueryAttribute queryElem, String attrQuery) {
    String attachment = "";
    String catalogEntrySolrID = SolrUtil.getSolrID(queryElem.getCatalogEntry());
    switch (queryElem.getReductionOperator()) {
      case EARLIEST:
        attachment = SOLR_FIELD_FIRST_VALUE + ":" + catalogEntrySolrID;
        break;
      case LATEST:
        attachment = SOLR_FIELD_LAST_VALUE + ":" + catalogEntrySolrID;
        break;
      case MIN:
        attachment = SOLR_FIELD_MIN_VALUE + ":" + catalogEntrySolrID;
        break;
      case MAX:
        attachment = SOLR_FIELD_MAX_VALUE + ":" + catalogEntrySolrID;
        break;
      default:
        break;
    }
    if (!attachment.isEmpty()) {
      attrQuery = "(" + attrQuery + " AND " + attachment + ")";
    }
    return attrQuery;
  }

  private String buildChildQuery(String attributeQuery, boolean exclusion,
          boolean attributeIsInIDGroup, boolean useIntermediateLayer) {
    String param = "qp" + paramIndex++;
    String docTypeQuery = SolrUtil.createDocTypeQuery(docType);
    String childLayerQuery = getChildLayerQuery();
    String childQuery = childLayerQuery + " AND " + docTypeQuery;
    if (useIntermediateLayer) {
      // this is a workaround for the bitmask problem as described in
      // https://issues.apache.org/jira/browse/SOLR-14687
      // or with an example in https://lists.apache.org/thread/4ryv8r0owrojshnp6731wmn0kb18w90p
      childQuery = childQuery + " NOT " + SOLR_FIELD_DOC_ID + ":" + 0;
    }
    if (attributeQuery != null && !attributeQuery.isEmpty()) {
      if (attributeIsInIDGroup)
        childQuery = attributeQuery;
      else
        childQuery += " AND " + attributeQuery;
    }
    if (idFilterString != null && !idFilterString.isEmpty()) {
      childQuery = childQuery + " AND (" + idFilterString + ")";
    }
    if (!attributeIsInIDGroup)
      params.put(param, childQuery);

    String parentLayerQuery = getParentLayerQuery(useIntermediateLayer);
    String parentQuery = parentLayerQuery + " AND " + docTypeQuery;

    // String existence = "+";
    String existence = "";
    if (exclusion)
      existence = "-";
    String queryTemplate = existence + "{!parent which=\"" + parentQuery + "\" v=$%s}";
    String query;
    if (attributeIsInIDGroup)
      query = childQuery;
    else
      query = String.format(queryTemplate, param);
    return query;
  }

  protected String handleOptionalQueryAttribute(QueryAttribute queryElem) throws QueryException {
    return "";
  }

  private String handleFilterUnknownAttributesNotSet(QueryAttribute queryElem)
          throws QueryException {
    boolean origIsOptional = queryElem.isOptional();
    queryElem.setFilterUnkown(true);
    queryElem.setOptional(false);
    String notOptional = visit(queryElem);
    ContentOperator origOperator = queryElem.getContentOperator();
    queryElem.setContentOperator(NOT_EXISTS);
    String notExits = visit(queryElem);
    queryElem.setOptional(origIsOptional);
    queryElem.setContentOperator(origOperator);
    queryElem.setFilterUnkown(false);
    return "(" + notOptional + " OR " + notExits + ")";
  }

  private String getSolrFieldValue(QueryAttribute queryElem) throws QueryException {
    String result;
    String formattedArgument = queryElem.getDesiredContent();
    ContentOperator operator = queryElem.getContentOperator();
    if ((queryElem.getCatalogEntry().getDataType() == DateTime) && (operator != BETWEEN)
            && (operator != PER_YEAR)) {
      formattedArgument = SolrUtil.format2SolrDate(formattedArgument);
    }
    if ((operator == EXISTS) || (operator == NOT_EXISTS)) {
      result = "*";
    } else if ((operator == LESS) || (operator == LESS_OR_EQUAL)) {
      if ((queryElem.getCatalogEntry().getDataType() == Number) && (operator == LESS)) {
        formattedArgument = excludeIntervalBoundary(formattedArgument, false);
        // TODO: no boundary exclusion for DateTime with LESS !
      }
      result = "[* TO " + formattedArgument + "]";
    } else if (operator == EQUALS) {
      if (queryElem.getCatalogEntry().getDataType() == DateTime ||
              queryElem.getCatalogEntry().getDataType() == SingleChoice ||
              queryElem.getCatalogEntry().getDataType() == Text) {
        result = "\"" + formattedArgument + "\"";
      } else {
        result = formattedArgument;
      }
    } else if ((operator == MORE) || (operator == MORE_OR_EQUAL)) {
      if ((queryElem.getCatalogEntry().getDataType() == Number) && (operator == MORE)) {
        formattedArgument = excludeIntervalBoundary(formattedArgument, true);
        // TODO: no boundary exclusion for DateTime with MORE !
      }
      result = "[" + formattedArgument + " TO *]";
    } else if (operator == BETWEEN || (operator == PER_YEAR)) {
      String[] splits = queryElem.getDesiredContentSplitted();
      if (operator == PER_YEAR)
        splits[1] = Integer.parseInt(splits[1]) + 1 + "";
      if (queryElem.getCatalogEntry().getDataType() == DateTime) {
        result = "[" + SolrUtil.format2SolrDate(splits[0]) + " TO "
                + SolrUtil.format2SolrDate(splits[1]) + "]";
      } else {
        result = "[" + splits[0] + " TO " + splits[1] + "]";
      }
    } else {
      throw new QueryStructureException(QueryStructureExceptionType.INTERVAL_SYNTAX, queryElem,
              "unknown parent_shell");
    }
    return result;
  }

  @Override
  public String visit(QuerySubQuery queryElem) {
    return null;
  }

  @Override
  public String visit(QueryStatisticColumn queryElem) {
    return null;
  }

  @Override
  public String visit(QueryStatisticFilter queryElem) throws QueryException {
    return visitQueryAnd(queryElem);
  }

  @Override
  public String visit(QueryStatisticRow queryElem) {
    return null;
  }

  @Override
  public String visit(QueryNot queryElem) {
    return null;
  }

  @Override
  public String visit(QueryNTrue queryElem) {
    return null;
  }

  @Override
  public String visit(QueryOr queryElem) throws QueryException {
    return visitQueryOr(queryElem);
  }

  protected String visitQueryOr(QueryStructureContainingElem queryElem) throws QueryException {
    if (queryElem.isOptional())
      return "";
    List<String> list = new ArrayList<>();
    for (QueryStructureElem qElem : queryElem.getChildren()) {
      String subQueryText = qElem.accept(this);
      if (subQueryText != null && !subQueryText.isEmpty()) {
        list.add(subQueryText);
      }
    }
    String result = StringUtilsUniWue.concat(list, " OR ");
    if (!result.isEmpty())
      result = "(" + result + ")";
    return result;
  }

  @Override
  public String visit(QueryTempFilter queryElem) {
    return null;
  }

  @Override
  public String visit(QueryAnd queryElem) throws QueryException {
    if (queryElem.isOptional())
      return "";
    return visitQueryAnd(queryElem);
  }

  protected String visitQueryAnd(QueryAnd queryElem) throws QueryException {
    List<String> list = new ArrayList<>();
    for (QueryStructureElem qElem : queryElem.getChildren()) {
      String subQueryText = qElem.accept(this);
      if (subQueryText != null && !subQueryText.isEmpty()) {
        list.add(subQueryText);
      }
    }
    String result = StringUtilsUniWue.concat(list, " AND ");
    if (!result.isEmpty())
      result = "(" + result + ")";
    return result;
  }

  @Override
  public String visit(QueryRoot queryElem) throws QueryException {
    useIntermediateLayer = queryElem.getFilterIDTypeToUseForCount() == FilterIDType.DocID;
    List<String> list = new ArrayList<>();
    for (QueryStructureElem child : queryElem.getChildren()) {
      String visit = child.accept(this);
      if (visit != null && !visit.isEmpty())
        list.add(visit);
    }
    String layerQuery = getParentLayerQuery(useIntermediateLayer);
    String docTypeQuery = SolrUtil.createDocTypeQuery(docType);
    String query = "*:* AND " + layerQuery + " AND " + docTypeQuery;
    if (!list.isEmpty()) {
      String queryString = String.join(" AND ", list);
      query = query + " AND " + queryString;
    }
    return query;
  }

  @Override
  public String visit(QueryIDFilter queryElem) throws QueryException {
    boolean excludeSubQuery = false;
    if (queryElem.getFilterIDType() == FilterIDType.GROUP) {
      if (oneAttributeHasANegatedOperator(queryElem)) {
        QueryComplementCreatorVisitor complement;
        try {
          complement = new QueryComplementCreatorVisitor();
          queryElem = (QueryIDFilter) queryElem.accept(complement);
          excludeSubQuery = true;
        } catch (SQLException e) {
          throw new QueryException(e);
        }
      }
    }
    buildIDFilterString(queryElem);
    String andList = visitQueryAnd(queryElem);
    // String andList = StringUtilsUniWue.concat(list, " AND ");
    if (queryElem.getFilterIDType() == FilterIDType.GROUP) {
      return buildChildQuery(andList, excludeSubQuery, false, useIntermediateLayer);
    } else {
      return andList;
    }
  }

  private boolean oneAttributeHasANegatedOperator(QueryIDFilter queryElem) {
    return queryElem.getAttributesRecursive().stream()
            .anyMatch(n -> n.getContentOperator().isNegated());
  }

  protected void buildIDFilterString(QueryIDFilter queryElem) {
    if (queryElem.getFilterIDType() != FilterIDType.Year && queryElem.hasIDRestrictions()) {
      String idFilterField = "";
      if (queryElem.getFilterIDType() == FilterIDType.PID) {
        idFilterField = SOLR_FIELD_PATIENT_ID;
      } else if (queryElem.getFilterIDType() == FilterIDType.CaseID) {
        idFilterField = SOLR_FIELD_CASE_ID;
      } else if (queryElem.getFilterIDType() == FilterIDType.DocID) {
        idFilterField = SOLR_FIELD_DOC_ID;
      }
      List<String> idFilterQueries = new ArrayList<>();
      if (queryElem.hasGivenTimeRestrictions()) {
        for (Long aID : queryElem.getRestrictedTimes().keySet()) {
          String idRestrictionString = idFilterField + ":" + aID;
          List<String> curTimeRestrictions = new ArrayList<>();
          for (Date aTimeRestriction : queryElem.getRestrictedTimes().get(aID)) {
            if (queryElem.isRestrictingTimesAreDates()) {
              String startTime = SolrUtil.format2SolrDate(aTimeRestriction);
              Calendar calendar = Calendar.getInstance();
              calendar.setTime(aTimeRestriction);
              calendar.set(Calendar.DAY_OF_MONTH, calendar.get(Calendar.DAY_OF_MONTH) + 1);
              String endTime = SolrUtil.format2SolrDate(new Date(calendar.getTimeInMillis()));
              String curTimeRestrictionString = SOLR_FIELD_MEASURE_TIME + ":[" + startTime + " TO "
                      + endTime + "]";
              curTimeRestrictions.add(curTimeRestrictionString);
            } else {
              curTimeRestrictions.add(SOLR_FIELD_MEASURE_TIME + ":\""
                      + SolrUtil.format2SolrDate(aTimeRestriction) + "\"");
            }
          }
          String timeFilterString = StringUtilsUniWue.concat(curTimeRestrictions, " OR ");
          idFilterQueries.add("(" + idRestrictionString + " AND (" + timeFilterString + "))");
          if (idToTimeFilterStringMap == null) {
            idToTimeFilterStringMap = new HashMap<>();
          }
          idToTimeFilterStringMap.put(aID, timeFilterString);
        }
      } else {
        for (Long aID : queryElem.getIds()) {
          idFilterQueries.add(idFilterField + ":" + aID);
        }
      }
      if (!idFilterQueries.isEmpty()) {
        idFilterString = StringUtilsUniWue.concat(idFilterQueries, " OR ");
      }
    } else if (queryElem.getFilterIDType() == FilterIDType.Year) {
      List<String> yearFilterRestrictions = new ArrayList<>();
      for (Set<Date> aYearSet : queryElem.getRestrictedTimes().values()) {
        for (Date aYear : aYearSet) {
          String startTime = SolrUtil.format2SolrDate(aYear);
          Calendar calendar = Calendar.getInstance();
          calendar.setTime(aYear);
          calendar.set(Calendar.YEAR, calendar.get(Calendar.YEAR) + 1);
          String endTime = SolrUtil.format2SolrDate(new Date(calendar.getTimeInMillis()));
          String curTimeRestrictionString = SOLR_FIELD_MEASURE_TIME + ":[" + startTime + " TO "
                  + endTime + "]";
          yearFilterRestrictions.add(curTimeRestrictionString);
        }
      }
      if (!yearFilterRestrictions.isEmpty()) {
        idFilterString = StringUtilsUniWue.concat(yearFilterRestrictions, " OR ");
        idToTimeFilterStringMap = new HashMap<>();
        idToTimeFilterStringMap.put((long) -1, idFilterString);
      }
    }
  }

  @Override
  public String visit(QueryTempOpAbs queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String visit(QueryTempOpRel queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String visit(QueryValueCompare queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  public Map<String, String> getParams() {
    return params;
  }

  public Map<Long, String> getIdToTimeFilterStringMap() {
    return idToTimeFilterStringMap;
  }

  public String getIdFilterString() {
    return idFilterString;
  }
}
