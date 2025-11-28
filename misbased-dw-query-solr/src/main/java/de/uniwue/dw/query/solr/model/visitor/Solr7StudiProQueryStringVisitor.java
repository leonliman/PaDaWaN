package de.uniwue.dw.query.solr.model.visitor;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.exception.QueryStructureException;
import de.uniwue.dw.query.model.exception.QueryStructureException.QueryStructureExceptionType;
import de.uniwue.dw.query.model.lang.*;
import de.uniwue.dw.query.solr.SolrUtil;
import de.uniwue.dw.query.solr.model.solrText.SolrQueryParser;
import de.uniwue.misc.util.StringUtilsUniWue;
import org.antlr.v4.runtime.tree.ParseTree;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static de.uniwue.dw.core.model.data.CatalogEntryType.DateTime;
import static de.uniwue.dw.core.model.data.CatalogEntryType.Number;
import static de.uniwue.dw.query.model.lang.ContentOperator.*;

public class Solr7StudiProQueryStringVisitor implements IQueryElementVisitor<String> {
  public static final BigDecimal SMALL_VALUE_FOR_INTERVALL_BOUNDARY = new BigDecimal("0.0001");

  @Override
  public String visit(QueryAttribute queryElem) throws QueryException {
    String result;
    // handle optional flag
    if (queryElem.isOptional()) {
      result = handleOptionalQueryAttribute(queryElem);
      // handle just-display-parent_shell
    } else if (queryElem.getContentOperator() == EMPTY) {
      result = "*:*";
      // handle filter unknown flag
    } else if (!queryElem.isFilterUnkown()) {
      result = handleFilterUnkownAttributesNotSet(queryElem);
      // handle text queries
    } else if (queryElem.getContentOperator() == CONTAINS
            || queryElem.getContentOperator() == CONTAINS_POSITIVE) {
      ParseTree desiredContentAsParseTree = queryElem.getDesiredContentAsParseTree();
      result = SolrQueryParser.parse(desiredContentAsParseTree, queryElem);
    } else if (queryElem.getContentOperator() == CONTAINS_NOT
            || queryElem.getContentOperator() == CONTAINS_NOT_POSITIVE) {
      result = "(*:* !" + SolrQueryParser.parse(queryElem.getDesiredContentAsParseTree(), queryElem)
              + ")";
      // handle existance queries
    } else if (queryElem.getContentOperator() == EXISTS) {
      if (isChildQuery(queryElem))
        result = getChildQuery(queryElem);
      else
        result = SolrUtil.FIELD_EXISTANCE_FIELD + ":"
                + SolrUtil.getSolrID(queryElem.getCatalogEntry());
    } else if (queryElem.getContentOperator() == NOT_EXISTS) {
      result = "(*:* !" + SolrUtil.FIELD_EXISTANCE_FIELD + ":"
              + SolrUtil.getSolrID(queryElem.getCatalogEntry()) + ")";
    } else {
      result = getSolrFiledName(queryElem) + ":" + getSolrFieldValue(queryElem);
    }
    return result;
  }

  private String getChildQuery(QueryAttribute queryElem) {
    String query = "{!parent which=\"string_type:student\" v=\"string_type:dpp AND " +
            SolrUtil.FIELD_EXISTANCE_FIELD
            + ":" + SolrUtil.getSolrID(queryElem.getCatalogEntry()) + "\"}";
    return query;
  }

  private boolean isChildQuery(QueryAttribute queryElem) {
    String project = queryElem.getCatalogEntry().getProject();
    return (project.equals("degree") || project.equals("subject") || project.equals("orgunit"));
  }

  private String handleOptionalQueryAttribute(QueryAttribute queryElem) {
    return "";
  }

  private String handleFilterUnkownAttributesNotSet(QueryAttribute queryElem)
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
    String formattedArgutment = queryElem.getDesiredContent();
    ContentOperator operator = queryElem.getContentOperator();
    if ((queryElem.getCatalogEntry().getDataType() == DateTime) && (operator != BETWEEN)
            && (operator != PER_YEAR)) {
      formattedArgutment = SolrUtil.format2SolrDate(formattedArgutment);
    }
    if ((operator == EXISTS) || (operator == NOT_EXISTS)) {
      result = "*";
    } else if ((operator == LESS) || (operator == LESS_OR_EQUAL)) {
      if ((queryElem.getCatalogEntry().getDataType() == Number) && (operator == LESS)) {
        formattedArgutment = excloudIntervallBoundary(formattedArgutment, false);
        // TODO: no boundary exclusion for DateTime with LESS !
      }
      result = "[* TO " + formattedArgutment + "]";
    } else if (operator == EQUALS) {
      result = formattedArgutment;
    } else if ((operator == MORE) || (operator == MORE_OR_EQUAL)) {
      if ((queryElem.getCatalogEntry().getDataType() == Number) && (operator == MORE)) {
        formattedArgutment = excloudIntervallBoundary(formattedArgutment, true);
        // TODO: no boundary exclusion for DateTime with MORE !
      }
      result = "[" + formattedArgutment + " TO *]";
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

  public static String excloudIntervallBoundary(String value, boolean increment) {
    try {
      if (increment)
        return new BigDecimal(value).add(SMALL_VALUE_FOR_INTERVALL_BOUNDARY).toString();
      else
        return new BigDecimal(value).subtract(SMALL_VALUE_FOR_INTERVALL_BOUNDARY).toString();
    } catch (NumberFormatException e) {
      // TODO: bad hack
      return value;
    }
  }

  private static String getSolrFiledName(QueryAttribute queryElem) {
    return SolrUtil.getSolrFieldName(queryElem);

    // String fieldname = SharedSolrUtil.getSolrFieldName(queryElem.catalogEntry);
    // if (queryElem.reductionOperator == ReductionOperator.Earliest)
    // fieldname = fieldname + "_first";
    // else if (queryElem.reductionOperator == ReductionOperator.Latest)
    // fieldname = fieldname + "_last";
    // else if (queryElem.reductionOperator == ReductionOperator.Min)
    // fieldname = fieldname + "_min";
    // else if (queryElem.reductionOperator == ReductionOperator.Max)
    // fieldname = fieldname + "_max";
    // else if (queryElem.reductionOperator == ReductionOperator.None)
    // fieldname = fieldname + "";
    //
    // if (queryElem.contentOperator == containsPositive)
    // fieldname = fieldname + "_positive";
    // return SharedSolrUtil.formatFileName(fieldname);
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
    if (queryElem.isOptional())
      return "";
    List<String> list = new ArrayList<>();
    for (QueryStructureElem qElem : queryElem.getChildren()) {
      String subQueryText = qElem.accept(this);
      if (!qElem.isOptional() && !subQueryText.isEmpty()) {
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

  private String visitQueryAnd(QueryAnd queryElem) throws QueryException {
    List<String> list = new ArrayList<>();
    for (QueryStructureElem qElem : queryElem.getChildren()) {
      String subQueryText = qElem.accept(this);
      if (!qElem.isOptional() && !subQueryText.isEmpty()) {
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
    List<String> list = new ArrayList<String>();
    for (QueryStructureElem child : queryElem.getChildren()) {
      String visit = child.accept(this);
      if (visit != null && !visit.isEmpty())
        list.add(visit);
    }
//    double version = Double.valueOf(queryElem.getVersion());
//    if (version >= 0.7) {
//      boolean isPatientQuery = queryElem.isPatientQuery();
//      if (isPatientQuery) {
//        list.add("string_doc_type:patient");
//      } else {
//        list.add("string_doc_type:case");
//      }
//    }
    return StringUtilsUniWue.concat(list, " AND ");
  }

  @Override
  public String visit(QueryIDFilter queryElem) throws QueryException {
    List<String> list = new ArrayList<String>();
    for (QueryStructureElem child : queryElem.getChildren()) {
      String visit = child.accept(this);
      if (visit != null && !visit.isEmpty())
        list.add(visit);
    }
    return StringUtilsUniWue.concat(list, " AND ");
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

}
