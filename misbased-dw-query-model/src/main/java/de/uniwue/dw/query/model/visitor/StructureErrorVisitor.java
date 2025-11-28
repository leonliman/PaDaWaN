package de.uniwue.dw.query.model.visitor;

import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.query.model.client.DefaultQueryRunner;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.exception.QueryStructureException;
import de.uniwue.dw.query.model.exception.QueryStructureException.QueryStructureExceptionType;
import de.uniwue.dw.query.model.lang.*;
import de.uniwue.misc.util.RegexUtil;
import de.uniwue.misc.util.TimeUtil;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class StructureErrorVisitor implements IQueryElementVisitor<Set<QueryStructureException>> {

  // // returns a list of error types if there is any problem with the configuration of the query
  // element

  private DefaultQueryRunner queryRunner;

  public StructureErrorVisitor(DefaultQueryRunner queryRunner) {
    this.queryRunner = queryRunner;
  }

  @Override
  public Set<QueryStructureException> visit(QueryAttribute queryElem) throws QueryException {
    Set<QueryStructureException> result = new HashSet<QueryStructureException>();
    List<ContentOperator> operatorsNeedingDesiredContent = Arrays
            .asList(new ContentOperator[] { ContentOperator.LESS_OR_EQUAL, ContentOperator.LESS,
                    ContentOperator.MORE, ContentOperator.MORE_OR_EQUAL, ContentOperator.BETWEEN });

    if (operatorsNeedingDesiredContent.contains(queryElem.getContentOperator())) {
      if (queryElem.getDesiredContent().isEmpty()) {
        result.add(new QueryStructureException(QueryStructureExceptionType.NO_DESIRED_CONTENT_GIVEN,
                queryElem));
      }
      if ((queryElem.getCatalogEntry().getDataType() == CatalogEntryType.Number)
              && argumentMustHaveExactlyOneNumberOrDate(queryElem.getContentOperator())
              && !RegexUtil.allNumbersRegex.matcher(queryElem.getDesiredContent()).find()) {
        result.add(new QueryStructureException(QueryStructureExceptionType.VALUE_IS_NO_NUMBER,
                queryElem, "Value is no number for attribute '"
                + queryElem.getCatalogEntry().getName() + "'"));
      }
      if ((queryElem.getCatalogEntry().getDataType() == CatalogEntryType.DateTime)
              && argumentMustHaveExactlyOneNumberOrDate(queryElem.getContentOperator())
              && (TimeUtil.parseDate(queryElem.getDesiredContent()) == null)) {
        result.add(new QueryStructureException(QueryStructureExceptionType.VALUE_IS_NO_DATE,
                queryElem,
                "Value is no date for attribute '" + queryElem.getCatalogEntry().getName() + "'"));
      }
    }
    if (queryElem.getContentOperator() == ContentOperator.BETWEEN) {
      if (queryElem.getDesiredContentSplitted().length != 2) {
        result.add(new QueryStructureException(
                QueryStructureExceptionType.INVALID_NUMBER_OF_INTERVAL_BOUNDERIES, queryElem,
                "Tow boundaries are expected for attribute '"
                        + queryElem.getCatalogEntry().getName() + "'"));
      }
    }
    if (queryElem.getRoot().isStatisticQuery()) {
      checkValidityForDistributionQuery(result, queryElem);
    }
    if (!queryRunner.canProcessFulltextNearOperators()) {
      String desiredContent = queryElem.getDesiredContent();
      if ((desiredContent != null) && !desiredContent.isEmpty()) {
        if (desiredContent.matches(".*\\[.*\\].*")) {
          result.add(new QueryStructureException(
                  QueryStructureExceptionType.ENGINE_CANNOT_PERFORM_OPERATION, queryElem,
                  "Engine cannot process near operators in fulltext query"));
        }
      }
    }
    if ((queryElem.getTempOpsAbs().size() > 0)
            && !queryRunner.canProcessAbsoluteTemporalContraints()
            && !queryRunner.canDoPostProcessing()) {
      result.add(new QueryStructureException(
              QueryStructureExceptionType.ENGINE_CANNOT_PERFORM_OPERATION, queryElem,
              "Engine cannot process absolute temporal constraints"));
    }
    if ((queryElem.getTemporalOpsRel().size() > 0)
            && !queryRunner.canProcessRelativeTemporalCompares()
            && !queryRunner.canDoPostProcessing()) {
      result.add(new QueryStructureException(
              QueryStructureExceptionType.ENGINE_CANNOT_PERFORM_OPERATION, queryElem,
              "Engine cannot process relative temporal constraints"));
    }
    if ((queryElem.displayInfoDate() || queryElem.displayCaseID() || queryElem.displayDocID())
            && !queryRunner.canProcessInfoAdditonalInfos()) {
      result.add(new QueryStructureException(
              QueryStructureExceptionType.ENGINE_CANNOT_PERFORM_OPERATION, queryElem,
              "Engine cannot process additional informations for fact"));
    }
    return result;
  }

  private boolean argumentMustHaveExactlyOneNumberOrDate(ContentOperator op) {
    return (op == ContentOperator.LESS || op == ContentOperator.LESS_OR_EQUAL
            || op == ContentOperator.EQUALS || op == ContentOperator.MORE
            || op == ContentOperator.MORE_OR_EQUAL);
  }

  private void checkValidityForDistributionQuery(Set<QueryStructureException> result,
          QueryAttribute queryElem) throws QueryException {
    if (queryElem.getContentOperator() == ContentOperator.PER_INTERVALS) {
      String[] split = queryElem.getDesiredContentSplitted();
      boolean isFirst = true;
      double lastValue = 0;
      for (String s : split) {
        if (queryElem.getCatalogEntry().getDataType() == CatalogEntryType.DateTime) {
          try {
            List<Date> dates = TimeUtil.parseDates(queryElem.getDesiredContentSplitted());
            result.addAll(checkIftimeIntervalAreTooNarrow(dates, queryElem));
          } catch (ParseException e) {
            result.add((new QueryStructureException(QueryStructureExceptionType.VALUE_IS_NO_DATE,
                    queryElem)));
          }
        } else if (queryElem.getCatalogEntry().getDataType() == CatalogEntryType.Number) {
          double curValue = Double.parseDouble(s);
          if ((curValue - lastValue < 5) && !isFirst) {
            result.add(new QueryStructureException(QueryStructureExceptionType.TOO_NARROW_INTERVALS,
                    queryElem));
          }
          lastValue = curValue;
        }
        isFirst = false;
      }

    } else if (queryElem.getContentOperator() == ContentOperator.BETWEEN
            && queryElem.getCatalogEntry().getDataType() == CatalogEntryType.DateTime) {
      try {
        List<Date> dates = TimeUtil.parseDates(queryElem.getDesiredContentSplitted());
        if (dates.size() != 2)
          result.add((new QueryStructureException(
                  QueryStructureExceptionType.INVALID_NUMBER_OF_INTERVAL_BOUNDERIES, queryElem)));
        result.addAll(checkIftimeIntervalAreTooNarrow(dates, queryElem));
      } catch (ParseException e) {
        result.add((new QueryStructureException(QueryStructureExceptionType.VALUE_IS_NO_DATE,
                queryElem)));
      }

    }
    if (

            isFilterAttribute(queryElem)) {
      List<ContentOperator> validFilterOperators = Arrays.asList(new ContentOperator[] {
              ContentOperator.EXISTS, ContentOperator.NOT_EXISTS, ContentOperator.LESS_OR_EQUAL,
              ContentOperator.LESS, ContentOperator.MORE, ContentOperator.MORE_OR_EQUAL,
              ContentOperator.BETWEEN, ContentOperator.CONTAINS, ContentOperator.CONTAINS_NOT,
              ContentOperator.CONTAINS_POSITIVE, ContentOperator.CONTAINS_NOT_POSITIVE, ContentOperator.EQUALS });
      if (!validFilterOperators.contains(queryElem.getContentOperator())) {
        result.add(new QueryStructureException(
                QueryStructureExceptionType.NOT_ALLOWED_FILTER_OPERATOR, queryElem));
      }
    }
  }

  private void checkIfNumberIntervalAreTooNarrow(String[] desiredContentSplitted,
          QueryAttribute queryElem) throws QueryStructureException {
    int lastNumber = Integer.MIN_VALUE - 1;
    boolean first = true;
    for (String s : desiredContentSplitted) {
      int curValue = 0;
      if (s.isEmpty()) {
        if (first)
          curValue = Integer.MIN_VALUE + 1;
        else
          curValue = Integer.MAX_VALUE - 1;
      } else {
        curValue = Integer.parseInt(s);
      }
      if (first) {
        first = false;
      } else {
        if (Math.abs(curValue - lastNumber) < 5) {
          throw new QueryStructureException(QueryStructureExceptionType.TOO_NARROW_INTERVALS,
                  queryElem);
        }
      }
      lastNumber = curValue;
    }

  }

  private static List<QueryStructureException> checkIftimeIntervalAreTooNarrow(List<Date> dates,
          QueryStructureElem queryElem) {
    List<QueryStructureException> result = new ArrayList<>();
    if (dates.size() < 2)
      return result;
    Iterator<Date> iterator = dates.iterator();
    Date lowerBound = null;
    Date upperBound = iterator.next();
    while (iterator.hasNext()) {
      lowerBound = upperBound;
      upperBound = iterator.next();
      long diffInMillies = upperBound.getTime() - lowerBound.getTime();
      long diffInDays = TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);
      if (diffInDays < 30) {
        result.add(new QueryStructureException(QueryStructureExceptionType.TIME_INTERVAL_TOO_NARROW,
                queryElem));
      }
    }
    return result;
  }

  private boolean isFilterAttribute(QueryAttribute anElem) {
    List<QueryElem> ancestors = anElem.getAncestors();
    for (QueryElem ancestor : ancestors) {
      if (ancestor instanceof QueryStatisticFilter) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Set<QueryStructureException> visit(QuerySubQuery queryElem) {
    return new HashSet<QueryStructureException>();
  }

  @Override
  public Set<QueryStructureException> visit(QueryStatisticColumn queryElem) throws QueryException {
    return visitChilds(queryElem);
  }

  @Override
  public Set<QueryStructureException> visit(QueryStatisticFilter queryElem) throws QueryException {
    return visitChilds(queryElem);

  }

  @Override
  public Set<QueryStructureException> visit(QueryStatisticRow queryElem) throws QueryException {
    return visitChilds(queryElem);
  }

  @Override
  public Set<QueryStructureException> visit(QueryNot queryElem) throws QueryException {
    Set<QueryStructureException> result = visitChilds(queryElem);
    if (queryElem.getChildren().size() == 0) {
      result.add(new QueryStructureException(QueryStructureExceptionType.NOT_WITHOUT_CHILD));
    }
    return result;
  }

  @Override
  public Set<QueryStructureException> visit(QueryNTrue queryElem) throws QueryException {
    return visitChilds(queryElem);

  }

  @Override
  public Set<QueryStructureException> visit(QueryOr queryElem) throws QueryException {
    return visitChilds(queryElem);

  }

  @Override
  public Set<QueryStructureException> visit(QueryTempFilter queryElem) throws QueryException {
    return visitChilds(queryElem);

  }

  @Override
  public Set<QueryStructureException> visit(QueryAnd queryElem) throws QueryException {
    return visitChilds(queryElem);

  }

  @Override
  public Set<QueryStructureException> visit(QueryRoot queryElem) throws QueryException {
    Set<QueryStructureException> result = visitChilds(queryElem);
    // this should not throw an exception. It is used for post processed count queries
    // if (!queryElem.getRoot().isStatisticQuery()) {
    // boolean oneOutput = false;
    // for (QueryStructureElem aChild : queryElem.getChildren()) {
    // if (aChild.displayColumns()) {
    // oneOutput = true;
    // }
    // }
    // if (!oneOutput) {
    // result.add(new QueryStructureException(QueryStructureExceptionType.NO_OUTPUT_ATTRIBUTE));
    // }
    // }
    if (!queryRunner.canDoPostProcessing() && queryRunner.hasToDoPostProcessing(queryElem)) {
      result.add(new QueryStructureException(
              QueryStructureExceptionType.ENGINE_CANNOT_PERFORM_OPERATION, queryElem,
              "Postprocessing would be necessary but engine cannot post process"));
    }
    int attributesWithMultipleRows = 0;
    for (QueryAttribute anAttr : queryElem.getAttributesRecursive()) {
      if (anAttr.isMultipleRows()) {
        attributesWithMultipleRows++;
      }
    }
    if (attributesWithMultipleRows > 1) {
      result.add(new QueryStructureException(
              QueryStructureExceptionType.MORE_THAN_ONE_ATTR_WITH_MULTIPLE_ROWS));
    }
    boolean oneNonOptionalChildElement = false;
    for (QueryStructureElem aChild : queryElem.getChildren()) {
      if (!aChild.active) {
        continue;
      }
      if (!aChild.isOptional()) {
        oneNonOptionalChildElement = true;
      }
    }
    if (!oneNonOptionalChildElement) {
      result.add(
              new QueryStructureException(QueryStructureExceptionType.NO_NON_OPTIONAL_OP_FOR_ROOT));
    }
    // if (!queryRunner.canProcessRelativeTemporalCompares()) {
    // if (queryElem.getTempOpsRelRecursive().size() > 0) {
    // result.add(new QueryStructureException(
    // QueryStructureExceptionType.ENGINE_CANNOT_PERFORM_OPERATION,
    // queryElem.getTempOpsRelRecursive().get(0),
    // "Engine cannot process relative compares."));
    // }
    // }
    // if (!queryRunner.canProcessRelativeQuantitativeCompares()) {
    // if (queryElem.getValueComparesRecursive().size() > 0) {
    // result.add(new QueryStructureException(
    // QueryStructureExceptionType.ENGINE_CANNOT_PERFORM_OPERATION,
    // queryElem.getValueComparesRecursive().get(0),
    // "Engine cannot process relative compares."));
    // }
    // }
    // if (queryElem.isDistinct() && !queryRunner.canProcessDistinct()) {
    // result.add(new QueryStructureException(
    // QueryStructureExceptionType.ENGINE_CANNOT_PERFORM_OPERATION, queryElem,
    // "Engine cannot process distinct."));
    // }
    return result;
  }

  @Override
  public Set<QueryStructureException> visit(QueryIDFilter queryElem) throws QueryException {
    Set<QueryStructureException> result = visitChilds(queryElem);
    QueryIDFilter ancestorFilterWithTimes = queryElem.getAncestorFilterWithTimes();
    if (((ancestorFilterWithTimes != null) && (ancestorFilterWithTimes != queryElem))
            && queryElem.hasTimeRestrictions()) {
      result.add(new QueryStructureException(
              QueryStructureExceptionType.NO_CASCADED_TIME_RESTRICTIONS));
    }
    // if ((queryElem.hasIDRestrictions() || queryElem.hasTimeRestrictions())
    // && !queryRunner.canProcessFixedIDs()) {
    // result.add(new QueryStructureException(
    // QueryStructureExceptionType.ENGINE_CANNOT_PERFORM_OPERATION, queryElem,
    // "Engine cannot process fixed IDs or time restrictions"));
    // }
    // if (queryElem.isDistinct() && !queryRunner.canProcessDistinct()) {
    // result.add(new QueryStructureException(
    // QueryStructureExceptionType.ENGINE_CANNOT_PERFORM_OPERATION, queryElem,
    // "Engine cannot process distinct."));
    // }
    return result;
  }

  public Set<QueryStructureException> visit(QueryStructureElem queryElem) {
    Set<QueryStructureException> result = new HashSet<QueryStructureException>();
    return result;
  }

  public Set<QueryStructureException> visitQueryStructureContainingElem(
          QueryStructureContainingElem queryElem) {
    Set<QueryStructureException> result = new HashSet<QueryStructureException>();
    return result;
  }

  protected Set<QueryStructureException> visitChilds(QueryStructureContainingElem queryElem)
          throws QueryException {
    Set<QueryStructureException> result = new HashSet<QueryStructureException>();
    Set<QueryStructureException> furtherErrors = visitQueryStructureContainingElem(queryElem);
    result.addAll(furtherErrors);
    for (QueryStructureElem child : queryElem.getChildren()) {
      furtherErrors = child.accept(this); // for subclasses of QueryStructureElem
      result.addAll(furtherErrors);
    }
    return result;
  }

  @Override
  public Set<QueryStructureException> visit(QueryTempOpAbs queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Set<QueryStructureException> visit(QueryTempOpRel queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Set<QueryStructureException> visit(QueryValueCompare queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

}
