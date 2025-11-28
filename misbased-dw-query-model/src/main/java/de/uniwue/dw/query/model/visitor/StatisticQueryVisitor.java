package de.uniwue.dw.query.model.visitor;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.*;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;
import de.uniwue.dw.query.model.manager.QueryManipulationManager;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StatisticQueryVisitor implements IQueryElementVisitor<List<QueryStructureElem>> {

  public static final BigDecimal SMALL_VALUE_FOR_INTERVALL_BOUNDARY = new BigDecimal("0.0001");

  public static final String SONS = "Nachfolger";

  private static final String ALL = "Alle";

  private List<QueryStructureElem> columns = new ArrayList<>();

  private List<QueryStructureElem> rows = new ArrayList<>();

  private QueryStatisticFilter statisticFilter;

  private FilterIDType filterIDTypeToUseForCount;

  private boolean isDistinct;

  private String rootComment;

  private static List<QueryStructureElem> createAttributesWithGenerator(QueryAttribute entry)
          throws QueryException {
    List<QueryStructureElem> attributes = new ArrayList<QueryStructureElem>();
    if (entry.getContentOperator() == ContentOperator.NOT_EXISTS) {
      QueryAttribute attribute = createAttribute(entry);
      attributes.add(attribute);
    } else if (entry.getDesiredContent() == null || entry.getDesiredContent().isEmpty()) {
      QueryAttribute attribute = createAttribute(entry);
      entry.setContentOperator(ContentOperator.EXISTS);
      attributes.add(attribute);

    } else if (entry.getCatalogEntry().getDataType() == CatalogEntryType.Number) {
      List<QueryAttribute> numericAttributes = createNumericDistributionAttributes(entry);
      attributes.addAll(numericAttributes);
    } else if (entry.getCatalogEntry().getDataType() == CatalogEntryType.DateTime) {
      List<QueryAttribute> dateAttributes = createDateDistributionAttributes(entry);
      attributes.addAll(dateAttributes);
    } else if (entry.getDesiredContent().equalsIgnoreCase(SONS)) {
      List<QueryAttribute> sonAttributes = createChildrenAttributes(entry);
      attributes.addAll(sonAttributes);
    } else if (entry.getDesiredContent().equalsIgnoreCase(ALL)) {
      List<QueryAttribute> allValuesAttributes = createAllValueAttributes(entry);
      attributes.addAll(allValuesAttributes);
    } else {
      QueryAttribute attribute = createAttribute(entry);
      attributes.add(attribute);
    }
    return attributes;
  }

  private static QueryAttribute createAttribute(QueryAttribute entry) {
    if (entry.getContentOperator() == null)
      entry.setContentOperator(ContentOperator.EXISTS);
    return entry;
  }

  private static List<QueryAttribute> createNumericDistributionAttributes(QueryAttribute entry)
          throws QueryException {
    if (entry.getContentOperator() == ContentOperator.PER_INTERVALS) {
      String[] numbers = entry.getDesiredContentSplitted();
      List<String> asList = Arrays.asList(numbers);
      return createNumericDistributionAttributes(entry, asList, true);
    } else {
      ArrayList<QueryAttribute> list = new ArrayList<QueryAttribute>();
      list.add(entry);
      return list;
    }
  }

  private static List<QueryAttribute> createNumericDistributionAttributes(QueryAttribute entry,
          List<String> cutPoints, boolean b) throws QueryException {
    List<QueryAttribute> attributes = new ArrayList<QueryAttribute>();
    String lastNumber = null;
    boolean minusInfinityIsBoundary = false;
    boolean first = true;
    for (String curNumber : cutPoints) {
      curNumber = curNumber.trim();
      if (curNumber.isEmpty() && first) {
        minusInfinityIsBoundary = true;
      } else if (curNumber.isEmpty() && !first) {
        ContentOperator operator = ContentOperator.MORE;
        String argument = lastNumber;
        QueryAttribute a = new QueryAttribute(entry.getCatalogEntry(), operator, argument,
                entry.getReductionOperator());
        attributes.add(a);
      } else {
        ContentOperator operator;
        String argument;
        if (lastNumber == null) {
          operator = ContentOperator.LESS_OR_EQUAL;
          argument = curNumber;
        } else {
          operator = ContentOperator.BETWEEN;
          String lowerBound = lastNumber;
          if (isNumber(lastNumber))
            lowerBound = excloudIntervallBoundary(lowerBound, true);
          argument = lowerBound + " " + QueryAttribute.PERIOD_DELIMITER + " " + curNumber;
        }
        if (lastNumber != null || minusInfinityIsBoundary) {
          QueryAttribute a = new QueryAttribute(entry.getCatalogEntry(), operator, argument,
                  entry.getReductionOperator());
          attributes.add(a);
        }
        lastNumber = curNumber;
      }
      first = false;
    }
    return attributes;
  }

  private static boolean isNumber(String value) {
    if (value == null) {
      return false;
    }
    try {
      Double.parseDouble(value.replaceAll(",", "."));
      return true;
    } catch (NumberFormatException nfe) {
      return false;
    }
  }

  private static String excloudIntervallBoundary(String value, boolean increment) {
    if (increment)
      return new BigDecimal(value).add(SMALL_VALUE_FOR_INTERVALL_BOUNDARY).toString();
    else
      return new BigDecimal(value).subtract(SMALL_VALUE_FOR_INTERVALL_BOUNDARY).toString();
  }

  private static List<QueryAttribute> createDateDistributionAttributes(QueryAttribute entry)
          throws QueryException {
    String[] dates = entry.getDesiredContentSplitted();
    if (entry.getContentOperator() == ContentOperator.BETWEEN) {
      String start = dates[0];
      String end = dates[1];
      List<String> solrDates = new ArrayList<String>();
      solrDates.add(start);
      solrDates.add(end);
      return createNumericDistributionAttributes(entry, solrDates, false);
    }

    if (entry.getContentOperator() == ContentOperator.PER_INTERVALS) {
      List<String> cutPoints = Arrays.asList(dates);
      return createNumericDistributionAttributes(entry, cutPoints, true);
    }

    if (dates.length == 2 && entry.getContentOperator() == ContentOperator.PER_YEAR) {
      int start = Integer.parseInt(dates[0].trim());
      int end = Integer.parseInt(dates[1].trim());

      List<String> dateBounderies = new ArrayList<String>();
      for (int year = start; year <= end + 1; year++) {
        String dateBound = "1.1." + year;
        // String solrDate = year + "-01-01T00:00:00Z";
        dateBounderies.add(dateBound);
      }
      return createNumericDistributionAttributes(entry, dateBounderies, false);
    }
    if (dates.length == 1 && entry.getContentOperator() == ContentOperator.PER_MONTH) {
      List<String> dateBounderies = new ArrayList<String>();
      for (int month = 1; month <= 12; month++) {
        String solrDate = "1." + month + "." + dates[0];
        dateBounderies.add(solrDate);
      }
      String solrDate = "1.1." + (Integer.valueOf(dates[0]) + 1);
      dateBounderies.add(solrDate);
      return createNumericDistributionAttributes(entry, dateBounderies, false);
    }
    if (dates.length == 2 && entry.getContentOperator() == ContentOperator.PER_MONTH) {
      int start = Integer.parseInt(dates[0].trim());
      int end = Integer.parseInt(dates[1].trim());

      List<String> dateBounderies = new ArrayList<String>();
      for (int y = start; y <= end + 1; y++) {
        for (int m = 1; m <= 12; m++) {
          String date = "1." + m + "." + y;
          dateBounderies.add(date);
        }
      }
      return createNumericDistributionAttributes(entry, dateBounderies, false);
    }
    if (dates.length == 1) {
      ArrayList<QueryAttribute> list = new ArrayList<QueryAttribute>();
      list.add(entry);
      return list;
      // List<String> solrDates = new ArrayList<String>();
      // solrDates.add(dates[0].trim());
      // return createNumericDistributionAttributes(entry, solrDates, true);
    }
    return null;
  }

  private static List<QueryAttribute> createChildrenAttributes(QueryAttribute entry)
          throws QueryException {
    List<QueryAttribute> attrs = new ArrayList<QueryAttribute>();
    List<CatalogEntry> children = entry.getCatalogEntry().getChildren();
    for (CatalogEntry catalogEntry : children) {
      QueryAttribute elem = new QueryAttribute(catalogEntry, ContentOperator.EXISTS, "",
              ReductionOperator.NONE);
      attrs.add(elem);
    }
    return attrs;
  }

  private static List<QueryAttribute> createAllValueAttributes(QueryAttribute entry)
          throws QueryException {
    List<QueryAttribute> attrs = new ArrayList<QueryAttribute>();
    QueryAttribute elem = new QueryAttribute(entry.getCatalogEntry(), ContentOperator.EXISTS, "",
            ReductionOperator.NONE);
    attrs.add(elem);
    elem = new QueryAttribute(entry.getCatalogEntry(), ContentOperator.NOT_EXISTS, "",
            ReductionOperator.NONE);
    attrs.add(elem);
    return attrs;
  }

  @Override
  public List<QueryStructureElem> visit(QueryAttribute queryElem) throws QueryException {
    return createAttributesWithGenerator(queryElem);
  }

  @Override
  public List<QueryStructureElem> visit(QuerySubQuery queryElem) {
    return null;
  }

  @Override
  public List<QueryStructureElem> visit(QueryStatisticColumn queryElem) throws QueryException {
    List<QueryStructureElem> visitChilds = visitChilds(queryElem);
    columns = visitChilds;
    return columns;
  }

  @Override
  public List<QueryStructureElem> visit(QueryStatisticFilter queryElem) throws QueryException {
    this.statisticFilter = queryElem;
    List<QueryStructureElem> visitChilds = visitChilds(queryElem);
    return visitChilds;
  }

  @Override
  public List<QueryStructureElem> visit(QueryStatisticRow queryElem) throws QueryException {
    List<QueryStructureElem> visitChilds = visitChilds(queryElem);
    rows = visitChilds;
    return rows;
  }

  @Override
  public List<QueryStructureElem> visit(QueryNot queryElem) throws QueryException {
    return visitChilds(queryElem);
  }

  @Override
  public List<QueryStructureElem> visit(QueryNTrue queryElem) throws QueryException {
    return visitChilds(queryElem);
  }

  @Override
  public List<QueryStructureElem> visit(QueryOr queryElem) throws QueryException {
    List<QueryStructureElem> result = new ArrayList<QueryStructureElem>();
    List<List<QueryStructureElem>> generatedLists = new ArrayList<List<QueryStructureElem>>();
    for (QueryStructureElem child : queryElem.getChildren()) {
      List<QueryStructureElem> childs = child.accept(this);
      generatedLists.add(childs);
    }
    List<List<QueryStructureElem>> combinations;
    if (queryElem.isPowerSet())
      combinations = createPowerSet(generatedLists);
    else
      combinations = combineSimple(generatedLists);

    for (List<QueryStructureElem> list : combinations) {
      QueryOr queryListElement = new QueryOr(null);
      queryListElement.setName(queryElem.getName());
      try {
        queryListElement.addAllChildren(list);
      } catch (QueryException e) {
        e.printStackTrace();
      }
      result.add(queryListElement);
    }
    return result;
  }

  @Override
  public List<QueryStructureElem> visit(QueryTempFilter queryElem) throws QueryException {
    return visitChilds(queryElem);
  }

  @Override
  public List<QueryStructureElem> visit(QueryAnd queryElem) throws QueryException {
    List<QueryStructureElem> result = new ArrayList<QueryStructureElem>();
    List<List<QueryStructureElem>> generatedLists = new ArrayList<List<QueryStructureElem>>();
    for (QueryStructureElem child : queryElem.getChildren()) {
      List<QueryStructureElem> childs = child.accept(this);
      generatedLists.add(childs);
    }

    List<List<QueryStructureElem>> combinations;
    if (queryElem.isPowerSet()) {
      combinations = createPowerSet(generatedLists);
    } else {
      combinations = combineSimple(generatedLists);
    }
    for (List<QueryStructureElem> list : combinations) {
      QueryAnd queryListElement = new QueryAnd(null);
      queryListElement.setName(queryElem.getName());
      try {
        queryListElement.addAllChildren(list);
      } catch (QueryException e) {
        e.printStackTrace();
      }
      result.add(queryListElement);
    }
    return result;
  }

  private List<List<QueryStructureElem>> combineSimple(
          List<List<QueryStructureElem>> generatedLists) {
    List<List<QueryStructureElem>> result = new ArrayList<List<QueryStructureElem>>();
    List<QueryStructureElem> list = new ArrayList<QueryStructureElem>();
    for (List<QueryStructureElem> generatedList : generatedLists) {
      list.addAll(generatedList);
    }
    result.add(list);
    return result;
  }

  private List<List<QueryStructureElem>> createPowerSet(
          List<List<QueryStructureElem>> generatedLists) {
    List<List<QueryStructureElem>> result = new ArrayList<List<QueryStructureElem>>();
    for (List<QueryStructureElem> list : generatedLists) {
      List<List<QueryStructureElem>> concatenation = concatenate(list, result);
      result.addAll(concatenation);
    }
    return result;
  }

  private List<List<QueryStructureElem>> concatenate(List<QueryStructureElem> list1,
          List<List<QueryStructureElem>> list2) {
    List<List<QueryStructureElem>> result = new ArrayList<List<QueryStructureElem>>();
    for (QueryStructureElem e1 : list1) {
      List<QueryStructureElem> l1 = new ArrayList<QueryStructureElem>();
      l1.add(e1);
      result.add(l1);
      for (List<QueryStructureElem> e2 : list2) {
        List<QueryStructureElem> l12 = new ArrayList<QueryStructureElem>();
        l12.add(e1);
        for (QueryStructureElem e : e2)
          l12.add(e);
        result.add(l12);
      }
    }
    return result;
  }

  @Override
  public List<QueryStructureElem> visit(QueryRoot queryElem) throws QueryException {
    filterIDTypeToUseForCount = queryElem.getFilterIDTypeToUseForCount();
    isDistinct = queryElem.isDistinct();
    rootComment = queryElem.getComment();
    return visitChilds(queryElem);
  }

  private List<QueryStructureElem> visitChilds(QueryStructureContainingElem queryElem)
          throws QueryException {
    List<QueryStructureElem> allChilds = new ArrayList<QueryStructureElem>();
    for (QueryStructureElem child : queryElem.getChildren()) {
      List<QueryStructureElem> childs = child.accept(this);
      allChilds.addAll(childs);
    }
    return allChilds;
  }

  public QueryStatisticFilter getStatisticFilter() {
    return statisticFilter;
  }

  public FilterIDType getFilterIDTypeToUseForCount() {
    return filterIDTypeToUseForCount;
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  public String getRootComment() {
    return rootComment;
  }

  public boolean hasFilter() {
    return getStatisticFilter() != null;
  }

  public boolean hasColumns() {
    return columns.size() != 0;
  }

  public List<QueryStructureElem> getColumns() {
    return columns;
  }

  public List<QueryStructureElem> getRows() {
    return rows;
  }

  @Override
  public List<QueryStructureElem> visit(QueryTempOpAbs queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<QueryStructureElem> visit(QueryTempOpRel queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<QueryStructureElem> visit(QueryValueCompare queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<QueryStructureElem> visit(QueryIDFilter idFilter) throws QueryException {
    if (idFilter.getFilterIDType() == FilterIDType.GROUP) {
      try {
        QueryAnd and = new QueryAnd(null);
        QueryManipulationManager manipulator = new QueryManipulationManager();
        manipulator.copySubElements(idFilter, and);
        List<QueryStructureElem> generatedAndList = and.accept(this);
        List<QueryStructureElem> result = new ArrayList<>();
        for (QueryStructureElem generatedElem : generatedAndList) {
          QueryAnd generatedAnd = (QueryAnd) generatedElem;
          QueryIDFilter group = new QueryIDFilter(idFilter.getContainer());
          group.setFilterIDType(FilterIDType.GROUP);
          manipulator.copySubElements(generatedAnd, group);
          result.add(group);
        }
        return result;
      } catch (XMLStreamException | SQLException | ParserConfigurationException | SAXException
               | IOException e) {
        throw new QueryException(e);
      }
    } else {
      return visitChilds(idFilter);
    }
  }
}
