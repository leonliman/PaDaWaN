package de.uniwue.dw.query.model.quickSearch;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.hooks.IDwCatalogHooks;
import de.uniwue.dw.core.model.manager.ICatalogAndTextSuggester;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.ContentOperator;
import de.uniwue.dw.query.model.lang.DisplayStringVisitor;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryIDFilter;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.lang.QueryStatisticColumn;
import de.uniwue.dw.query.model.lang.QueryStatisticFilter;
import de.uniwue.dw.query.model.lang.QueryStatisticRow;
import de.uniwue.dw.query.model.lang.QueryStructureElem;
import de.uniwue.dw.query.model.lang.ReductionOperator;
import de.uniwue.dw.query.model.table.QueryTableEntry.Position;
import de.uniwue.misc.util.ConfigException;

public class QueryRoot2QuickSearchVisitor extends DisplayStringVisitor implements IDwCatalogHooks {

  public static final String QUICK_SEARCH_PERIOD_DELIMITER = "...";

  public static final String QUICK_SEARCH_PER_YEAR_DELIMITER = "-";

  private QueryQuickSearchRepresentation quickSearch = new QueryQuickSearchRepresentation();

  private ICatalogAndTextSuggester suggester;

  public QueryRoot2QuickSearchVisitor(ICatalogAndTextSuggester suggester) {
    this.suggester = suggester;
    QuickSearchPeriod period = new QuickSearchPeriod("", "", ContentOperator.PER_YEAR, true);
    quickSearch.setPeriod(period);
  }

  @Override
  protected String getDisplayText(QueryAttribute queryElem) throws QueryException {
    CatalogEntry entry = queryElem.getCatalogEntry();
    String formattedCatalogName = QuickSearchUtil.formatCatalogEntryWithUniquiNameNotation(entry);
    if (queryElem.getReductionOperator() != ReductionOperator.NONE) {
      formattedCatalogName += " " + queryElem.getReductionOperator().getQuickSearchText();
    }
    String content = queryElem.getDesiredContent().trim().replaceAll("\\s+", " ");
    if (queryElem.getContentOperator() == ContentOperator.EXISTS)
      return formattedCatalogName + " " + content;
    if (queryElem.getContentOperator() == ContentOperator.NOT_EXISTS)
      return "-" + formattedCatalogName + " " + content;
    if (queryElem.getContentOperator() == ContentOperator.EQUALS)
      return formattedCatalogName + " = " + content;
    if (queryElem.getContentOperator() == ContentOperator.LESS)
      return formattedCatalogName + " < " + formatNumber(content);
    if (queryElem.getContentOperator() == ContentOperator.LESS_OR_EQUAL)
      return formattedCatalogName + " <= " + formatNumber(content);
    if (queryElem.getContentOperator() == ContentOperator.MORE)
      return formattedCatalogName + " > " + formatNumber(content);
    if (queryElem.getContentOperator() == ContentOperator.MORE_OR_EQUAL)
      return formattedCatalogName + " >= " + formatNumber(content);
    if (queryElem.getContentOperator() == ContentOperator.PER_YEAR) {
      String[] split = { content };
      if (content.contains(QueryAttribute.PERIOD_DELIMITER))
        split = queryElem.getDesiredContentSplitted();
      // else
      // split = content.split(" ");
      return formattedCatalogName + " " + formatNumber(split[0]) + " "
              + QUICK_SEARCH_PER_YEAR_DELIMITER + " " + formatNumber(split[1]);
    }
    if (queryElem.getContentOperator() == ContentOperator.BETWEEN) {
      String[] split = queryElem.getDesiredContentSplitted();
      return formattedCatalogName + " " + formatNumber(split[0]) + QUICK_SEARCH_PERIOD_DELIMITER
              + formatNumber(split[1]);
    }
    if (queryElem.getContentOperator() == ContentOperator.CONTAINS)
      return "in:" + formattedCatalogName + " " + QuickSearchUtil.formatTextQueryTerms(content);
    if (queryElem.getContentOperator() == ContentOperator.CONTAINS_POSITIVE)
      return "in:" + formattedCatalogName + " + " + QuickSearchUtil.formatTextQueryTerms(content);
    if (queryElem.getContentOperator() == ContentOperator.CONTAINS_NOT)
      return "-in:" + formattedCatalogName + " " + QuickSearchUtil.formatTextQueryTerms(content);
    if (queryElem.getContentOperator() == ContentOperator.CONTAINS_NOT_POSITIVE)
      return "-in:" + formattedCatalogName + " + " + QuickSearchUtil.formatTextQueryTerms(content);

    return formattedCatalogName;
  }

  @Override
  public String visit(QueryRoot queryElem) throws QueryException {
    quickSearch.setPatientQuery(queryElem.isDistinct());
    quickSearch.setDistributionQuery(queryElem.isStatisticQuery());
    quickSearch.setPreviewRows(queryElem.getLimitResult());
    if (queryElem.isStatisticQuery())
      return super.visit(queryElem);
    else {
      for (QueryStructureElem ele : queryElem.getChildren()) {
        analyseQueryElem(ele, Position.row);
      }
      return null;
    }

  }

  @Override
  public String visit(QueryStatisticRow queryElem) throws QueryException {
    for (QueryStructureElem ele : queryElem.getChildren()) {
      analyseQueryElem(ele, Position.row);
    }
    return null;
  }

  @Override
  public String visit(QueryStatisticColumn queryElem) throws QueryException {
    for (QueryStructureElem ele : queryElem.getChildren()) {
      analyseQueryElem(ele, Position.column);
    }
    return null;
  }

  @Override
  public String visit(QueryStatisticFilter queryElem) throws QueryException {
    for (QueryStructureElem ele : queryElem.getChildren()) {
      analyseQueryElem(ele, Position.filter);
    }
    return null;
  }

  private void analyseQueryElem(QueryStructureElem ele, Position position) throws QueryException {
    if (ele.getClass() == QueryIDFilter.class) {
      for (QueryStructureElem child : ((QueryIDFilter) ele).getChildren())
        analyseQueryElem(child, position);
    } else {
      if (isTimePeriodElem(ele) && quickSearch.getPeriod() == null) {
        setTimePeriod(ele);
      } else {
        String queryText = ele.accept(this);
        QuickSearchLine line = new QuickSearchLine(queryText, position);
        boolean isResultRow = shouldBeDisplayed(ele);
        line.setDisplayInResult(isResultRow);
        line.setFilterUnknown(ele.isFilterUnkown());
        line.setFilter(!ele.isOptional());
        quickSearch.addQueryLine(line);
      }
    }
  }

  private boolean shouldBeDisplayed(QueryStructureElem ele) {
    if (ele instanceof QueryAttribute) {
      if (((QueryAttribute) ele).displayValue())
        return true;

    }
    for (QueryStructureElem son : ele.getChildren()) {
      if (shouldBeDisplayed(son))
        return true;
    }
    return false;
  }

  private void setTimePeriod(QueryStructureElem ele) throws QueryException {
    if (ele instanceof QueryAttribute) {
      QueryAttribute attr = (QueryAttribute) ele;
      QuickSearchPeriod period = new QuickSearchPeriod();
      String argument = attr.getDesiredContent();
      if (argument != null && !argument.isEmpty()) {
        String[] split = attr.getDesiredContentSplitted();
        period.setFrom(split[0]);
        if (split.length == 2)
          period.setTo(split[1]);
        period.setUnit(attr.getContentOperator());
        boolean isColumn = ele.getParent() instanceof QueryStatisticColumn;
        period.setColumn(isColumn);
        quickSearch.setPeriod(period);
      }
    }
  }

  private boolean isTimePeriodElem(QueryStructureElem ele) {
    if (ele instanceof QueryAttribute) {
      QueryAttribute attr = (QueryAttribute) ele;
      try {
        if (attr.getCatalogEntry().equals(getAdmissionDateCatalogEntry())) {
          return attr.getContentOperator() == ContentOperator.PER_YEAR
                  || attr.getContentOperator() == ContentOperator.PER_MONTH;
        }
      } catch (ConfigException e) {
        e.printStackTrace();
      }
    }
    return false;
  }

  private CatalogEntry getAdmissionDateCatalogEntry() throws ConfigException {
    return suggester.getDocumentTimeCatalogEntry();
  }

  public QueryQuickSearchRepresentation getQueryQuickSearchRepresentation() {
    return quickSearch;
  }

}
