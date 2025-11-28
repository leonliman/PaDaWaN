package de.uniwue.dw.query.model.quickSearch;

import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.hooks.IDwCatalogHooks;
import de.uniwue.dw.core.model.manager.ICatalogAndTextSuggester;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryIDFilter;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.lang.QueryStatisticColumn;
import de.uniwue.dw.query.model.lang.QueryStatisticFilter;
import de.uniwue.dw.query.model.lang.QueryStatisticRow;
import de.uniwue.dw.query.model.lang.QueryStructureContainingElem;
import de.uniwue.dw.query.model.lang.QueryStructureElem;
import de.uniwue.dw.query.model.lang.ReductionOperator;
import de.uniwue.dw.query.model.manager.IQueryClientIOManager;
import de.uniwue.dw.query.model.manager.QueryManipulationManager;
import de.uniwue.dw.query.model.table.QueryTableEntry.Position;
import de.uniwue.misc.util.ConfigException;

public class ModelConverter implements IDwCatalogHooks {

  public static final String STORED_QUERY_PREFIX = "Anfrage:";

  private static Logger logger = LogManager.getLogger(ModelConverter.class);

  public static QueryQuickSearchRepresentation queryRoot2QuickSearchRepresentation(
          QueryRoot queryRoot, ICatalogAndTextSuggester suggester) throws QueryException {

    QueryRoot2QuickSearchVisitor visitor = new QueryRoot2QuickSearchVisitor(suggester);
    queryRoot.accept(visitor);
    return visitor.getQueryQuickSearchRepresentation();
  }

  public static QueryRoot quickSearchRepresentation2queryRoot(QueryQuickSearchRepresentation query,
          User user, ICatalogAndTextSuggester catalogSuggester,
          IQueryClientIOManager queryClientIOManager) throws QueryException {
    QueryRoot queryRoot = new QueryRoot();
    QueryIDFilter caseFilter = new QueryIDFilter(queryRoot);
    caseFilter.setFilterIDType(FilterIDType.CaseID);
    // queryRoot.setVersion("0.6");
    queryRoot.setDistinct(query.isPatientQuery());
    queryRoot.setLimitResult(query.getPreviewRows());
    // if (queryHasAPeriod(query))
    // addTimePeriodToQuery(queryRoot, query.isDistributionQuery(), query.getPeriod(),
    // catalogSuggester);
    addQueryLinesToQuery(caseFilter, query.isDistributionQuery(), query.getLines(), user,
            catalogSuggester, queryClientIOManager);
    // queryRoot.shrink(new QueryManipulationManager());
    QueryManipulationManager.shrinkQuery(queryRoot);
    return queryRoot;
  }

  private static boolean queryHasAPeriod(QueryQuickSearchRepresentation query) {
    return query.getPeriod() != null && !query.getPeriod().getFrom().isEmpty()
            && !query.getPeriod().getTo().isEmpty();
  }

  private static void addTimePeriodToQuery(QueryRoot queryRoot, boolean isStatQuery,
          QuickSearchPeriod period, ICatalogAndTextSuggester catalogSuggester)
          throws QueryException {
    CatalogEntry admissionDate;
    try {
      admissionDate = getAdmissionDateCatalogEntry(catalogSuggester);
      String argument = period.getFrom() + " " + QueryAttribute.PERIOD_DELIMITER + " "
              + period.getTo();
      argument = argument.replaceAll("\\s+", " ");
      QueryAttribute queryElem = new QueryAttribute(admissionDate, period.getUnit(), argument,
              ReductionOperator.NONE);
      if (isStatQuery)
        addElem2StatQuery(queryRoot, queryElem, period.getPosition());
      else
        queryRoot.addChild(queryElem);
    } catch (ConfigException e) {
      e.printStackTrace();
    }
  }

  private static CatalogEntry getAdmissionDateCatalogEntry(
          ICatalogAndTextSuggester catalogSuggester) throws ConfigException {
    return catalogSuggester.getDocumentTimeCatalogEntry();
  }

  private static QueryStructureContainingElem addElem2StatQuery(
          QueryStructureContainingElem elemContainer, QueryStructureElem elem, Position position)
          throws QueryException {
    QueryStructureContainingElem positionList = getPositionElem(elemContainer, position);
    positionList.addChild(elem);
    return elemContainer;
  }

  private static QueryStructureContainingElem getPositionElem(
          QueryStructureContainingElem elemContainer, Position position) {
    QueryStructureContainingElem list = null;
    switch (position) {
      case row:
        list = (QueryStructureContainingElem) getChildOfClassType(elemContainer,
                QueryStatisticRow.class);
        if (list == null)
          list = new QueryStatisticRow(elemContainer);
        return list;
      case column:
        list = (QueryStructureContainingElem) getChildOfClassType(elemContainer,
                QueryStatisticColumn.class);
        if (list == null)
          list = new QueryStatisticColumn(elemContainer);
        return list;
      case filter:
        list = (QueryStructureContainingElem) getChildOfClassType(elemContainer,
                QueryStatisticFilter.class);
        if (list == null)
          list = new QueryStatisticFilter(elemContainer);
        return list;
    }
    System.err.println("Unkown state of Position: " + position);
    return new QueryStatisticRow(elemContainer);
  }

  private static QueryStructureElem getChildOfClassType(QueryStructureContainingElem elemContainer,
          @SuppressWarnings("rawtypes") Class c) {
    for (QueryStructureElem child : elemContainer.getChildren()) {
      if (child.getClass() == c) {
        return child;
      }
    }
    return null;
  }

  private static void addQueryLinesToQuery(QueryStructureContainingElem elemContainer,
          boolean isStatQuery, List<QuickSearchLine> lines, User user,
          ICatalogAndTextSuggester catalogSuggester, IQueryClientIOManager queryClientIOManager)
          throws QueryException {
    HashMap<Integer, QueryIDFilter> groupListDocId = new HashMap<Integer, QueryIDFilter>();
    HashMap<Integer, QueryIDFilter> groupListCaseId = new HashMap<Integer, QueryIDFilter>();

    for (QuickSearchLine line : lines) {
      QueryStructureElem queryElem = QuickSearchAPI.parse(line, user, catalogSuggester,
              queryClientIOManager);
      Integer caseId = line.getCaseId();
      Integer docId = line.getDocId();
      if (isStatQuery)
        addElem2StatQuery(elemContainer, queryElem, line.getPosition());
      else if (caseId != null || docId != null) {
        if (caseId != null) {
          QueryIDFilter filter = groupListCaseId.get(caseId);
          if (filter == null) {
            filter = new QueryIDFilter(elemContainer);
            filter.setFilterIDType(FilterIDType.CaseID);
            groupListCaseId.put(caseId, filter);
          }
          filter.addChild(queryElem);
        } else {
          QueryIDFilter filter = groupListDocId.get(docId);
          if (filter == null) {
            filter = new QueryIDFilter(elemContainer);
            filter.setFilterIDType(FilterIDType.DocID);
            groupListDocId.put(docId, filter);
          }
          filter.addChild(queryElem);
        }
      } else
        elemContainer.addChild(queryElem);
    }

  }

}
