package de.uniwue.dw.query.model;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.query.model.client.DefaultQueryRunner;
import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.*;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;
import de.uniwue.dw.query.model.manager.QueryManipulationManager;
import de.uniwue.dw.query.model.result.PatientQueryRunnable;
import de.uniwue.dw.query.model.result.Result;
import de.uniwue.dw.query.model.result.Row;
import de.uniwue.dw.query.model.result.StatisticalCell;
import de.uniwue.dw.query.model.result.StatisticalCell.StatisticalQueryType;
import de.uniwue.dw.query.model.result.export.ExportConfiguration;
import de.uniwue.dw.query.model.visitor.StatisticQueryVisitor;
import de.uniwue.misc.util.thread.IUniWueProgressMonitorAdapter;
import org.apache.commons.math3.exception.NotStrictlyPositiveException;
import org.apache.commons.math3.linear.BlockRealMatrix;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.correlation.SpearmansCorrelation;
import org.springframework.util.StopWatch;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class StatisticQueryResultCreationHelper {

  private final StatisticQueryVisitor statisticsVisitor;

  private final User user;

  private final DefaultQueryRunner queryRunner;

  private final ExportConfiguration exportConfiguration;

  protected List<Map<Long, Double>> columnQueryResults;

  protected List<Map<Long, Double>> rowQueryResults;

  private int caseUnionIndex = 1;

  private QueryRoot queryRoot;

  private Result result;

  private List<QueryRoot> columnQueriesForCorrelationAnalysis;

  private List<QueryRoot> rowQueriesForCorrelationAnalysis;

  private FilterIDType countTypeToUseForCorrelationAnalysis;

  public StatisticQueryResultCreationHelper(QueryRoot queryRoot, User user, DefaultQueryRunner aQueryRunner,
          ExportConfiguration exportConfig)
          throws QueryException {
    this.queryRoot = queryRoot;
    this.user = user;
    this.queryRunner = aQueryRunner;
    this.exportConfiguration = exportConfig;
    statisticsVisitor = new StatisticQueryVisitor();
    statisticsVisitor.visit(queryRoot);
    createQueryStrings();
    if (statisticsVisitor.hasFilter()) {
      caseUnionIndex++;
    }
  }

  private static QueryRoot createQuery(FilterIDType filterIDTypeToUseForCount, boolean isDistinct,
          List<QueryStructureElem> elems, QueryStructureElem... morElems) throws QueryException {
    ArrayList<QueryStructureElem> queryAnds = new ArrayList<>(elems);
    queryAnds.addAll(Arrays.asList(morElems));
    return createQuery(filterIDTypeToUseForCount, isDistinct, queryAnds);
  }

  private static QueryRoot createQuery(FilterIDType filterIDTypeToUseForCount, boolean isDistinct,
          List<QueryStructureElem> elems) throws QueryException {
    try {
      QueryManipulationManager qmm = new QueryManipulationManager();
      List<QueryStructureElem> deepCopy = qmm.cloneElem(elems);
      QueryRoot root = new QueryRoot();
      root.setOnlyCount(true);
      QueryIDFilter containingElem = new QueryIDFilter(root);
      if (isDistinct && DWQueryConfig.queryAlwaysGroupDistinctQueriesOnDocLevel()) {
        containingElem.setFilterIDType(FilterIDType.DocID);
        root.setFilterIDTypeForDistinctCount(filterIDTypeToUseForCount);
      } else {
        containingElem.setFilterIDType(filterIDTypeToUseForCount);
      }
      root.setDistinct(isDistinct);
      containingElem.addAllChildren(deepCopy);
      return root;
    } catch (IOException | XMLStreamException | ParserConfigurationException | SAXException e) {
      throw new QueryException(e);
    }
  }

  public static QueryRoot getCalcUnionCountQuery(FilterIDType filterIDTypeToUseForCount, boolean isDistinct,
          List<QueryStructureElem> columns, QueryStructureElem... filter) throws QueryException {
    return getCalcUnionCountQuery(filterIDTypeToUseForCount, isDistinct, columns, Arrays.asList(filter));
  }

  public static QueryRoot getCalcUnionCountQuery(FilterIDType filterIDTypeToUseForCount, boolean isDistinct,
          List<QueryStructureElem> columns, List<QueryStructureElem> filters)
          throws QueryException {
    try {
      QueryManipulationManager qmm = new QueryManipulationManager();
      List<QueryStructureElem> copiedColumns = qmm.cloneElem(columns);
      List<QueryStructureElem> copiedFilters = qmm.cloneElem(filters);

      QueryRoot root = new QueryRoot();
      root.setOnlyCount(true);
      QueryIDFilter containingElem = new QueryIDFilter(root);
      if (isDistinct && DWQueryConfig.queryAlwaysGroupDistinctQueriesOnDocLevel()) {
        containingElem.setFilterIDType(FilterIDType.DocID);
        root.setFilterIDTypeForDistinctCount(filterIDTypeToUseForCount);
      } else {
        containingElem.setFilterIDType(filterIDTypeToUseForCount);
      }
      root.setDistinct(isDistinct);
      QueryOr or = new QueryOr(containingElem);
      or.addAllChildren(copiedColumns);
      containingElem.addAllChildren(copiedFilters);
      return root;
    } catch (IOException | XMLStreamException | ParserConfigurationException | SAXException e) {
      throw new QueryException(e);
    }
  }

  public static QueryRoot getCalcUnionCountQuery(FilterIDType filterIDTypeToUseForCount, boolean isDistinct,
          List<QueryStructureElem> columns, List<QueryStructureElem> filters,
          QueryStructureElem... moreFilters) throws QueryException {
    ArrayList<QueryStructureElem> queryAnds = new ArrayList<>(filters);
    queryAnds.addAll(Arrays.asList(moreFilters));
    return getCalcUnionCountQuery(filterIDTypeToUseForCount, isDistinct, columns, queryAnds);
  }

  private static BlockRealMatrix getMatrixForOverlappingPIDs(Map<Long, Double> rowResult,
          Map<Long, Double> columnResult) {
    List<Double> rowValuesToKeep = new ArrayList<>();
    List<Double> columnValuesToKeep = new ArrayList<>();
    for (Long rowKey : rowResult.keySet()) {
      if (columnResult.containsKey(rowKey)) {
        rowValuesToKeep.add(rowResult.get(rowKey));
        columnValuesToKeep.add(columnResult.get(rowKey));
      }
    }
    if (rowValuesToKeep.isEmpty() || columnValuesToKeep.isEmpty()) {
      return null;
    }
    double[][] matrix = new double[rowValuesToKeep.size()][2];
    for (int i = 0; i < rowValuesToKeep.size(); i++) {
      matrix[i][0] = rowValuesToKeep.get(i);
      matrix[i][1] = columnValuesToKeep.get(i);
    }
    return new BlockRealMatrix(matrix);
  }

  /**
   * Returns the header with the column names.
   */
  public List<String> getColumnNames() {
    return result.getHeader();
  }

  /**
   * Returns the names of the rows. The header with the column names is not a row and is not
   * included in this method.
   */
  public List<String> getRowNames() throws GUIClientException {
    ArrayList<String> rowNames = new ArrayList<>();
    for (Row row : result.getRows()) {
      Object rowNameObj = computeCellValue(getCell(row, 0));
      String rowName = rowNameObj != null ? rowNameObj.toString() : "";
      rowNames.add(rowName);
    }
    return rowNames;
  }

  public StatisticalCell getCell(int column, int row) {
    return (StatisticalCell) result.getCell(column, row);
  }

  public StatisticalCell getCell(Row row, int column) {
    return (StatisticalCell) row.getCell(column);
  }

  /**
   * Returns a cell value and computes it, if it is necessary. The header with the column names is
   * not part of the table.
   *
   * @param column index of the column. Starts with 0. If your table has got row names, then they are in
   *               column 0.
   * @param row    index of the row. Starts with 0. The row 0 is the first data row, Not the header.
   * @return value of the cell
   */
  public Object getCellValue(int column, int row) throws GUIClientException {
    StatisticalCell cell = getCell(row, column);
    return computeCellValue(cell);
  }

  private Object computeCellValue(StatisticalCell cell) throws GUIClientException {
    Object value = cell.value;
    if (value == null) {
      if (cell.resultType == StatisticalQueryType.SUM) {
        value = calcSum(cell);
      } else if (cell.resultType == StatisticalQueryType.SUM_MINUS_CASE_UNION) {
        value = calcSumMinusUnion(cell);
      } else if (cell.resultType == StatisticalQueryType.QUERY_ROOT) {
        // as the new runnable does not need to export anything it does not need an exportConfig
        PatientQueryRunnable queryRunnable = queryRunner.runQueryInternal(cell.queryRoot, user,
                null);
        value = queryRunnable.getOnlyCountNumber();
      } else {
        value = "";
      }
      cell.value = value;
    }
    return value;
  }

  private Number calcSumMinusUnion(StatisticalCell cell) throws GUIClientException {
    int columnNumber = cell.getColumnNumber();

    computeCellValue(getCell(cell.getRow(), caseUnionIndex));
    computeCellValue(getCell(cell.getRow(), columnNumber - 1));
    String caseUnionString = getCell(cell.getRow(), caseUnionIndex).value.toString();
    String sumString = getCell(cell.getRow(), columnNumber - 1).value.toString();

    if (caseUnionString.matches("\\d+") && sumString.matches("\\d+")) {
      int caseUnion = Integer.parseInt(caseUnionString);
      int sum = Integer.parseInt(sumString);
      return sum - caseUnion;
    }
    return Double.NaN;
  }

  private Object calcSum(StatisticalCell cell) throws GUIClientException {
    int colNumber = cell.getColumnNumber();
    int firstAddendIndex = caseUnionIndex + 1;
    int sum = 0;
    for (int i = firstAddendIndex; i < colNumber; i++) {
      StatisticalCell addend = (StatisticalCell) cell.getRow().getCell(i);
      computeCellValue(addend);
      String numberValue = addend.value.toString();
      if (numberValue.matches("\\d+")) {
        int count = Integer.parseInt(numberValue);
        sum += count;
      }
    }
    return sum;
  }

  /**
   * Returns the size of the columns.
   */
  public int getColumns() {
    return result.getHeader().size();
  }

  /**
   * Returns the size of the data rows. The header with the column names is excluded.
   */
  public int getRows() {
    return result.getRows().size();
  }

  /**
   * Returns the size of data cells, excluding the header with the column names.
   */
  public int getCellCount() {
    return getColumns() * getRows();
  }

  protected void createQueryStrings() throws QueryException {
    String filterText = DisplayStringVisitor
            .getDisplayString(statisticsVisitor.getStatisticFilter());
    filterText = filterText.isEmpty() ? "" : " (" + filterText + ")";
    String filterTextStart = exportConfiguration.getStatisticsAllColumnName();
    if (DWQueryConfig.getInstance().getBooleanParameter("dw.index.neo4j.useKIRaImport", false))
      filterTextStart = exportConfiguration.getFilterIDTypeStraighteningStepsName();
    if (!filterText.isEmpty())
      filterText = filterTextStart + filterText;
    else
      filterText = filterTextStart;

    result = new Result();
    result.addColumn("");
    boolean addAdditionalCalculations = exportConfiguration.isExcelIncludeTotalAndSumRowsAndColumns();
    boolean performCorrelationAnalysis = statisticsVisitor.getRootComment() != null &&
            statisticsVisitor.getRootComment().startsWith("#CorrelationAnalysis#");
    if (DWQueryConfig.getInstance().getBooleanParameter("dw.index.neo4j.useKIRaImport", false)) {
      addAdditionalCalculations = false;
    }
    if (statisticsVisitor.hasFilter() && addAdditionalCalculations) {
      result.addColumn(filterText);
    }
    if (!statisticsVisitor.hasColumns() || addAdditionalCalculations) {
      result.addColumn(getUnderlyingSetUnit());
    }
    QueryStatisticFilter filterElem = statisticsVisitor.getStatisticFilter();
    for (QueryStructureElem columnElement : statisticsVisitor.getColumns()) {
      result.addColumn(DisplayStringVisitor.getDisplayString(columnElement));
    }
    if (statisticsVisitor.hasColumns() && addAdditionalCalculations) {
      result.addColumn(exportConfiguration.getStatisticsSumColumnName());
      result.addColumn(exportConfiguration.getStatisticsDuplicatesColumnName());
    }

    FilterIDType filterIDTypeToUseForCount = statisticsVisitor.getFilterIDTypeToUseForCount();
    boolean isDistinct = statisticsVisitor.isDistinct();
    if ((statisticsVisitor.getRows() != null && statisticsVisitor.getRows().isEmpty()) || addAdditionalCalculations) {
      createAllRow(filterIDTypeToUseForCount, isDistinct, filterText, filterElem, addAdditionalCalculations);
    }
    createNormalRows(filterIDTypeToUseForCount, isDistinct, filterElem, addAdditionalCalculations);

    if (performCorrelationAnalysis)
      createCorrelationAnalysisQueries(filterIDTypeToUseForCount, isDistinct, filterElem);
  }

  private void createCorrelationAnalysisQueries(FilterIDType filterIDTypeToUseForCount, boolean isDistinct,
          QueryStatisticFilter filterElem) throws QueryException {
    rowQueriesForCorrelationAnalysis = new ArrayList<>();
    columnQueriesForCorrelationAnalysis = new ArrayList<>();
    countTypeToUseForCorrelationAnalysis = filterIDTypeToUseForCount;
    List<QueryStructureElem> filterList = new ArrayList<>();
    if (statisticsVisitor.hasFilter()) {
      filterList = filterElem.getChildren();
    }
    for (QueryStructureElem rowElement : statisticsVisitor.getRows()) {
      QueryRoot rowQueryRoot = createQuery(filterIDTypeToUseForCount, isDistinct, filterList, rowElement);
      int numWithDisplayValue = prepareQueryRootForCorrelationAnalysis(rowQueryRoot, filterIDTypeToUseForCount);
      if (numWithDisplayValue > 0)
        rowQueriesForCorrelationAnalysis.add(rowQueryRoot);
      else
        rowQueriesForCorrelationAnalysis.add(null);
    }
    for (QueryStructureElem columnElement : statisticsVisitor.getColumns()) {
      QueryRoot columnQueryRoot = createQuery(filterIDTypeToUseForCount, isDistinct, filterList, columnElement);
      int numWithDisplayValue = prepareQueryRootForCorrelationAnalysis(columnQueryRoot, filterIDTypeToUseForCount);
      if (numWithDisplayValue > 0)
        columnQueriesForCorrelationAnalysis.add(columnQueryRoot);
      else
        columnQueriesForCorrelationAnalysis.add(null);
    }
    if (rowQueriesForCorrelationAnalysis.stream().filter(Objects::nonNull).toList().isEmpty() ||
            columnQueriesForCorrelationAnalysis.stream().filter(Objects::nonNull).toList().isEmpty()) {
      rowQueriesForCorrelationAnalysis = null;
      columnQueriesForCorrelationAnalysis = null;
    }
  }

  public int getNumberOfCorrelationAnalysisQueries() {
    if (rowQueriesForCorrelationAnalysis != null && columnQueriesForCorrelationAnalysis != null)
      return rowQueriesForCorrelationAnalysis.stream().filter(Objects::nonNull).toList().size() +
              columnQueriesForCorrelationAnalysis.stream().filter(Objects::nonNull).toList().size();
    return 0;
  }

  public String[][] runCorrelationAnalysis(StopWatch queryStopWatch, StopWatch correlationStopWatch, int worked,
          IUniWueProgressMonitorAdapter monitor)
          throws GUIClientException {
    if (rowQueriesForCorrelationAnalysis == null || columnQueriesForCorrelationAnalysis == null)
      return null;
    rowQueryResults = null;
    columnQueryResults = null;
    rowQueryResults = runQueriesForCorrelationAnalysis(
            rowQueriesForCorrelationAnalysis, queryStopWatch, worked, monitor, 0,
            getNumberOfCorrelationAnalysisQueries());
    columnQueryResults = runQueriesForCorrelationAnalysis(
            columnQueriesForCorrelationAnalysis, queryStopWatch, worked, monitor,
            rowQueriesForCorrelationAnalysis.stream().filter(Objects::nonNull).toList().size(),
            getNumberOfCorrelationAnalysisQueries());
    String[][] correlationWithPValues = new String[rowQueriesForCorrelationAnalysis.size()][columnQueriesForCorrelationAnalysis.size()];
    for (int i = 0; i < rowQueriesForCorrelationAnalysis.size(); i++) {
      for (int j = 0; j < columnQueriesForCorrelationAnalysis.size(); j++) {
        Map<Long, Double> rowResult = rowQueryResults.get(i);
        Map<Long, Double> columnResult = columnQueryResults.get(j);
        if (rowResult == null || columnResult == null) {
          correlationWithPValues[i][j] = "-";
          continue;
        }
        if (rowResult.isEmpty() || columnResult.isEmpty()) {
          correlationWithPValues[i][j] = "0";
          continue;
        }
        try {
          correlationStopWatch.start("Correlation analysis for row " + i + " and column " + j);
          BlockRealMatrix realMatrix = getMatrixForOverlappingPIDs(rowResult, columnResult);
          if (realMatrix == null) {
            correlationWithPValues[i][j] = "0";
            correlationStopWatch.stop();
            continue;
          } else if (realMatrix.getRowDimension() < 2 || realMatrix.getColumnDimension() < 2) {
            correlationWithPValues[i][j] = "1";
            correlationStopWatch.stop();
            continue;
          }
          PearsonsCorrelation pearsonsCorrelation = getCorrelationForMatrix(realMatrix);
          double correlation = pearsonsCorrelation.getCorrelationMatrix().getEntry(0, 1);
          try {
            double pValue = pearsonsCorrelation.getCorrelationPValues().getEntry(0, 1);
            String formattedCorrelationWithPValue = String.format("%,d; %.4f (%.6f)", realMatrix.getRowDimension(),
                    correlation, pValue);
            correlationWithPValues[i][j] = formattedCorrelationWithPValue;
          } catch (NotStrictlyPositiveException e) {
            correlationWithPValues[i][j] = String.format("%,d; %.4f", realMatrix.getRowDimension(), correlation);
          }
          correlationStopWatch.stop();
        } catch (Exception e) {
          throw new GUIClientException(e);
        }
      }
    }
    return correlationWithPValues;
  }

  private PearsonsCorrelation getCorrelationForMatrix(BlockRealMatrix realMatrix) {
    SpearmansCorrelation spearmansCorrelation = new SpearmansCorrelation(realMatrix);
    return spearmansCorrelation.getRankCorrelation();
  }

  private List<Map<Long, Double>> runQueriesForCorrelationAnalysis(List<QueryRoot> queries,
          StopWatch stopWatch, int worked, IUniWueProgressMonitorAdapter monitor, int numOfExecutedQueries,
          int totalNumOfQueriesToExecute) throws GUIClientException {
    List<Map<Long, Double>> results = new ArrayList<>();
    for (QueryRoot query : queries) {
      if (query == null) {
        results.add(null);
        continue;
      }
      numOfExecutedQueries++;
      stopWatch.start("Query number " + numOfExecutedQueries + " of " + totalNumOfQueriesToExecute +
              " for the correlation analysis");

      Set<FilterIDType> supportedFilterIDTypes = Set.of(FilterIDType.PID, FilterIDType.CaseID, FilterIDType.DocID);
      if (!supportedFilterIDTypes.contains(countTypeToUseForCorrelationAnalysis))
        throw new GUIClientException("The configured count type (" + countTypeToUseForCorrelationAnalysis + ") " +
                "is not supported for the correlation analysis. Supported types are: " + supportedFilterIDTypes + ".");

      int queryID = queryRunner.createQuery(query, user);
      Result result = queryRunner.runQueryBlocking(queryID);
      Map<Long, Double> curResult = new HashMap<>();
      for (Row row : result.getRows()) {
        List<Information> curInfos = row.getCell(1).getCellData().getValues();
        if (curInfos.size() != 1)
          throw new GUIClientException(
                  "The data returned by queries used for the correlation analysis must consist of exactly one value per element of the selected count type (" +
                          countTypeToUseForCorrelationAnalysis + ")");
        Information curInfo = curInfos.get(0);
        long idToUse = countTypeToUseForCorrelationAnalysis == FilterIDType.CaseID ? curInfo.getCaseID()
                : countTypeToUseForCorrelationAnalysis == FilterIDType.DocID ? curInfo.getDocID() : curInfo.getPid();
        if (idToUse < 1)
          throw new GUIClientException(
                  "The data returned by queries used for the correlation analysis must contain IDs for the selected count type (" +
                          countTypeToUseForCorrelationAnalysis + ")");
        if (curResult.containsKey(idToUse))
          throw new GUIClientException(
                  "The data returned by queries used for the correlation analysis must return exactly one row per element of the selected count type (" +
                          countTypeToUseForCorrelationAnalysis + ")");
        Double value = curInfo.getValueDec();
        if (value == null)
          throw new GUIClientException(
                  "The queries used for the correlation analysis must return numeric values for their attributes selected for the correlation analysis");
        curResult.put(idToUse, value);
      }
      results.add(curResult);
      monitor.worked(worked);
      stopWatch.stop();
    }
    return results;
  }

  private int prepareQueryRootForCorrelationAnalysis(QueryRoot queryRoot, FilterIDType filterIDTypeToUseForCount)
          throws QueryException {
    queryRoot.setOnlyCount(false);
    queryRoot.setDisplayPID(true);
    queryRoot.setLimitResult(0);
    List<QueryAttribute> attributes = queryRoot.getAttributesRecursive();
    int numWithDisplayValue = 0;
    for (QueryAttribute curAttribute : attributes) {
      String curAttributeComment = curAttribute.getComment();
      if (curAttributeComment == null || !curAttributeComment.startsWith("#N4JCorr#")) {
        curAttribute.setDisplayValue(false);
      } else {
        numWithDisplayValue++;
        if (filterIDTypeToUseForCount == FilterIDType.CaseID) {
          curAttribute.setDisplayCaseID(true);
        } else if (filterIDTypeToUseForCount == FilterIDType.DocID) {
          curAttribute.setDisplayDocID(true);
        }
      }
    }
    if (numWithDisplayValue > 1)
      throw new QueryException("For each row and column of the query not more than one attribute may be used for " +
              "the correlation analysis");
    return numWithDisplayValue;
  }

  private void createNormalRows(FilterIDType filterIDTypeToUseForCount, boolean isDistinct,
          QueryStatisticFilter filterElem, boolean addAdditionalCalculations)
          throws QueryException {
    if (statisticsVisitor.getRows() != null) {
      List<QueryStructureElem> filterList = statisticsVisitor.hasFilter() ? filterElem.getChildren()
              : new ArrayList<>();
      for (QueryStructureElem rowElement : statisticsVisitor.getRows()) {
        Row row = result.createNewRow();
        row.createNewStatisticalCell().value = DisplayStringVisitor.getDisplayString(rowElement);
        if (statisticsVisitor.hasFilter() && addAdditionalCalculations) {
          StatisticalCell cell = row.createNewStatisticalCell();
          QueryRoot rowQueryRoot = createQuery(filterIDTypeToUseForCount, isDistinct, filterList, rowElement);
          setQueryRoot(cell, rowQueryRoot);
        }

        if (!statisticsVisitor.hasColumns() || addAdditionalCalculations) {
          StatisticalCell unionCountCell = row.createNewStatisticalCell();
          QueryRoot getCalcUnionCountQuery = getCalcUnionCountQuery(filterIDTypeToUseForCount, isDistinct,
                  statisticsVisitor.getColumns(), filterList, rowElement);
          setQueryRoot(unionCountCell, getCalcUnionCountQuery);
        }

        for (QueryStructureElem columnElement : statisticsVisitor.getColumns()) {
          StatisticalCell cell = row.createNewStatisticalCell();
          QueryRoot rowColFilterQuery = createQuery(filterIDTypeToUseForCount, isDistinct, filterList, rowElement,
                  columnElement);
          setQueryRoot(cell, rowColFilterQuery);
        }
        if (statisticsVisitor.hasColumns() && addAdditionalCalculations) {
          createSumCell(row.createNewStatisticalCell());
          createSumMinusCaseUnionCell(row.createNewStatisticalCell());
        }
      }
    }
  }

  private void setQueryRoot(StatisticalCell cell, QueryRoot queryRoot) throws QueryException {
    cell.resultType = StatisticalQueryType.QUERY_ROOT;
    cell.queryRoot = queryRoot;
    cell.toolTip = createTooltip(queryRoot);
  }

  private void createAllRow(FilterIDType filterIDTypeToUseForCount, boolean isDistinct, String filterText,
          QueryStatisticFilter filterElem, boolean addAdditionalCalculations) throws QueryException {
    Row allRow = result.createNewRow();
    allRow.createNewStatisticalCell().value = filterText;
    List<QueryStructureElem> filterList = new ArrayList<>();
    if (statisticsVisitor.hasFilter()) {
      filterList = filterElem.getChildren();
      if (addAdditionalCalculations) {
        StatisticalCell filterCell = allRow.createNewStatisticalCell();
        QueryRoot filterQuery = createQuery(filterIDTypeToUseForCount, isDistinct, filterList);
        setQueryRoot(filterCell, filterQuery);
      }
    }

    if (!statisticsVisitor.hasColumns() || addAdditionalCalculations) {
      QueryRoot casesUnionCountQueryRoot = getCalcUnionCountQuery(filterIDTypeToUseForCount, isDistinct,
              statisticsVisitor.getColumns(), filterList);

      StatisticalCell casesUnionCountCell = allRow.createNewStatisticalCell();
      setQueryRoot(casesUnionCountCell, casesUnionCountQueryRoot);
    }

    for (QueryStructureElem columnElement : statisticsVisitor.getColumns()) {
      StatisticalCell cell = allRow.createNewStatisticalCell();
      QueryRoot columnsQuery = createQuery(filterIDTypeToUseForCount, isDistinct, filterList, columnElement);
      setQueryRoot(cell, columnsQuery);
    }
    if (statisticsVisitor.hasColumns() && addAdditionalCalculations) {
      createSumCell(allRow.createNewStatisticalCell());
      createSumMinusCaseUnionCell(allRow.createNewStatisticalCell());
    }
  }

  private void createSumMinusCaseUnionCell(StatisticalCell cell) {
    cell.resultType = StatisticalQueryType.SUM_MINUS_CASE_UNION;
    cell.toolTip = "Differenz der Spalten 'Summe' und '" + getUnderlyingSetUnit() + "'";
  }

  private void createSumCell(StatisticalCell cell) {
    cell.resultType = StatisticalQueryType.SUM;
    String tooltip = "Summe der Spalten: ";
    String columnNames = statisticsVisitor.getColumns().stream().map(n -> {
      try {
        return DisplayStringVisitor.getDisplayString(n);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.joining(", "));
    tooltip += columnNames;
    cell.toolTip = tooltip;
  }

  private String getUnderlyingSetUnit() {
    FilterIDType filterIDTypeToUseForCount = statisticsVisitor.getFilterIDTypeToUseForCount();
    boolean isDistinct = statisticsVisitor.isDistinct();
    String returnValue = exportConfiguration.getFilterIDTypePatientsName();
    if (isDistinct) {
      String returnValueAppendix = " (" + exportConfiguration.getFilterIDTypeGroupedByName() + " " +
              exportConfiguration.getFilterIDTypeGroupedByPatientsName() + ")";
      if (DWQueryConfig.queryAlwaysGroupDistinctQueriesOnDocLevel()) {
        returnValueAppendix = " (" + exportConfiguration.getFilterIDTypeGroupedByName() + " " +
                exportConfiguration.getFilterIDTypeGroupedByDocumentsName() + ")";
        if (filterIDTypeToUseForCount == FilterIDType.CaseID) {
          returnValue = exportConfiguration.getFilterIDTypeCasesName();
        } else if (filterIDTypeToUseForCount == FilterIDType.DocID) {
          returnValue = exportConfiguration.getFilterIDTypeDocumentsName();
        }
      } else {
        if (filterIDTypeToUseForCount == FilterIDType.CaseID) {
          returnValueAppendix = " (" + exportConfiguration.getFilterIDTypeGroupedByName() + " " +
                  exportConfiguration.getFilterIDTypeGroupedByCasesName() + ")";
        } else if (filterIDTypeToUseForCount == FilterIDType.DocID) {
          returnValueAppendix = " (" + exportConfiguration.getFilterIDTypeGroupedByName() + " " +
                  exportConfiguration.getFilterIDTypeGroupedByDocumentsName() + ")";
        }
      }
      returnValue += returnValueAppendix;
    } else {
      if (DWQueryConfig.getInstance().getBooleanParameter("dw.index.neo4j.useKIRaImport", false)) {
        returnValue = exportConfiguration.getFilterIDTypeStraighteningStepsName();
      } else if (filterIDTypeToUseForCount == FilterIDType.CaseID) {
        returnValue = exportConfiguration.getFilterIDTypeCasesName();
      } else if (filterIDTypeToUseForCount == FilterIDType.DocID) {
        returnValue = exportConfiguration.getFilterIDTypeDocumentsName();
      }
    }
    return returnValue;
  }

  private String createTooltip(QueryStructureElem elem) throws QueryException {
    String tooltip = "Anzahl der " + getUnderlyingSetUnit() + " im DW diesen Eigenschaften:<br>";
    String filterText = DisplayStringVisitor.getDisplayString(elem);
    tooltip += filterText;
    return tooltip;
  }

  public QueryRoot getQueryRoot() {
    return queryRoot;
  }

  public void setQueryRoot(QueryRoot queryRoot) {
    this.queryRoot = queryRoot;
  }

  public String getToolTip(int column, int row) {
    return result.getCell(row, column).toolTip;
  }

}
