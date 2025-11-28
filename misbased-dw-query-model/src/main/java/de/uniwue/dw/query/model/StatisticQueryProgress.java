package de.uniwue.dw.query.model;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.query.model.client.DefaultQueryRunner;
import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.client.IGUIClient;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.result.QueryRunnable;
import de.uniwue.dw.query.model.result.Row;
import de.uniwue.dw.query.model.result.StatisticalCell;
import de.uniwue.dw.query.model.result.export.ExportConfiguration;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.StopWatch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class StatisticQueryProgress extends QueryRunnable {

    public static final int TOTAL_WORK = 10000;

    public static final int SENDING_QUERY_WORK = 50;

    public static final int RECEIVING_DOCS_WORK = 650;

    public static final int NUMBER_OF_EXPORTED_ROWS = 1000000;

    private static final Logger logger = LogManager.getLogger(StatisticQueryProgress.class);

    private final StatisticQueryResultCreationHelper helper;

    private int numberOfQueries;

    public StatisticQueryProgress(QueryRoot queryRoot, User user, ExportConfiguration exportConfig,
                                  IGUIClient guiClient, DefaultQueryRunner queryRunner) throws QueryException {
        super(TOTAL_WORK, queryRoot, user, exportConfig, guiClient);
        helper = new StatisticQueryResultCreationHelper(getQueryRoot(), user, queryRunner, exportConfig);
    }

    private void updateMonitor() throws QueryException {
        int worked = TOTAL_WORK / numberOfQueries;
        getMonitor().worked(worked);
    }

    @Override
    public void createOutput() throws QueryException {
        StopWatch stopWatch = new StopWatch("StatisticQueryProgress");
        StopWatch correlationQueryStopWatch = new StopWatch("CorrelationQuery");
        StopWatch correlationAnalysisStopWatch = new StopWatch("CorrelationAnalysis");

        long start = System.currentTimeMillis();

        numberOfQueries = helper.getCellCount() + helper.getNumberOfCorrelationAnalysisQueries();
        List<String> coloumNames = helper.getColumnNames();
        outputHandler.setHeader(coloumNames);
        for (int rowIndex = 0; rowIndex <= helper.getRows() - 1; rowIndex++) {
            Row row = new Row();
            for (int column = 0; column <= helper.getColumns() - 1; column++) {
                if (canceled) {
                    return;
                }

                if (column > 0)
                    stopWatch.start("Query for row " + rowIndex + " and column " + column);

                StatisticalCell cell = row.createNewStatisticalCell();
                try {
                    cell.value = helper.getCellValue(column, rowIndex);
                } catch (GUIClientException e) {
                    throw new QueryException(e);
                }
                cell.toolTip = helper.getToolTip(column, rowIndex);
                updateMonitor();

                if (column > 0)
                    stopWatch.stop();
            }
            outputHandler.addRow(row);
        }

        try {
            String[][] correlationWithPValues = helper.runCorrelationAnalysis(correlationQueryStopWatch,
                    correlationAnalysisStopWatch, TOTAL_WORK / numberOfQueries, getMonitor());
            if (correlationWithPValues != null)
                outputHandler.getResult().setCorrelationWithPValues(correlationWithPValues);
        } catch (GUIClientException e) {
            throw new QueryException(e);
        }

        long end = System.currentTimeMillis();
        long queryTime = end - start;
        logger.trace("query duration: " + queryTime + " ms");
        if (outputHandler != null) {
            outputHandler.setQueryTime(queryTime);
        }
        getMonitor().done();

        printStopWatchResult(stopWatch, helper.getRows(), helper.getColumns() - 1, queryTime, "Query");
        if (outputHandler.getResult() != null && outputHandler.getResult().getCorrelationWithPValues() != null) {
            printStopWatchResult(correlationQueryStopWatch, helper.getRows(), helper.getColumns() - 1, queryTime,
                    "CorrelationQuery");
            printStopWatchResult(correlationAnalysisStopWatch, helper.getRows(), helper.getColumns() - 1, queryTime,
                    "CorrelationAnalysis");
        }
    }

    private void printStopWatchResult(StopWatch stopWatch, int numRows, int numColumns, long totalTime, String title) {
        if (!DwClientConfiguration.getInstance().getBooleanParameter("radiologyImporter.logTime", false))
            return;

        List<StopWatch.TaskInfo> taskInfos = List.of(stopWatch.getTaskInfo());
        double averageMilliSecondsPerQuery =
                taskInfos.stream().mapToDouble(StopWatch.TaskInfo::getTimeMillis).sum() / taskInfos.size();
        double standardDeviationMilliSecondsPerQuery = new StandardDeviation()
                .evaluate(taskInfos.stream().mapToDouble(StopWatch.TaskInfo::getTimeMillis).toArray(),
                        averageMilliSecondsPerQuery);

        System.out.println("\n");
        System.out.println(title);
        System.out.println("Number of rows: " + numRows);
        System.out.println("Number of columns: " + numColumns);
        System.out.println("Number of queries: " + taskInfos.size());
        System.out.println("Average time per query: " + averageMilliSecondsPerQuery + " ms");
        System.out.println("Standard deviation of time per query: " + standardDeviationMilliSecondsPerQuery + " ms");
        System.out.println("Total time: " + totalTime + " ms");
        System.out.println("Total time of stop watch task: " + stopWatch.getTotalTimeMillis() + " ms");

        if (Objects.equals(title, "CorrelationQuery")) {
            List<Integer> resultSizes = new ArrayList<>();
            for (Map<Long, Double> singleResult : helper.rowQueryResults)
                resultSizes.add(singleResult.size());
            for (Map<Long, Double> singleResult : helper.columnQueryResults)
                resultSizes.add(singleResult.size());
            int totalResultSize = resultSizes.stream().mapToInt(Integer::intValue).sum();
            double averageResultSize = resultSizes.stream().mapToInt(Integer::intValue).average().orElse(0.0);
            double standardDeviationResultSize = new StandardDeviation()
                    .evaluate(resultSizes.stream().mapToDouble(Integer::doubleValue).toArray(), averageResultSize);
            System.out.println("Average number of results returned by each query: " + averageResultSize);
            System.out.println("Standard deviation of number of results returned by each query: "
                    + standardDeviationResultSize);
            System.out.println("Total number of results returned by all queries: " + totalResultSize);
        }

        System.out.println("\n");
    }

    public QueryRoot getQueryOfCell(int column, int row) {
        return helper.getCell(column, row).queryRoot;
    }
}
