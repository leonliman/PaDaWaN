package de.uniwue.dw.query.model.result;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.manager.ICatalogClientManager;
import de.uniwue.dw.query.model.DWQueryConfig;
import de.uniwue.dw.query.model.client.*;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.manager.QueryIOManager;
import de.uniwue.dw.query.model.manager.QueryManipulationManager;
import de.uniwue.dw.query.model.result.export.*;
import de.uniwue.misc.util.EnvironmentUniWue;
import de.uniwue.misc.util.thread.CancelableRunnableWithProgress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.Objects;

/*
 * An instance of this class is returned by a guiClient when a query has to be run. The instance is configured with an ExportConfiguration.
 * When the query has been performed the results can be either the Result object or a file which resides in the resultStream.
 * When the ExportConfiguration has been given an output filename the output file is automatically written to this file.
 */
public abstract class QueryRunnable extends CancelableRunnableWithProgress {

  static final Logger logger = LogManager.getLogger(QueryRunnable.class.getName());

  /*
   * The query which has to be performed
   */
  private final QueryRoot queryRoot;

  /*
   * An RunningQueryID to reference this running query
   */
  private final int id;

  private final IGUIClient guiClient;

  public IOutputHandler outputHandler;

  // a copy of the queryRoot given during instantiation of the QueryRunnable. If any modifications
  // or optimizations have been applied to the current queryRoot this could be useful
  protected QueryRoot originalQueryRoot;

  protected User user;

  /*
   * Happened an exception during the asynchronous execution of the query ?
   */
  private QueryException exception;

  private long startTime, endTime;

  private ExportConfiguration exportConfig;

  public QueryRunnable(int workTotal, QueryRoot queryRoot, User user,
          ExportConfiguration exportConfig, IGUIClient guiClient) throws QueryException {
    super(workTotal);
    this.exportConfig = Objects.requireNonNullElseGet(exportConfig, () -> new ExportConfiguration(ExportType.GUI));
    this.user = user;
    this.id = EnvironmentUniWue.random.nextInt();
    this.guiClient = guiClient;
    this.queryRoot = queryRoot.copyForQuery(getCatalogClientManager());
    this.queryRoot.setFilterIDTypeForDistinctCount(queryRoot.getFilterIDTypeForDistinctCount());
    this.originalQueryRoot = queryRoot.copyForQuery(getCatalogClientManager());
    this.originalQueryRoot.setFilterIDTypeForDistinctCount(queryRoot.getFilterIDTypeForDistinctCount());
    expandQuery();
    initialize();
  }

  /*
   * This is the main method which has to be implemented by subclasses. In here the result is
   * created with the help of the respective methods used by the particular search index technology
   * which is used (Solr, SQL, Elastic Search...). If the creation of the exported file is also done
   * during the creation of the result (streaming of documents into the result file) the
   * implementing subclass has to instantiate a CSVExporterStreaming in this method and set the
   * exportConfiguration to streaming so that the export is not done in writeResultToStream() a
   * second time afterwards.
   */
  public abstract void createOutput() throws QueryException;

  protected IGUIClient getGUIClient() {
    return guiClient;
  }

  protected ICatalogClientManager getCatalogClientManager() {
    return getGUIClient().getCatalogClientProvider();
  }

  protected void initialize() throws QueryException {
  }

  protected void stopStreaming() {
  }

  public boolean isCanceled() {
    return canceled;
  }

  @Override
  public void doCancel() {
    try {
      getMonitor().setCanceled();
      canceled = true;
      outputHandler.close();
      if (getExportConfig().getOutputPath() != null) {
        Files.delete(getExportConfig().getOutputPath());
      }
    } catch (QueryException | IOException e) {
      logger.error(e.getMessage());
    }
  }

  public IQueryRunner getQueryRunner() throws QueryException {
    try {
      return getGUIClient().getQueryRunner();
    } catch (GUIClientException e) {
      throw new QueryException(e);
    }
  }

  private void creatOutputHandler() throws QueryException {
    if (exportConfig == null) {
      exportConfig = new ExportConfiguration(ExportType.GUI);
    }
    int kAnonymity = user.getKAnonymity();
    switch (exportConfig.getExportFileType()) {
      case GUI:
        outputHandler = new MemoryOutputHandler(this, exportConfig, kAnonymity);
        break;
      case RESULT_TABLE:
        outputHandler = new MemoryOutputHandler(this, exportConfig, kAnonymity);
        break;
      case CSV:
        outputHandler = new CSVStreamHandler(this, exportConfig, kAnonymity);
        break;
      case EXCEL:
        outputHandler = new ExcelHandler(this, exportConfig, kAnonymity);
        break;
      default:
        outputHandler = new MemoryOutputHandler(this, exportConfig, kAnonymity);
        break;
    }
  }

  private void expandQuery() throws QueryException {
    QueryManipulationManager queryManager = new QueryManipulationManager();
    try {
      QueryIOManager queryAllQueriesManager = new QueryIOManager(
              guiClient.getCatalogClientProvider());
      queryRoot.expandSubQueries(queryManager, queryAllQueriesManager);
    } catch (SQLException e) {
      throw new QueryException(e);
    }
  }

  private boolean checkCache() throws QueryException {
    if (DWQueryConfig.queryUseCache()) {
      Result result = QueryCache.getInstance().get(queryRoot);
      if (result != null) {
        outputHandler.setResult(result);
        done = true;
      }
    }
    return done;
  }

  public Result getResult() {
    return outputHandler.getResult();
  }

  protected void createOutputInternal() throws QueryException {
    createOutput();
  }

  @Override
  protected void work() {
    getMonitor().setTaskName("Anfrage wird an das Data-Warehouse gestellt");
    try {
      setStartTime(System.currentTimeMillis());
      creatOutputHandler();
      boolean loadedFromCache = true;
      if (!checkCache()) {
        loadedFromCache = false;
        createOutputInternal();
        Result result = outputHandler.getResult();
        if (DWQueryConfig.queryUseCache()) {
          if (result != null) {
            QueryCache.getInstance().put(queryRoot, result);
          }
        }
      }
      setEndTime(System.currentTimeMillis());
      if (loadedFromCache) {
        outputHandler.setQueryTime(getEndTime() - getStartTime());
      }
      if ((getQueryRunner()) != null) {
        ((DefaultQueryRunner) getQueryRunner()).queryFinished(this);
      }
      outputHandler.done();
      getMonitor().done();
    } catch (QueryException e) {
      exception = e;
      doCancel();
      e.printStackTrace();
    } catch (GUIClientException e) {
      e.printStackTrace();
    }
  }

  public QueryException getException() {
    return exception;
  }

  public void setException(QueryException anEx) {
    exception = anEx;
  }

  public QueryRoot getQueryRoot() {
    return queryRoot;
  }

  public ExportConfiguration getExportConfig() {
    return outputHandler.getExportConfiguration();
  }

  public void setExportConfig(ExportConfiguration exportConfig) {
    this.exportConfig = exportConfig;
  }

  public int getID() {
    return id;
  }

  public User getUser() {
    return user;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

}
