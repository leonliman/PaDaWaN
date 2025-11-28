package de.uniwue.dw.query.model.client;

import de.uniwue.dw.core.client.api.configuration.IDWClientKeys;
import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.query.model.StatisticQueryProgress;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.exception.QueryStructureException;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryIDFilter;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.manager.QueryManipulationManager;
import de.uniwue.dw.query.model.result.IPostProcessor;
import de.uniwue.dw.query.model.result.PatientQueryRunnable;
import de.uniwue.dw.query.model.result.QueryRunnable;
import de.uniwue.dw.query.model.result.Result;
import de.uniwue.dw.query.model.result.export.ExportConfiguration;
import de.uniwue.dw.query.model.result.export.ExportType;
import de.uniwue.dw.query.model.visitor.StructureErrorVisitor;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class DefaultQueryRunner implements IQueryRunner, IDWClientKeys {

  protected final Map<Integer, QueryRunnable> runningQueries = new HashMap<>();

  protected IGUIClient guiClient;

  public DefaultQueryRunner(IGUIClient guiClient) {
    this.guiClient = guiClient;
  }

  @Override
  public String getEngineName() {
    String engineName = toString().replaceAll("^.*\\.", "");
    engineName = engineName.replaceAll("@.*", "");
    for (int i = 0; i < 20 - engineName.length(); i++) {
      engineName = engineName + " ";
    }
    return engineName;
  }

  @Override
  public int createQuery(QueryRoot queryRoot, User user, IPostProcessor aPostProcessor)
          throws GUIClientException {
    int createQuery = createQuery(queryRoot, user);
    QueryRunnable queryRunnable = runningQueries.get(createQuery);
    ((PatientQueryRunnable) queryRunnable).postProcessors.add(aPostProcessor);
    return createQuery;
  }

  @Override
  public int createQuery(QueryRoot queryRoot, User user) throws GUIClientException {
    ExportConfiguration exportConfig = new ExportConfiguration();
    return createQuery(queryRoot, user, exportConfig);
  }

  @Override
  public int createQuery(QueryRoot queryRoot, User user, ExportConfiguration exportConfig)
          throws GUIClientException {
    checkQueryRootSyntax(queryRoot);
    QueryManipulationManager.shrinkQuery(queryRoot);
    // List<Group> groups = null;
    // if (user != null) {
    // groups = DWQueryConfig.getFilterDocumentsByGroup() ? user.getGroups() : null;
    // }
    QueryRunnable runningQury;
    if (queryRoot.isStatisticQuery()) {
      runningQury = new StatisticQueryProgress(queryRoot, user, exportConfig, guiClient, this);
    } else {
      runningQury = runQueryInternal(queryRoot, user, exportConfig);
    }
    runningQueries.put(runningQury.getID(), runningQury);
    logRequest(queryRoot, user, runningQury.getID(), exportConfig);
    return runningQury.getID();
  }

  @Override
  public int cancelQuery(int runningQueryID) throws GUIClientException {
    getQueryRunnable(runningQueryID).doCancel();
    return runningQueryID;
  }

  @Override
  public void runQuery(int runningQueryID) throws GUIClientException {
    try {
      getQueryRunnable(runningQueryID).run();
    } catch (InvocationTargetException e) {
      throw new GUIClientException(e);
    } catch (InterruptedException e) {
      throw new GUIClientException(e);
    }
  }

  @Override
  public Result runQueryBlocking(int runningQueryID) throws GUIClientException {
    runQuery(runningQueryID);
    QueryStatus progress = getProgress(runningQueryID);
    while (!progress.isDone()) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        throw new GUIClientException(e);
      }
      progress = getProgress(runningQueryID);
    }
    Result result = getResult(runningQueryID);
    return result;
  }

  private void checkQueryRootSyntax(QueryRoot queryRoot) throws QueryException {
    Set<QueryStructureException> errors = getStructureErrors(queryRoot);
    if (!errors.isEmpty())
      throw new QueryException(QueryStructureException.getReadableErrorString(errors));
  }

  private QueryRunnable getQueryRunnable(int runningQueryID) {
    if (runningQueries.containsKey(runningQueryID)) {
      QueryRunnable runningQuery = runningQueries.get(runningQueryID);
      return runningQuery;
    } else {
      return null;
    }
  }

  @Override
  public boolean containsQuery(int runningQueryID) {
    return runningQueries.containsKey(runningQueryID);
  }

  @Override
  public Result getResult(int runningQueryID) {
    if (runningQueries.containsKey(runningQueryID)) {
      QueryRunnable runningQuery = runningQueries.get(runningQueryID);
      // runningQueries.get(runningQueryID).getMonitor().isDone()
      return runningQuery.getResult();
    } else {
      return null;
    }
  }

  public void queryFinished(QueryRunnable queryRunnable) throws GUIClientException {
    long resultCount = 0;
    if (queryRunnable instanceof PatientQueryRunnable) {
      resultCount = ((PatientQueryRunnable) queryRunnable).getNumFound();
    }
    guiClient.getQueryClientIOManager().updateLogQuery(queryRunnable.getUser().getUsername(),
            queryRunnable.getID(), resultCount,
            queryRunnable.getEndTime() - queryRunnable.getStartTime());
  }

  @Override
  public QueryStatus getProgress(int runningQueryID) {
    QueryRunnable queryRunnable = runningQueries.get(runningQueryID);
    QueryStatus status = new QueryStatus();
    status.setWorkedPercentageAsFraction(queryRunnable.getMonitor().getWorkedPercentage());
    status.setException(queryRunnable.getException());
    status.setDone(queryRunnable.getMonitor().isDone());
    return status;
  }

  @Override
  public void disposeQuery(int runningQueryID) {
    if (runningQueries.containsKey(runningQueryID)) {
      // QueryRunnable runningQuery = runningQueries.get(runningQueryID);
      // TODO: abort/cancel query
      runningQueries.remove(runningQueryID);
    }
  }

  @Override
  public ExportConfiguration getExportConfiguration(int runningQueryID) {
    if (runningQueries.containsKey(runningQueryID)) {
      QueryRunnable runningQuery = runningQueries.get(runningQueryID);
      return runningQuery.getExportConfig();
    } else {
      return null;
    }
  }

  @Override
  public QueryRoot getQueryRoot(int runningQueryID) {
    if (runningQueries.containsKey(runningQueryID)) {
      QueryRunnable runningQuery = runningQueries.get(runningQueryID);
      return runningQuery.getQueryRoot();
    } else {
      return null;
    }
  }

  @Override
  public boolean exportQuery(int queryID, Path destination, ExportType exportType)
          throws IOException {

    QueryRunnable runningQuery = runningQueries.get(queryID);

    ExportConfiguration config = new ExportConfiguration(exportType);
    config.setOutputPath(destination);
    runningQuery.setExportConfig(config);
    try {
      runningQuery.run();
      return true;
    } catch (InvocationTargetException | InterruptedException e) {
      e.printStackTrace();
    }

    // Exporter exporter;
    // if (isExporter)
    // exporter = new ExcelExporter(runningQuery);
    // else
    // exporter = new CSVExporter(runningQuery);
    // return exporter.export();
    return false;
  }

  protected StructureErrorVisitor createStructureErrorVisitor() {
    return new StructureErrorVisitor(this);
  }

  @Override
  public Set<QueryStructureException> getStructureErrors(QueryRoot queryRoot)
          throws QueryException {
    StructureErrorVisitor visitor = createStructureErrorVisitor();
    Set<QueryStructureException> visit = queryRoot.accept(visitor);
    // Set<QueryStructureException> visit = visitor.visit(queryRoot);
    // String readableErrorString = "";
    // readableErrorString = QueryStructureException.getReadableErrorString(visit);
    return visit;
  }

  private void logRequest(QueryRoot queryRoot, User user, int queryID,
          ExportConfiguration exportConfig) throws GUIClientException {
    queryRoot.expandSubQueries(new QueryManipulationManager(),
            guiClient.getQueryClientIOManager().getQueryIOManager());
    guiClient.getQueryClientIOManager().logQuery(queryRoot.generateXML(), user.getUsername(),
            queryID, exportConfig.getExportFileType().toString(), getEngineName(),
            getEngineVersion());
  }

  public abstract PatientQueryRunnable runQueryInternal(QueryRoot queryRoot, User user,
          ExportConfiguration exportConfig) throws GUIClientException;

  @Override
  public void dispose() {
  }

  @Override
  public boolean canProcessRelativeTemporalCompares() {
    return false;
  }

  @Override
  public boolean canProcessFixedIDs() {
    return false;
  }

  @Override
  public boolean canProcessAbsoluteTemporalContraints() {
    return false;
  }

  @Override
  public boolean canProcessFulltextNearOperators() {
    return false;
  }

  @Override
  public boolean canProcessRelativeQuantitativeCompares() {
    return false;
  }

  @Override
  public boolean canProcessInfoAdditonalInfos() {
    return true;
  }

  @Override
  public boolean canProcessDistincts() {
    return false;
  }

  @Override
  public boolean canProcessGroupCases() {
    return false;
  }

  @Override
  public boolean canProcessGroupDocs() {
    return false;
  }

  @Override
  public boolean canProcessMultipleRows() {
    return false;
  }

  @Override
  public boolean canDoPostProcessing() {
    return true;
  }

  private HashMap<QueryRoot, Boolean> hasToDoPostProcessingCache = new HashMap<>();

  @Override
  public boolean postProcessingCanChangesResultCount(QueryRoot queryRoot) throws QueryException {
    if (hasToDoPostProcessing(queryRoot)) {
      for (QueryIDFilter aFilter : queryRoot.getIDFilterRecursive()) {
        if (aFilter.isDistinct() && !canProcessDistincts()) {
          return true;
        }
        if ((aFilter.getFilterIDType() == FilterIDType.CaseID) && !canProcessGroupCases()) {
          return true;
        }
        if ((aFilter.getFilterIDType() == FilterIDType.DocID) && !canProcessGroupDocs()) {
          return true;
        }
      }
      if (!canProcessMultipleRows()) {
        for (QueryAttribute anAttribute : queryRoot.getAttributesRecursive()) {
          if (anAttribute.isMultipleRows()) {
            return true;
          }
        }
      }
    } else {
      return false;
    }
    return false;
  }

  @Override
  public boolean hasToDoPostProcessing(QueryRoot queryRoot) throws QueryException {
    queryRoot.toString();
    Boolean result = hasToDoPostProcessingCache.get(queryRoot);
    if (result == null) {
      result = false;
      if ((queryRoot.getTempOpsRelRecursive().size() > 0)
              && !canProcessRelativeTemporalCompares()) {
        result = true;
      }
      if ((queryRoot.getValueComparesRecursive().size() > 0)
              && !canProcessRelativeQuantitativeCompares()) {
        result = true;
      }
      for (QueryIDFilter aFilter : queryRoot.getIDFilterRecursive()) {
        if (aFilter.isDistinct() && !canProcessDistincts()) {
          result = true;
        }
        if ((aFilter.getFilterIDType() == FilterIDType.CaseID) && !canProcessGroupCases()) {
          result = true;
        }
        if ((aFilter.getFilterIDType() == FilterIDType.DocID) && !canProcessGroupDocs()) {
          result = true;
        }
      }
      if (!canProcessDistincts() && queryRoot.isDistinct()) {
        result = true;
      }
      if (!canProcessInfoAdditonalInfos()) {
        for (QueryAttribute anAttribute : queryRoot.getAttributesRecursive()) {
          if (anAttribute.displayCaseID() || anAttribute.displayDocID()
                  || anAttribute.displayInfoDate()) {
            result = true;
          }
        }
      }
      if (!canProcessMultipleRows()) {
        for (QueryAttribute anAttribute : queryRoot.getAttributesRecursive()) {
          if (anAttribute.isMultipleRows()) {
            result = true;
          }
        }
      }
      if (!canProcessAbsoluteTemporalContraints()
              && (queryRoot.getTempOpsAbsRecursive().size() > 0)) {
        result = true;
      }
      hasToDoPostProcessingCache.put(queryRoot, result);
    }
    return result;
  }

  @Override
  public QueryRoot getQueryOfCell(int runningQueryID, int column, int row) {
    QueryRunnable queryRunnable = getQueryRunnable(runningQueryID);
    if (queryRunnable.getQueryRoot().isStatisticQuery()) {
      return ((StatisticQueryProgress) queryRunnable).getQueryOfCell(column, row);
    }
    return null;
  }

}
