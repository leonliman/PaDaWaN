package de.uniwue.dw.query.model.client;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.exception.QueryStructureException;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.result.IPostProcessor;
import de.uniwue.dw.query.model.result.Result;
import de.uniwue.dw.query.model.result.export.ExportConfiguration;
import de.uniwue.dw.query.model.result.export.ExportType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

public interface IQueryRunner {

  /*
   * Start a query. The result is an ID with which the query can be referred to by further calls to
   * the GUIClient
   */
  int createQuery(QueryRoot queryRoot, User user) throws GUIClientException;

  /*
   * If there is a need for another post proecessor than the une in the query.model package, this
   * can be provided here.
   */
  int createQuery(QueryRoot queryRoot, User user, IPostProcessor aPostProcessor) throws GUIClientException;

  int createQuery(QueryRoot queryRoot, User user, ExportConfiguration exportConfig) throws GUIClientException;

  void runQuery(int runningQueryID) throws GUIClientException;

  boolean containsQuery(int runningQueryID);

  String getEngineName();

  String getEngineVersion();

  /**
   * Cancels a running Query by ID
   */
  int cancelQuery(int runningQueryID) throws GUIClientException;

  /**
   * If the query is a statistic query then this method returns a query that calculated the count
   * for the respective cell
   */
  QueryRoot getQueryOfCell(int runningQueryID, int column, int row);

  /**
   * Runs the query and blocks the current thread until the query is finished. Returns the query's
   * result
   */
  Result runQueryBlocking(int runningQueryID) throws GUIClientException;

  /*
   * Retrieve the result of the running query from the server. If the query has not been completed
   * so far this can be a part of the final result
   */
  Result getResult(int runningQueryID);

  /*
   * Retrieve the fraction of total work of the running query on the server which has already been
   * completed.
   */
  QueryStatus getProgress(int runningQueryID);

  /*
   * Dispose any resources that remain at the server belonging to the respective queryID. If the
   * Query is still running it is aborted.
   */
  void disposeQuery(int runningQueryID);

  /*
   * Retrieve the ExportConfiguration of the running query from the server that has been given at
   * the start of the query
   */
  ExportConfiguration getExportConfiguration(int runningQueryID);

  /*
   * Retrieve the QueryRoot of the running query from the server that has been given at the start of
   * the query
   */
  QueryRoot getQueryRoot(int runningQueryID);

  /*
   * Check the given query for general problems and problems with the respective query engine
   * implementation
   */
  Set<QueryStructureException> getStructureErrors(QueryRoot queryRoot) throws QueryException;

  public boolean exportQuery(int queryID, Path destination, ExportType exporType)
          throws IOException;

  /*
   * Dispose the respective query engine and free all resources that have been used by it
   */
  void dispose();

  /*
   * Is the query engine capable of processing the following query features natively in its engine.
   * If this is not the case the feature is post processed in the generic model
   */
  boolean canProcessRelativeTemporalCompares();

  boolean canProcessFixedIDs();

  boolean canProcessAbsoluteTemporalContraints();

  boolean canProcessFulltextNearOperators();

  boolean canProcessRelativeQuantitativeCompares();

  boolean canProcessInfoAdditonalInfos();

  boolean canProcessDistincts();

  boolean canProcessGroupDocs();

  boolean canProcessGroupCases();

  boolean canProcessMultipleRows();

  /*
   * Can the output result of the engine be post processed by the PostProcessor-class. If the engine
   * does create its result itself instead of adding the rows via createResultRow in the
   * PatientQueryRunnable the post processing is not possible
   */
  boolean canDoPostProcessing();

  boolean hasToDoPostProcessing(QueryRoot query) throws QueryException;

  boolean postProcessingCanChangesResultCount(QueryRoot query) throws QueryException;
}
