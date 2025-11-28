package de.uniwue.dw.query.solr.export;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.query.model.client.IGUIClient;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.result.PatientQueryRunnable;
import de.uniwue.dw.query.model.result.export.ExportConfiguration;
import de.uniwue.dw.query.solr.SolrConstants;
import de.uniwue.dw.query.solr.SolrUtil;
import de.uniwue.dw.query.solr.client.ISolrConstants;
import de.uniwue.dw.query.solr.model.visitor.CaseQueryParamsVisitor;
import de.uniwue.dw.solr.api.DWSolrConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.StreamingBinaryResponseParser;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;

import java.io.IOException;

public class SolrPatientQueryRunnable extends PatientQueryRunnable {

  public static final int TOTAL_WORK = 10000;

  private static final Logger logger = LogManager.getLogger(SolrPatientQueryRunnable.class);

  private SolrQuery solrQuery;

  // this flag is used by the internal createOutput process. When this flag is set don't fire any
  // exceptions that are related to the shutdown.
  private boolean gotKilledByShutdown = false;

  public SolrPatientQueryRunnable(QueryRoot queryRoot, User user, ExportConfiguration exportConfig,
          IGUIClient guiClient) throws QueryException {
    super(caclTotalWork(queryRoot.getLimitResult()), queryRoot, user, exportConfig, guiClient);
  }

  private static int caclTotalWork(int limitResult) {
    int totalWork = SolrConstants.SENDING_QUERY_WORK + TOTAL_WORK;
    return totalWork;
  }

  @Override
  protected void initialize() throws QueryException {
    createQuery();
  }

  private void createQuery() throws QueryException {
    SolrQuery solrQuery = SolrUtil.getSolrQueryString(getQueryRoot());
    CaseQueryParamsVisitor queryVisitor = new CaseQueryParamsVisitor(solrQuery);
    getQueryRoot().accept(queryVisitor);
    solrQuery = queryVisitor.getSolrQuery();
    if (getQueryRunner().hasToDoPostProcessing(getQueryRoot())) {
      solrQuery.setRows(ISolrConstants.MAX_ROWS);
    }
    // System.out.println("FinalSolrQuery: " + solrQuery);
    SolrUtil.addGroupFilter2Query(user.getGroups(), solrQuery);
    logger.debug(solrQuery);
    super.initialize();
  }

  @Override
  public long getOnlyCountNumber() throws QueryException {
    try {
      long start = System.currentTimeMillis();
      QueryRoot queryRoot = getQueryRoot();
      // String solrQuery = SolrUtil.getSolrQueryString(queryRoot);
      SolrClient server = DWSolrConfig.getInstance().getSolrManager().getServer();
      // long numFound = SolrUtil.getNumFound(server,
      // queryRoot.isDistinct(), solrQuery, user.getGroups());

      long numFound = SolrUtil.getNumFound(server, queryRoot, user.getGroups());

      long end = System.currentTimeMillis();
      long queryTime = end - start;
      logger.debug("query successfull. result created. query duration " + queryTime);
      if (outputHandler != null) {
        outputHandler.setQueryTime(queryTime);
      }
      return numFound;
    } catch (SolrServerException | IOException e) {
      throw new QueryException(e);
    }
  }

  @Override
  protected void stopStreaming() {
    // Hier sollte das streaming beendet werden. Aktuell wird einfach die Connection beendet.
    // Unschoen, aber der einzige mir bekannte Weg.
    System.out.println("Killing SolrServer now!");
    gotKilledByShutdown = true;
    DWSolrConfig.getInstance().getSolrManager().shutdown();
    // logger.error("Query should be canceled now. But no known way to do this.");
  }

  @Override
  public void doCancel() {
    stopStreaming();
    super.doCancel();
  }

  @Override
  public void createOutput() throws QueryException {
    createQuery();
    getMonitor().subTask("Anfrage wird an das Data-Warehouse gestellt");
    long start = System.currentTimeMillis();
    SolrStreamingResponseCallback solrCallback = new SolrStreamingResponseCallback(this);
    ResponseParser parser = new StreamingBinaryResponseParser(solrCallback);
    QueryRequest req = new QueryRequest(solrQuery, METHOD.POST);
    req.setStreamingResponseCallback(solrCallback);
    req.setResponseParser(parser);
    try {
      QueryResponse response = req.process(DWSolrConfig.getInstance().getSolrManager().getServer());
    } catch (Exception e) {
      if (!gotKilledByShutdown) {
        e.printStackTrace();
        throw new QueryException(e);
      }
    }
    long end = System.currentTimeMillis();
    long queryTime = end - start;
    logger.debug("query successfull. result created. query duration " + queryTime);
    outputHandler.setQueryTime(queryTime);
  }

}
