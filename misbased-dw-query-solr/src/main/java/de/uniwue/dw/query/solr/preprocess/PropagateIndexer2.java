package de.uniwue.dw.query.solr.preprocess;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.CountType;
import de.uniwue.dw.core.client.authentication.group.AuthManager;
import de.uniwue.dw.core.client.authentication.group.Group;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.core.model.data.Patient;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.query.model.index.AbstractDataSource2Index;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.dw.query.model.index.tree.ForestBuilder;
import de.uniwue.dw.query.model.index.tree.Node;
import de.uniwue.dw.query.model.index.tree.PropagatorVisitor;
import de.uniwue.dw.query.solr.SolrUtil;
import de.uniwue.dw.query.solr.client.ISolrConstants;
import de.uniwue.dw.query.solr.client.SolrManager;
import de.uniwue.dw.query.solr.preprocess.model.IndexVisitor;
import de.uniwue.dw.query.solr.suggest.CatalogIndexer;
import de.uniwue.dw.solr.api.DWSolrConfig;
import de.uniwue.misc.util.TimeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/*
 * This is the new indexer that derives from the abstract indexing classes
 */
public class PropagateIndexer2 extends AbstractDataSource2Index implements ISolrConstants {

  private static final String CASE_PREFIX = "c";

  private static final String PATIENT_PREFIX = "p";

  private static final Logger logger = LogManager.getLogger(PropagateIndexer2.class);

  public static String patientDocType = "patient";

  public static String caseDocType = "case";

  public static String docTypeFieldName = "string_doc_type";

  private SolrClient server;

  private SolrManager solrManager;

  private CatalogIndexer catalogIndexer;

  public PropagateIndexer2() throws IndexException {
    super();
  }

  public static String caseID2SolrDocumentID(Long caseID) {
    if (caseID == null)
      return null;
    return CASE_PREFIX + caseID;
  }

  public static Long solrDocumentID2CaseID(String solrDocID) {
    if (solrDocID == null)
      return null;
    return Long.parseLong(solrDocID.replaceFirst(CASE_PREFIX, ""));
  }

  public static String patientID2SolrDocumentID(Long patientID) {
    if (patientID == null)
      return null;
    return PATIENT_PREFIX + patientID;
  }

  public static Long solrDocumentID2PatientID(String solrDocID) {
    if (solrDocID == null)
      return null;
    return Long.parseLong(solrDocID.replaceFirst(PATIENT_PREFIX, ""));
  }

  private static void deleteDocumetsInSolr(List<String> solrIdsToDelete)
          throws SolrServerException, IOException {
    if (!solrIdsToDelete.isEmpty()) {
      SolrClient server = DWSolrConfig.getInstance().getSolrManager().getServer();
      server.deleteById(solrIdsToDelete);
    }
  }

  @Override
  protected void initialize() throws IndexException {
    solrManager = DWSolrConfig.getInstance().getSolrManager();
    super.initialize();
    server = solrManager.getServer();
    AuthManager groupManager;
    try {
      groupManager = DwClientConfiguration.getInstance().getAuthManager();
      catalogIndexer = new CatalogIndexer(getCatalogManager(), groupManager);
    } catch (SQLException e) {
      throw new IndexException(e);
    }
  }

  public void optimizeServer() throws SolrServerException, IOException {
    server.optimize();
    System.out.println("Server optimzied at " + TimeUtil.currentTime());
  }

  @Override
  protected void indexCatalogEntry(CatalogEntry anEntry) throws IndexException {
    try {
      catalogIndexer.indexCatalogEntry(anEntry);
    } catch (SolrServerException e) {
      throw new IndexException(e);
    } catch (IOException e) {
      throw new IndexException(e);
    }
  }

  public void indexCaseData(long pid, long caseid, List<Information> infos) throws IndexException {
    if (caseid == 0) {
      return;
    }
    try {
      List<Group> entitledGroups = getEntitledGroupsForDataID(caseid);
      SolrInputDocument doc = new SolrInputDocument();
      addGroups(doc, entitledGroups);
      doc.addField("id", caseID2SolrDocumentID(caseid));
      doc.addField("patient", pid);
      doc.addField(PropagateIndexer2.docTypeFieldName, PropagateIndexer2.caseDocType);
      ForestBuilder forester = new ForestBuilder(getCatalogManager());
      forester.insertInformation(infos);
      List<Node> roots = forester.getForest().getRoots();
      PropagatorVisitor propagator = new PropagatorVisitor();
      IndexVisitor indexVisitor = new IndexVisitor(getCatalogManager(), doc);
      for (Node root : roots) {
        root.acceptVisitor(propagator);
        root.acceptVisitor(indexVisitor);
      }
      // forester.getForest().printForest();
      // this is for downward compatibility because the IDs got a prefix, so delete the documents
      // without the prefix
      server.deleteById(Long.toString(caseid));
      server.add(doc);
      // int x = 0;
    } catch (SolrServerException | SQLException | IOException e) {
      throw new IndexException(e);
    }
  }

  public void indexPatientData(long pid, List<Information> infos) throws IndexException {
    try {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", patientID2SolrDocumentID(pid));
      doc.addField(PropagateIndexer2.docTypeFieldName, PropagateIndexer2.patientDocType);
      ForestBuilder forester = new ForestBuilder(getCatalogManager());
      forester.insertInformation(infos);
      List<Node> roots = forester.getForest().getRoots();
      PropagatorVisitor propagator = new PropagatorVisitor();
      IndexVisitor indexVisitor = new IndexVisitor(getCatalogManager(), doc);
      for (Node root : roots) {
        root.acceptVisitor(propagator);
        root.acceptVisitor(indexVisitor);
      }
      // forester.getForest().printForest();
      // this is for downward compatibility because the IDs got a prefix, so delete the documents
      // without the prefix
      server.deleteById(Long.toString(pid));
      server.add(doc);
      // int x = 0;
    } catch (SolrServerException | SQLException | IOException e) {
      throw new IndexException(e);
    }
  }

  @Override
  public void indexData(Patient patient) throws IndexException {
    List<Information> infos = patient.getAllContainingInfos();
    long pid = patient.getPid();
    HashMap<Long, List<Information>> caseID2infos = new HashMap<Long, List<Information>>();
    for (Information anInfo : infos) {
      if (!caseID2infos.containsKey(anInfo.getCaseID())) {
        caseID2infos.put(anInfo.getCaseID(), new ArrayList<Information>());
      }
      List<Information> infoListForCaseID = caseID2infos.get(anInfo.getCaseID());
      infoListForCaseID.add(anInfo);
    }
    for (Long aCaseID : caseID2infos.keySet()) {
      List<Information> infoListForCaseID = caseID2infos.get(aCaseID);
      indexCaseData(pid, aCaseID, infoListForCaseID);
    }
    if (DWSolrConfig.indexPatientLayer()) {
      indexPatientData(pid, infos);
    }
  }

  private void addGroups(SolrInputDocument doc, List<Group> entitledGroups) {
    for (Group g : entitledGroups) {
      doc.addField(ISolrConstants.SOLR_FIELD_GROUPS, g.getName());
    }
  }

  @Override
  protected String getServerID() {
    return solrManager.getServerURL();
  }

  @Override
  public void deleteIndex() throws IndexException {
    try {
      server.deleteByQuery("*:*");
      server.commit();
    } catch (SolrServerException e) {
      throw new IndexException(e);
    } catch (IOException e) {
      throw new IndexException(e);
    }
  }

  @Override
  protected void commit() throws IndexException {
    try {
      server.commit();
    } catch (SolrServerException | IOException e) {
      throw new IndexException(e);
    }
  }

  @Override
  protected void deleteData(long dataID, List<Information> info2Delete) throws IndexException {
    try {
      if (DWSolrConfig.indexPatientLayer()) {
        server.deleteById(patientID2SolrDocumentID(dataID));
      }
      HashSet<Long> handledCaseIDs = new HashSet<Long>();
      for (Information anInfo : info2Delete) {
        long caseID = anInfo.getCaseID();
        if (!handledCaseIDs.contains(caseID)) {
          server.deleteById(caseID2SolrDocumentID(caseID));
          handledCaseIDs.add(caseID);
        }
      }
      indexData(dataID);
    } catch (SolrServerException e) {
      throw new IndexException(e);
    } catch (IOException e) {
      throw new IndexException(e);
    }
  }

  @Override
  protected long calculateCount(CatalogEntry anEntry, CountType countType) throws IndexException {
    try {
      long count = 0;
      String solrFieldName = SolrUtil.getSolrID(anEntry);
      String queryString = "containing_fields:" + solrFieldName;
      if (countType == CountType.distinctPID) {
        queryString += " AND string_doc_type:patient";
      } else if (countType == CountType.distinctCaseID) {
        queryString += " AND string_doc_type:case";
      } else {
        queryString += " AND string_doc_type:case";
      }
      count = SolrUtil.getNumFound(server, false, queryString);
      return count;
    } catch (SolrServerException | IOException e) {
      throw new IndexException(e);
    }
  }

  @Override
  public void cleanCatalogIndex() throws IndexException {
    try {
      String q = "*:*";
      SolrQuery query = new SolrQuery(q);
      String fq = "string_field_type:catalog_entry";
      query.addField("id");
      query.addFilterQuery(fq);
      query.setRows(MAX_ROWS);
      SolrClient server = DWSolrConfig.getInstance().getSolrManager().getServer();
      long start = System.currentTimeMillis();
      QueryResponse rsp = server.query(query, METHOD.POST);
      logger.trace("CatalogEntries found: " + rsp.getResults().getNumFound());
      logger.trace("Query time: " + rsp.getQTime());
      long end = System.currentTimeMillis();
      long duration = end - start;
      duration = duration / 1000;
      // System.out.println("duration: " + duration + " s");
      CatalogManager catalogManager = DwClientConfiguration.getInstance().getCatalogManager();
      List<String> solrIdsToDelete = new ArrayList<>();
      for (SolrDocument doc : rsp.getResults()) {
        Object idO = doc.getFieldValue("id");
        String idSolr = idO.toString();
        String idDB = idSolr.replace("ce", "");
        int id = Integer.parseInt(idDB);
        CatalogEntry entry = catalogManager.getEntryByID(id);
        if (entry == null) {
          // System.out.println(id + " " + idO);
          solrIdsToDelete.add(idSolr);
        }
      }
      logger.info(solrIdsToDelete.size() + " catalog entries to delete.");
      deleteDocumetsInSolr(solrIdsToDelete);
      logger.info(solrIdsToDelete.size() + " catalog entries deleted.");
      server.commit();
      logger.info("Commited to Solr server.");
    } catch (SolrServerException | SQLException | IOException e) {
      throw new IndexException(e);
    }
  }

}
