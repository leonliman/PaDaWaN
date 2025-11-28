package de.uniwue.dw.query.solr.preprocess;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.CountType;
import de.uniwue.dw.core.client.authentication.group.AuthManager;
import de.uniwue.dw.core.client.authentication.group.Group;
import de.uniwue.dw.core.model.data.*;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.query.model.DWQueryConfig;
import de.uniwue.dw.query.model.index.AbstractDataSource2Index;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.dw.query.model.manager.IndexLogManager;
import de.uniwue.dw.query.solr.SolrUtil;
import de.uniwue.dw.query.solr.client.ISolrConstants;
import de.uniwue.dw.query.solr.client.SolrManager;
import de.uniwue.dw.query.solr.suggest.CatalogIndexer;
import de.uniwue.dw.solr.api.DWSolrConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/*
 * This is the new indexer that derives from the abstract indexing classes
 */
public class NestedDocumentIndexer extends AbstractDataSource2Index implements ISolrConstants {

  public static final String PATIENT_DOC_TYPE = "patient";

  public static final String CASE_DOC_TYPE = "case";

  public static final String INFO_DOC_TYPE = "info";

  public static final String CASE_PREFIX = "c";

  // public static final String DOC_TYPE_FIELD_NAME = "string_doc_type";

  // public static final String EXTREME_VALUE_FIELD_NAME = "string_extreme_value";

  public static final String PATIENT_PREFIX = "p";

  public static final String INFO_PREFIX = "i";

  public static final String MEASURE_TIME_FIELD_NAME = "date_measure_time";

  public static final String REF_ID_FIELD_NAME = "string_refID";

  private static final Logger logger = LogManager.getLogger(NestedDocumentIndexer.class);

  private SolrClient server;

  private SolrClient patientLayerServer;

  private SolrManager solrManager;

  private CatalogIndexer catalogIndexer;

  public NestedDocumentIndexer() throws IndexException {
    super();
  }

  public static String documentID2SolrDocumentID(Long documentID, Long topID, DOC_TYPE docType) {
    if (documentID == null || topID == null)
      return null;
    return DOCUMENT_PREFIX + documentID + docType.prefix + topID;
  }

  public static Long solrDocumentID2DocumentID(String solrDocID, DOC_TYPE docType) {
    if (solrDocID == null)
      return null;
    return Long.parseLong(solrDocID.split(docType.prefix)[0].replaceFirst(DOCUMENT_PREFIX, ""));
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
    patientLayerServer = solrManager.getPatientLayerServer();
    AuthManager groupManager;
    try {
      groupManager = DwClientConfiguration.getInstance().getAuthManager();
      catalogIndexer = new CatalogIndexer(getCatalogManager(), groupManager);
    } catch (SQLException e) {
      throw new IndexException(e);
    }
  }

  @Override
  protected void indexCatalogEntry(CatalogEntry anEntry) throws IndexException {
    try {

      catalogIndexer.indexCatalogEntry(anEntry);
    } catch (SolrServerException | IOException e) {
      e.printStackTrace();
    }

  }

  public void indexCaseData(long pid, Case aCase, NestedDocumentCreator documentCreator)
          throws IndexException {
    if (aCase == null || aCase.getCaseID() == 0) {
      //TODO check why CaseID = 0 is ignored
      return;
    }
    try {
      SolrInputDocument doc = new SolrInputDocument();
      String solrId = caseID2SolrDocumentID(aCase.getCaseID());
      doc.addField(SOLR_FIELD_ID, solrId);

      if (!DWQueryConfig.doIncrementalUpdate()) {
        List<Group> entitledGroups = getEntitledGroupsForDataID(aCase.getCaseID());
        addGroups(doc, entitledGroups);
        doc.addField(SOLR_FIELD_PARENT_CHILD_LAYER, PARENT_LAYER);
        doc.addField(SOLR_FIELD_PATIENT_ID, pid);
        doc.addField(SOLR_FIELD_CASE_ID, aCase.getCaseID());
        doc.addField(SOLR_FIELD_DOC_TYPE, DOC_TYPE.CASE.docType);
      }

      Map<Integer, Information[]> attrID2extremeValues = documentCreator.computeExtremeValues(
              aCase.getAllContainingInfoGroups());

      ArrayList<SolrInputDocument> documents = new ArrayList<>();
      ArrayList<Document> documentsInCase = new ArrayList<>(aCase.getDocuments());
      if (!aCase.getInfos().isEmpty()) {
        Document dummyDocument = new Document(0);
        dummyDocument.setInfos(aCase.getInfos());
        documentsInCase.add(dummyDocument);
      }
      if (!aCase.getInfoGroups().isEmpty()) {
        Document dummyDocument = new Document(0);
        List<Information> infos = new ArrayList<>();
        List<InfoGroup> infoGroups = new ArrayList<>();
        for (InfoGroup infoGroup : aCase.getInfoGroups()) {
          if (Objects.equals(infoGroup.getCompoundID(), Information.COMPOUNDID_DEFAULT_VALUE))
            infos.addAll(infoGroup.getInfos());
          else
            infoGroups.add(infoGroup);
        }
        dummyDocument.setInfos(infos);
        dummyDocument.setInfoGroups(infoGroups);
        documentsInCase.add(dummyDocument);
      }
      for (Document aDocument : documentsInCase) {
        if (aDocument.getDocID() == 0L) {
          documentCreator.addInfoGroups(null, doc, DOC_TYPE.CASE, pid, aCase.getCaseID(),
                  aDocument.getAllContainingInfoGroups(), attrID2extremeValues);
        } else {
          SolrInputDocument subDoc = new SolrInputDocument();
          String solrSubId = documentID2SolrDocumentID(aDocument.getDocID(), aCase.getCaseID(), DOC_TYPE.CASE);
          subDoc.setField(SOLR_FIELD_ID, solrSubId);

          if (!DWQueryConfig.doIncrementalUpdate()) {
            List<Group> entitledGroups = getEntitledGroupsForDataID(aCase.getCaseID());
            addGroups(subDoc, entitledGroups);
            subDoc.addField(SOLR_FIELD_PARENT_CHILD_LAYER, PARENT_INTERMEDIATE_LAYER);
            subDoc.addField(SOLR_FIELD_PATIENT_ID, pid);
            subDoc.addField(SOLR_FIELD_CASE_ID, aCase.getCaseID());
            subDoc.addField(SOLR_FIELD_DOC_ID, aDocument.getDocID());
            subDoc.addField(SOLR_FIELD_DOC_TYPE, DOC_TYPE.CASE.docType);
          }

          documentCreator.addInfoGroups(doc, subDoc, DOC_TYPE.CASE, pid, aCase.getCaseID(),
                  aDocument.getAllContainingInfoGroups(), attrID2extremeValues);
          documents.add(subDoc);
        }
      }
      doc.addField("documents", documents);
      if (!isFirstIndexRun && !DWQueryConfig.doIncrementalUpdate()) {
        try {
          server.deleteById(solrId);
        } catch (SolrServerException e) {
          SolrServerException eAfterRetry = waitAndRetryDeletion(server, solrId, 1);
          if (eAfterRetry != null) {
            throw new SolrServerException(e);
          }
        }
      } else if (DWQueryConfig.doIncrementalUpdate()) {
        String queryString = SOLR_FIELD_PARENT_CHILD_LAYER + ":" + CHILD_LAYER;
        queryString += " AND ";
        queryString += SOLR_FIELD_DOC_TYPE + ":" + DOC_TYPE.CASE.docType;
        queryString += " AND ";
        queryString += SOLR_FIELD_PATIENT_ID + ":" + pid;
        queryString += " AND ";
        queryString += SOLR_FIELD_CASE_ID + ":" + aCase.getCaseID();
        queryString += " AND ";
        Set<String> solrFieldNames = new HashSet<>();
        for (Information info : aCase.getAllContainingInfos()) {
          CatalogEntry ce = getCatalogManager().getEntryByID(info.getAttrID());
          if (ce != null)
            solrFieldNames.add(SolrUtil.getSolrID(ce));
        }
        queryString += FIELD_EXISTANCE_FIELD + ":(" + String.join(" OR ", solrFieldNames) + ")";
        server.deleteByQuery(queryString);

        SolrInputDocument docForCatalogEntryRemoval = new SolrInputDocument();
        docForCatalogEntryRemoval.addField(SOLR_FIELD_ID, solrId);
        HashMap<String, Object> fieldModifier = new HashMap<>();
        fieldModifier.put("remove", solrFieldNames);
        docForCatalogEntryRemoval.addField(FIELD_EXISTANCE_FIELD, fieldModifier);
        server.add(docForCatalogEntryRemoval);
      }
      try {
        server.add(doc);
      } catch (HttpSolrClient.RemoteSolrException e) {
        HttpSolrClient.RemoteSolrException eAfterRetry = waitAndRetryAdding(server, doc, 1);
        if (eAfterRetry != null) {
          throw new SolrServerException(e);
        }
      }
    } catch (SolrServerException | SQLException | IOException | InterruptedException e) {
      throw new IndexException(e);
    }
  }

  public void indexPatientData(Patient patient, NestedDocumentCreator documentCreator)
          throws IndexException {

    if (patient == null || patient.getPid() == 0) {
      return;
    }
    try {
      long pid = patient.getPid();
      SolrInputDocument doc = new SolrInputDocument();
      String solrID = patientID2SolrDocumentID(pid);
      doc.addField(SOLR_FIELD_ID, solrID);
      if (!DWQueryConfig.doIncrementalUpdate()) {
        List<Group> entitledGroups = getEntitledGroupsForDataID(pid);
        addGroups(doc, entitledGroups);
        doc.addField(SOLR_FIELD_PARENT_CHILD_LAYER, PARENT_LAYER);
        doc.addField(SOLR_FIELD_PATIENT_ID, pid);
        doc.addField(SOLR_FIELD_DOC_TYPE, DOC_TYPE.PATIENT.docType);
      }

      Map<Integer, Information[]> attrID2extremeValues = documentCreator.computeExtremeValues(
              patient.getAllContainingInfoGroups());

      ArrayList<SolrInputDocument> documents = new ArrayList<>();
      ArrayList<Document> documentsInPatient = new ArrayList<>(patient.getAllContainingDocuments());
      List<List<Information>> infosWithoutDocumentID = patient.getAllInfosWithoutDocumentID();
      List<List<InfoGroup>> infoGroupsWithoutDocumentID = patient.getAllInfoGroupsWithoutDocumentID();
      for (int i = 0; i < infosWithoutDocumentID.size(); i++) {
        List<Information> infoList = infosWithoutDocumentID.get(i);
        List<InfoGroup> infoGroupList = infoGroupsWithoutDocumentID.get(i);
        Document dummyDocument = new Document(0);
        dummyDocument.setInfos(infoList);
        dummyDocument.setInfoGroups(infoGroupList);
        documentsInPatient.add(dummyDocument);
      }
      for (Document aDocument : documentsInPatient) {
        if (aDocument.getDocID() == 0L) {
          documentCreator.addInfoGroups(null, doc, DOC_TYPE.PATIENT, pid, null,
                  aDocument.getAllContainingInfoGroups(), attrID2extremeValues);
        } else {
          SolrInputDocument subDoc = new SolrInputDocument();
          String solrSubId = documentID2SolrDocumentID(aDocument.getDocID(), pid, DOC_TYPE.PATIENT);
          subDoc.setField(SOLR_FIELD_ID, solrSubId);

          if (!DWQueryConfig.doIncrementalUpdate()) {
            List<Group> entitledGroups = getEntitledGroupsForDataID(pid);
            addGroups(subDoc, entitledGroups);
            subDoc.addField(SOLR_FIELD_PARENT_CHILD_LAYER, PARENT_INTERMEDIATE_LAYER);
            subDoc.addField(SOLR_FIELD_PATIENT_ID, pid);
            subDoc.addField(SOLR_FIELD_DOC_ID, aDocument.getDocID());
            subDoc.addField(SOLR_FIELD_DOC_TYPE, DOC_TYPE.PATIENT.docType);
          }

          documentCreator.addInfoGroups(doc, subDoc, DOC_TYPE.PATIENT, pid, null,
                  aDocument.getAllContainingInfoGroups(), attrID2extremeValues);
          documents.add(subDoc);
        }
      }
      doc.addField("documents", documents);

      if (!isFirstIndexRun && !DWQueryConfig.doIncrementalUpdate()) {
        try {
          if (patientLayerServer != null) {
            patientLayerServer.deleteById(solrID);
          } else {
            server.deleteById(solrID);
          }
        } catch (SolrServerException e) {
          SolrServerException eAfterRetry;
          if (patientLayerServer != null) {
            eAfterRetry = waitAndRetryDeletion(patientLayerServer, solrID, 1);
          } else {
            eAfterRetry = waitAndRetryDeletion(server, solrID, 1);
          }
          if (eAfterRetry != null) {
            throw new SolrServerException(e);
          }
        }
      } else if (DWQueryConfig.doIncrementalUpdate()) {
        String queryString = SOLR_FIELD_PARENT_CHILD_LAYER + ":" + CHILD_LAYER;
        queryString += " AND ";
        queryString += SOLR_FIELD_DOC_TYPE + ":" + DOC_TYPE.PATIENT.docType;
        queryString += " AND ";
        queryString += SOLR_FIELD_PATIENT_ID + ":" + pid;
        queryString += " AND ";
        Set<String> solrFieldNames = new HashSet<>();
        for (Information info : patient.getAllContainingInfos()) {
          CatalogEntry ce = getCatalogManager().getEntryByID(info.getAttrID());
          if (ce != null)
            solrFieldNames.add(SolrUtil.getSolrID(ce));
        }
        queryString += FIELD_EXISTANCE_FIELD + ":(" + String.join(" OR ", solrFieldNames) + ")";
        if (patientLayerServer != null) {
          patientLayerServer.deleteByQuery(queryString);
        } else {
          server.deleteByQuery(queryString);
        }

        SolrInputDocument docForCatalogEntryRemoval = new SolrInputDocument();
        docForCatalogEntryRemoval.addField(SOLR_FIELD_ID, solrID);
        HashMap<String, Object> fieldModifier = new HashMap<>();
        fieldModifier.put("remove", solrFieldNames);
        docForCatalogEntryRemoval.addField(FIELD_EXISTANCE_FIELD, fieldModifier);
        if (patientLayerServer != null) {
          patientLayerServer.add(docForCatalogEntryRemoval);
        } else {
          server.add(docForCatalogEntryRemoval);
        }
      }
      try {
        if (patientLayerServer != null) {
          patientLayerServer.add(doc);
        } else {
          server.add(doc);
        }
      } catch (HttpSolrClient.RemoteSolrException e) {
        HttpSolrClient.RemoteSolrException eAfterRetry;
        if (patientLayerServer != null) {
          eAfterRetry = waitAndRetryAdding(patientLayerServer, doc, 1);
        } else {
          eAfterRetry = waitAndRetryAdding(server, doc, 1);
        }
        if (eAfterRetry != null) {
          throw new SolrServerException(e);
        }
      }
    } catch (SolrServerException | SQLException | IOException | InterruptedException e) {
      throw new IndexException(e);
    }

  }

  private SolrServerException waitAndRetryDeletion(SolrClient currentServer, String curID, int retryCount)
          throws InterruptedException, IOException {
    try {
      int waitTimeInMillis = retryCount * 60 * 1000;
      System.err.println("Doing deletion-retry number " + retryCount + "; waiting for " + waitTimeInMillis + "ms");
      Thread.sleep(waitTimeInMillis);
      currentServer.deleteById(curID);
      return null;
    } catch (SolrServerException e) {
      if (retryCount < 10) {
        return waitAndRetryDeletion(currentServer, curID, retryCount + 1);
      } else {
        return e;
      }
    }
  }

  private HttpSolrClient.RemoteSolrException waitAndRetryAdding(SolrClient currentServer, SolrInputDocument doc,
          int retryCount)
          throws InterruptedException, IOException, SolrServerException {
    try {
      int waitTimeInMillis = retryCount * 60 * 1000;
      System.err.println("Doing adding-retry number " + retryCount + "; waiting for " + waitTimeInMillis + "ms");
      Thread.sleep(waitTimeInMillis);
      currentServer.add(doc);
      return null;
    } catch (HttpSolrClient.RemoteSolrException e) {
      if (retryCount < 10) {
        return waitAndRetryAdding(currentServer, doc, retryCount + 1);
      } else {
        return e;
      }
    }
  }

  @Override
  public void indexData(Patient patient) throws IndexException {
    NestedDocumentCreator documentCreator;
    try {
      documentCreator = new NestedDocumentCreator(getCatalogManager());
      for (Case aCase : patient.getCases()) {
        indexCaseData(patient.getPid(), aCase, documentCreator);
      }
      if (DWSolrConfig.indexPatientLayer()) {
        indexPatientData(patient, documentCreator);
      }
    } catch (SQLException e) {
      throw new IndexException(e);
    }
  }

  private void addGroups(SolrInputDocument doc, List<Group> entitledGroups) {
    for (Group g : entitledGroups) {
      doc.addField(ISolrConstants.SOLR_FIELD_GROUPS, g.getName());
    }
  }

  @Override
  protected String getServerID() {
    String result = solrManager.getServerURL();
    if (solrManager.getPatientLayerServerURL() != null) {
      result += " & " + solrManager.getPatientLayerServerURL();
    }
    return result;
  }

  @Override
  public void deleteIndex() throws IndexException {
    try {
      server.deleteByQuery("*:*");
      if (patientLayerServer != null) {
        patientLayerServer.deleteByQuery("*:*");
      }
      commit();
    } catch (SolrServerException | IOException e) {
      throw new IndexException(e);
    }
  }

  @Override
  protected void commit() throws IndexException {
    try {
      server.commit();
      if (patientLayerServer != null) {
        patientLayerServer.commit();
      }
    } catch (SolrServerException | IOException e) {
      throw new IndexException(e);
    }
  }

  @Override
  protected void deleteData(long dataID, List<Information> info2Delete) throws IndexException {
    try {
      if (DWSolrConfig.indexPatientLayer()) {
        if (patientLayerServer != null) {
          patientLayerServer.deleteById(patientID2SolrDocumentID(dataID));
        } else {
          server.deleteById(patientID2SolrDocumentID(dataID));
        }
      }
      HashSet<Long> handledCaseIDs = new HashSet<Long>();
      for (Information anInfo : info2Delete) {
        long caseID = anInfo.getCaseID();
        if (!handledCaseIDs.contains(caseID)) {
          server.deleteById(caseID2SolrDocumentID(caseID));
          handledCaseIDs.add(caseID);
        }
      }
      /* If only some infos of a patient have been deleted and no values have been updated,
      the values that are still present for this patient have to be reindexed */
      indexData(dataID);
    } catch (SolrServerException | IOException e) {
      throw new IndexException(e);
    }
  }

  @Override
  protected long calculateCount(CatalogEntry anEntry, CountType countType) throws IndexException {
    try {
      long count = 0;
      String sorlFieldName = SolrUtil.getSolrID(anEntry);
      String queryString = "containing_fields:" + sorlFieldName;
      if (countType == CountType.distinctPID) {
        String docTypeQuery = SolrUtil.createDocTypeQuery(DOC_TYPE.PATIENT);
        String layerQuery = SOLR_FIELD_PARENT_CHILD_LAYER + ":" + PARENT_LAYER;
        queryString += " AND " + docTypeQuery + " AND " + layerQuery;
      } else if (countType == CountType.distinctCaseID) {
        String docTypeQuery = SolrUtil.createDocTypeQuery(DOC_TYPE.CASE);
        String layerQuery = SOLR_FIELD_PARENT_CHILD_LAYER + ":" + PARENT_LAYER;
        queryString += " AND " + docTypeQuery + " AND " + layerQuery;
      } else {
        String docTypeQuery = SolrUtil.createDocTypeQuery(DOC_TYPE.CASE);
        String layerQuery = SOLR_FIELD_PARENT_CHILD_LAYER + ":" + CHILD_LAYER;
        queryString += " AND " + docTypeQuery + " AND " + layerQuery;
      }
      if (patientLayerServer != null && countType == CountType.distinctPID) {
        count = SolrUtil.getNumFound(patientLayerServer, false, queryString);
      } else {
        count = SolrUtil.getNumFound(server, false, queryString);
      }
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
      QueryResponse rsp = server.query(query, DWSolrConfig.getSolrMethodToUse());
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

  public int getNumOfSegments() throws IOException, SolrServerException {
    CoreAdminRequest request = new CoreAdminRequest();
    request.setAction(CoreAdminParams.CoreAdminAction.STATUS);
    String collectionName;
    CoreAdminResponse response;
    String rootSolrURL = DWSolrConfig.getSolrServerUrl();
    if (rootSolrURL == null) {
      collectionName = "v2";
      EmbeddedSolrServer embeddedSolrServer = (EmbeddedSolrServer) DWSolrConfig.getInstance().getSolrManager()
              .getServer();
      response = request.process(embeddedSolrServer);
    } else {
      String stringToFind = "/solr/";
      int beginIndex = rootSolrURL.lastIndexOf(stringToFind);
      collectionName = rootSolrURL.substring(beginIndex).replace(stringToFind, "");
      if (collectionName.endsWith("/")) {
        collectionName = collectionName.substring(0, collectionName.length() - 1);
      }
      rootSolrURL = rootSolrURL.substring(0, beginIndex);
      HttpSolrClient solrClient = new HttpSolrClient.Builder(rootSolrURL + stringToFind).build();
      response = request.process(solrClient);
      solrClient.close();
    }
    for (int i = 0; i < response.getCoreStatus().size(); i++) {
      String name = response.getCoreStatus().getName(i);
      if (name.contains(collectionName)) {
        NamedList<Object> status = response.getCoreStatus().get(name);
        return (int) ((NamedList<Object>) status.get("index")).get("segmentCount");
      }
    }
    return -1;
  }

  public void optimizeIndex() throws IOException, SolrServerException {
    optimizeIndex(1);
  }

  public void optimizeIndex(int numSegments) throws IOException, SolrServerException {
    IndexLogManager.info("Started optimization of the index to " + numSegments + " segments", getServerID());
    UpdateResponse response = server.optimize(true, true, numSegments);
    IndexLogManager.info("Optimization of the index  to " + numSegments + " segments finished in "
            + response.getElapsedTime() + "ms with status code " + response.getStatus(), getServerID());
    if (patientLayerServer != null) {
      IndexLogManager.info("Started optimization of the patient layer index to " + numSegments + " segments",
              getServerID());
      response = patientLayerServer.optimize(true, true, numSegments);
      IndexLogManager.info("Optimization of the patient layer index to " + numSegments +
              " segments finished in " + response.getElapsedTime() + "ms with status code " +
              response.getStatus(), getServerID());
    }
  }

}
