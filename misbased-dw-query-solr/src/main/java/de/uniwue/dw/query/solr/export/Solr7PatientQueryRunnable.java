package de.uniwue.dw.query.solr.export;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.query.model.client.IGUIClient;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryIDFilter;
import de.uniwue.dw.query.model.lang.QueryOr;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.result.DesiredContentCheckerVisitor;
import de.uniwue.dw.query.model.result.Highlighter;
import de.uniwue.dw.query.model.result.PatientQueryRunnable;
import de.uniwue.dw.query.model.result.ResultCellData;
import de.uniwue.dw.query.model.result.export.ExportConfiguration;
import de.uniwue.dw.query.solr.SolrUtil;
import de.uniwue.dw.query.solr.client.ISolrConstants;
import de.uniwue.dw.query.solr.model.visitor.Solr7ChildQueryStringVisitor;
import de.uniwue.dw.solr.api.DWSolrConfig;
import de.uniwue.misc.util.TimeUtil;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class Solr7PatientQueryRunnable extends PatientQueryRunnable implements ISolrConstants {

  public static final int TOTAL_WORK = 10000;

  private static final int CHUNK_SIZE = 10;

  private static final Logger logger = LogManager.getLogger(Solr7PatientQueryRunnable.class);

  private final Highlighter highlighter = new Highlighter();

  public Solr7PatientQueryRunnable(int workTotal, QueryRoot queryRoot, User user,
          ExportConfiguration exportConfig, IGUIClient guiClient) throws QueryException {
    super(workTotal, queryRoot, user, exportConfig, guiClient);
  }

  public Solr7PatientQueryRunnable(QueryRoot queryRoot, User user, ExportConfiguration exportConfig,
          IGUIClient guiClient) throws QueryException {
    super(calcTotalWork(queryRoot.getLimitResult()), queryRoot, user, exportConfig, guiClient);
  }

  private static int calcTotalWork(int limitResult) {
    return SENDING_QUERY_WORK + TOTAL_WORK;
  }

  private static List<String> removeNextChunk(List<String> ids) {
    int toIndex = Math.min(ids.size(), CHUNK_SIZE);
    List<String> chunkList = new ArrayList<>(ids.subList(0, toIndex));
    ids.removeAll(chunkList);
    return chunkList;
  }

  private SolrClient getServerToUse() {
    if (SolrUtil.getDocType(getQueryRoot()) == DOC_TYPE.PATIENT &&
            DWSolrConfig.getSolrPatientLayerServerUrl() != null) {
      return DWSolrConfig.getInstance().getSolrManager().getPatientLayerServer();
    } else {
      return DWSolrConfig.getInstance().getSolrManager().getServer();
    }
  }

  @Override
  public long getOnlyCountNumber() throws QueryException {
    try {
      long start = System.currentTimeMillis();
      QueryRoot queryRoot = getQueryRoot();
      SolrClient server = getServerToUse();

      long numFound = SolrUtil.getNumFound(server, queryRoot, user.getGroups());

      long end = System.currentTimeMillis();
      long queryTime = end - start;
      logger.debug("query successful. result created. query duration " + queryTime);
      if (outputHandler != null) {
        outputHandler.setQueryTime(queryTime);
        outputHandler.setDocsFound(numFound);
      }
      return numFound;
    } catch (Exception e) {
      throw new QueryException(e);
    }
  }

  @Override
  public void createOutput() throws QueryException {
    long start = System.currentTimeMillis();
    List<String> ids;
    try {
      ids = queryIDs();

      logger.debug("ids size: " + ids.size());
      while (!ids.isEmpty() && !isCanceled()) {
        List<String> chunkIDs = removeNextChunk(ids);
        queryValuesForIDs(chunkIDs);
      }
      long end = System.currentTimeMillis();
      long queryTime = end - start;
      if (outputHandler != null) {
        outputHandler.setQueryTime(queryTime);
      }
    } catch (Exception e) {
      throw new QueryException(e);
    }
  }

  private void queryValuesForIDs(List<String> ids) throws QueryException {
    QueryIDFilter.FilterIDType filterIDTypeForCount = getQueryRoot().getFilterIDTypeToUseForCount();
    String idsQuery;
    if (filterIDTypeForCount == QueryIDFilter.FilterIDType.PID) {
      idsQuery = ids.stream().map(s -> SOLR_FIELD_PATIENT_ID + ":" + s)
              .collect(Collectors.joining(" OR "));
    } else if (filterIDTypeForCount == QueryIDFilter.FilterIDType.DocID) {
      idsQuery = ids.stream().map(s -> SOLR_FIELD_DOC_ID + ":" + s)
              .collect(Collectors.joining(" OR "));
    } else {
      idsQuery = ids.stream().map(s -> SOLR_FIELD_CASE_ID + ":" + s)
              .collect(Collectors.joining(" OR "));
    }
    try {
      SolrQuery query = buildQuery(idsQuery, getQueryRoot(), this.user);
      HashMap<Long, HashMap<QueryAttribute, List<Information>>> caseID2valueMap = executeQueryAndCollectValues(
              query);
      createResultRows(caseID2valueMap);
    } catch (SolrServerException | SolrException | IOException | SQLException e) {
      throw new QueryException(e);
    }
  }

  private void createResultRows(
          HashMap<Long, HashMap<QueryAttribute, List<Information>>> caseID2valueMap)
          throws QueryException {
    for (Entry<Long, HashMap<QueryAttribute, List<Information>>> caseEntry : caseID2valueMap
            .entrySet()) {
      HashMap<QueryAttribute, List<Information>> attr2InfoList = caseEntry.getValue();
      List<ResultCellData> cellDatas = new ArrayList<>();
      for (QueryAttribute queryAttribute : getAttributes()) {
        List<Information> infos = attr2InfoList.get(queryAttribute);
        ResultCellData resultCellData = new ResultCellData(infos, queryAttribute);
        cellDatas.add(resultCellData);
      }
      long pid = findAPid(cellDatas);
      createResultRow(cellDatas, pid);
    }
  }

  private long findAPid(List<ResultCellData> cellDatas) {
    for (ResultCellData cellData : cellDatas) {
      for (Information info : cellData.getValues()) {
        if (info.getPid() > 0)
          return info.getPid();
      }
    }
    return 0;
  }

  private HashMap<Long, HashMap<QueryAttribute, List<Information>>> executeQueryAndCollectValues(
          SolrQuery query) throws SolrServerException, SolrException, IOException {
    SolrClient server = getServerToUse();
    QueryResponse response = server.query(query, DWSolrConfig.getSolrMethodToUse());
    SolrDocumentList results = response.getResults();
    HashMap<Long, HashMap<QueryAttribute, List<Information>>> caseID2valueMap = new HashMap<>();
    for (SolrDocument doc : results) {
      long caseID = parseLong(doc.get(SOLR_FIELD_CASE_ID));
      long pid = parseLong(doc.get(SOLR_FIELD_PATIENT_ID));
      long docID = 0;
      if (getQueryRoot().hasToAddDocIDForQuery()) {
        Object docIDObject = doc.get(SOLR_FIELD_DOC_ID);
        docID = parseLong(docIDObject);
      }
      QueryIDFilter.FilterIDType filterIDTypeForCount = getQueryRoot().getFilterIDTypeToUseForCount();
      long groupID = caseID;
      if (filterIDTypeForCount == QueryIDFilter.FilterIDType.PID)
        groupID = pid;
      else if (filterIDTypeForCount == QueryIDFilter.FilterIDType.DocID)
        groupID = docID;
      Timestamp measureTimeStamp = parseTimestamp(doc.get(SOLR_FIELD_MEASURE_TIME));
      Collection<Object> containingFields = doc.getFieldValues(FIELD_EXISTANCE_FIELD);
      Set<String> fieldsExistence = containingFields.stream().map(Object::toString)
              .collect(Collectors.toSet());
      for (QueryAttribute attr : getAttributes()) {
        String solrID = SolrUtil.getSolrID(attr.getCatalogEntry());
        if (fieldsExistence.contains(solrID)) {
          String fieldName = SolrUtil.getSolrFieldName(attr);
          Object fieldValue = doc.getFieldValue(fieldName);
          if (checkReductionOp(doc, attr, solrID)) {
            if (fieldValue != null) {
              fieldValue = getFirstEntry(fieldValue, attr.getCatalogEntry());
            } else {
              fieldValue = "X";
            }
            if ((attr.getCatalogEntry().getDataType() == CatalogEntryType.Bool)
                    && (attr.getCatalogEntry().getChildren().size() == 0)) {
              fieldValue = "X";
            }
            Information info = createInfo(fieldValue, attr, pid, caseID, docID, measureTimeStamp);
            HashMap<QueryAttribute, List<Information>> attr2InfoList = caseID2valueMap.computeIfAbsent(groupID,
                    k -> new HashMap<>());
            List<Information> infos = attr2InfoList.computeIfAbsent(attr, k -> new ArrayList<>());
            infos.add(info);
          }
        }
      }
    }
    return caseID2valueMap;
  }

  private Object getFirstEntry(Object fieldValue, CatalogEntry catalogEntry) {
    if (fieldValue instanceof Collection) {
      Collection<Object> list = (Collection<Object>) fieldValue;
      if (list.isEmpty()) {
        fieldValue = "";
      } else if (list.size() > 1) {
        // TODO: add a more generic way to handle more than one entry;
        //  the following is only done to exclude the "DiagnoseTyp" from the result and show the real diagnoses
        boolean isDiagnoseMainEntry = catalogEntry.getProject().equalsIgnoreCase("untersuchung") &&
                catalogEntry.getExtID().equalsIgnoreCase("diagnose");
        boolean isDiagnoseSubEntry =
                catalogEntry.getProject().equalsIgnoreCase("diagnose") &&
                        !catalogEntry.getExtID().equalsIgnoreCase("diagnosetyp") &&
                        !catalogEntry.getParent().getExtID().equalsIgnoreCase("diagnosetyp");
        if (isDiagnoseMainEntry || isDiagnoseSubEntry) {
          for (Object o : list) {
            if (o.toString().contains(":")) {
              fieldValue = o;
              break;
            }
          }
        } else {
          fieldValue = list.iterator().next();
        }
      } else {
        fieldValue = list.iterator().next();
      }
    }
    return fieldValue;
  }

  private boolean checkReductionOp(SolrDocument doc, QueryAttribute attr, String solrID) {
    switch (attr.getReductionOperator()) {
      case MIN:
        return solrDocFieldContainsValue(doc, solrID, SOLR_FIELD_MIN_VALUE);
      case MAX:
        return solrDocFieldContainsValue(doc, solrID, SOLR_FIELD_MAX_VALUE);
      case EARLIEST:
        return solrDocFieldContainsValue(doc, solrID, SOLR_FIELD_FIRST_VALUE);
      case LATEST:
        return solrDocFieldContainsValue(doc, solrID, SOLR_FIELD_LAST_VALUE);
      default:
        return true;
    }
  }

  private boolean solrDocFieldContainsValue(SolrDocument doc, String solrID, String solrField) {
    Object fieldValue = doc.getFieldValue(solrField);
    if (fieldValue != null) {
      if (fieldValue instanceof Collection) {
        @SuppressWarnings("unchecked")
        Collection<Object> list = (Collection<Object>) fieldValue;
        return list.contains(solrID);
      } else {
        return fieldValue.equals(solrID);
      }
    } else {
      return false;
    }
  }

  private Information createInfo(Object fieldValue, QueryAttribute queryAttr, long patientID,
          long caseID, long docID, Timestamp measureTime) {
    CatalogEntry catalogEntry = queryAttr.getCatalogEntry();
    int catalogEntryAttrID = catalogEntry.getAttrId();
    long infoID = 0;
    long refID = 0;
    long groupID = 0;
    String value;
    double valueDec = 0;
    if (catalogEntry.getDataType() == CatalogEntryType.DateTime) {
      value = TimeUtil.format((Date) fieldValue);
    } else {
      value = fieldValue.toString();
    }
    if (catalogEntry.getDataType() == CatalogEntryType.Number) {
      try {
        valueDec = Double.parseDouble(value);
      } catch (NumberFormatException ignored) {
      }
    }
    Information info = new Information(infoID, catalogEntryAttrID, patientID, caseID, measureTime,
            null, null, value, value, valueDec, refID, docID, groupID);
    boolean isHit = true;
    if (highlighter.hasToHighlight(queryAttr)) {
      DesiredContentCheckerVisitor checkerVisitor = new DesiredContentCheckerVisitor();
      ParseTree desiredContentAsParseTree = queryAttr.getDesiredContentAsParseTree();
      checkerVisitor.visit(desiredContentAsParseTree);
      isHit = highlighter.highlight(queryAttr, info);
    }
    if (isHit) {
      if (queryAttr.isOnlyDisplayExistence()) {
        info.setValue("x");
        info.setValueShort("x");
      }
    } else if (queryAttr.isOptional() || queryAttr.getParent() instanceof QueryOr) {
      info.setValue("");
      info.setValueShort("");
    }
    return info;
  }

  private Timestamp parseTimestamp(Object object) {
    if (object instanceof Date) {
      Date d = (Date) object;
      return new Timestamp(d.getTime());
    } else
      return null;
  }

  private long parseLong(Object object) {
    if (object != null) {
      try {
        return Long.parseLong(object.toString());
      } catch (NumberFormatException e) {
        return 0;
      }
    }
    return 0;
  }

  public SolrQuery buildQuery(String idsQuery, QueryRoot queryRoot, User user)
          throws QueryException, SQLException {
    Solr7ChildQueryStringVisitor visitor = new Solr7ChildQueryStringVisitor(
            SolrUtil.getDocType(queryRoot));
    String attributesQuery = queryRoot.accept(visitor);

    String layerQuery = SOLR_FIELD_PARENT_CHILD_LAYER + ":" + CHILD_LAYER;
    String docTypeQuery = SolrUtil.createDocTypeQuery(SolrUtil.getDocType(queryRoot));

    if (visitor.getIdToTimeFilterStringMap() != null) {
      QueryIDFilter.FilterIDType filterIDTypeForCount = getQueryRoot().getFilterIDTypeToUseForCount();
      String curFilterField = SOLR_FIELD_CASE_ID;
      if (filterIDTypeForCount == QueryIDFilter.FilterIDType.PID) {
        curFilterField = SOLR_FIELD_PATIENT_ID;
      } else if (filterIDTypeForCount == QueryIDFilter.FilterIDType.DocID) {
        curFilterField = SOLR_FIELD_DOC_ID;
      }
      if (visitor.getIdFilterString().contains(curFilterField)) {
        for (Long aID : visitor.getIdToTimeFilterStringMap().keySet()) {
          String curTimeFilterString = visitor.getIdToTimeFilterStringMap().get(aID);
          String curIDFilterString = curFilterField + ":" + aID;
          idsQuery = idsQuery.replace(curIDFilterString,
                  "(" + curIDFilterString + " AND (" + curTimeFilterString + "))");
        }
      } else {
        idsQuery = idsQuery + " AND (" + visitor.getIdFilterString() + ")";
      }
    }

    String queryString = "(" + layerQuery + " AND " + docTypeQuery + ") AND (" + idsQuery
            + ") AND (" + attributesQuery + ")";

    SolrQuery query = new SolrQuery(queryString);
    logger.debug(query);

    for (QueryAttribute attr : attributes) {
      query.addField(SolrUtil.getSolrFieldName(attr));
      addReductionOperatorField(query, attr);
    }
    query.addField(SOLR_FIELD_ID);
    query.addField(SOLR_FIELD_PATIENT_ID);
    query.addField(SOLR_FIELD_CASE_ID);
    query.addField(SOLR_FIELD_MEASURE_TIME);
    query.setRows(MAX_ROWS);
    query.addField(FIELD_EXISTANCE_FIELD);

    query.setSort(SOLR_FIELD_MEASURE_TIME, ORDER.asc);

    if (getQueryRoot().hasToAddDocIDForQuery()) {
      query.addField(SOLR_FIELD_DOC_ID);
    }
    logger.debug(query);
    return query;
  }

  private void addReductionOperatorField(SolrQuery query, QueryAttribute attr) {
    switch (attr.getReductionOperator()) {
      case MIN:
        query.addField(SOLR_FIELD_MIN_VALUE);
        break;
      case MAX:
        query.addField(SOLR_FIELD_MAX_VALUE);
        break;
      case EARLIEST:
        query.addField(SOLR_FIELD_FIRST_VALUE);
        break;
      case LATEST:
        query.addField(SOLR_FIELD_LAST_VALUE);
        break;
      default:
        break;
    }
  }

  private List<String> queryIDs() throws QueryException, SolrServerException, IOException {
    SolrQuery query = createIdQuery();
    SolrClient server = getServerToUse();
    QueryResponse response = server.query(query, DWSolrConfig.getSolrMethodToUse());
    SolrDocumentList results = response.getResults();
    setNumResultsFound(response.getResults().getNumFound());
    List<String> ids = new ArrayList<>();
    for (SolrDocument d : results) {
      QueryIDFilter.FilterIDType filterIDTypeForCount = getQueryRoot().getFilterIDTypeToUseForCount();
      Object idO;
      if (filterIDTypeForCount == QueryIDFilter.FilterIDType.PID) {
        idO = d.getFieldValue(SOLR_FIELD_PATIENT_ID);
      } else if (filterIDTypeForCount == QueryIDFilter.FilterIDType.DocID) {
        idO = d.getFieldValue(SOLR_FIELD_DOC_ID);
      } else {
        idO = d.getFieldValue(SOLR_FIELD_CASE_ID);
      }
      String id = idO.toString();
      ids.add(id);
    }
    return ids;
  }

  private SolrQuery createIdQuery() throws QueryException {
    SolrQuery solrQuery = SolrUtil.getSolrQueryString(getQueryRoot());
    QueryIDFilter.FilterIDType countFilterIDType = getQueryRoot().getFilterIDTypeToUseForCount();
    if (countFilterIDType == QueryIDFilter.FilterIDType.PID) {
      solrQuery.addField(SOLR_FIELD_PATIENT_ID);
    } else if (countFilterIDType == QueryIDFilter.FilterIDType.DocID) {
      solrQuery.addField(SOLR_FIELD_DOC_ID);
    } else {
      solrQuery.addField(SOLR_FIELD_CASE_ID);
    }
    int rows = getQueryRoot().getLimitResult();
    if (rows == 0)
      rows = MAX_ROWS;
    solrQuery.setRows(rows);
    SolrUtil.addGroupFilter2Query(user.getGroups(), solrQuery);
    logger.debug(solrQuery);
    return solrQuery;
  }

}
