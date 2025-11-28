package de.uniwue.dw.query.solr;

import de.uniwue.dw.core.client.authentication.group.Group;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.query.model.DWQueryConfig;
import de.uniwue.dw.query.model.client.QueryCache;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.*;
import de.uniwue.dw.query.model.result.Cell.CellType;
import de.uniwue.dw.query.solr.client.ISolrConstants;
import de.uniwue.dw.query.solr.model.visitor.Solr7ParentQueryStringVisitor;
import de.uniwue.dw.query.solr.suggest.CatalogIndexer;
import de.uniwue.dw.solr.api.DWSolrConfig;
import de.uniwue.misc.util.TimeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SolrUtil implements ISolrConstants {

  public static final String AND = "AND";

  public static final String OR = "OR";

  private static final Logger logger = LogManager.getLogger(SolrUtil.class);

  private static final Pattern fieldFormatPattern = Pattern.compile("[^a-zA-Z_0-9]");

  public static String getSolrFieldName(CatalogEntry catalogEntry) {
    CellType cellType = CellType.Value;
    return getSolrFieldName(catalogEntry, cellType);
  }

  public static String getSolrFieldName(CatalogEntry catalogEntry, CellType cellType) {
    String fieldName;
    if (cellType == CellType.Value) {
      if (catalogEntry.getDataType() == CatalogEntryType.Number) {
        fieldName = "float_" + getSolrID(catalogEntry);
      } else if (catalogEntry.getDataType() == CatalogEntryType.Bool
              || catalogEntry.getDataType() == CatalogEntryType.Structure) {
        fieldName = SOLR_FIELD_BOOLEAN_STRUCTURE_VALUE;
      } else if (catalogEntry.getDataType() == CatalogEntryType.Text) {
        fieldName = "text_" + getSolrID(catalogEntry);
      } else if (catalogEntry.getDataType() == CatalogEntryType.SingleChoice) {
        fieldName = "string_" + getSolrID(catalogEntry);
      } else if (catalogEntry.getDataType() == CatalogEntryType.DateTime) {
        fieldName = "date_" + getSolrID(catalogEntry);
      } else {
        throw new IllegalArgumentException("SolrFeldname nicht bekannt fuer Catalogentry: name="
                + catalogEntry.getName() + " extid=" + catalogEntry.getExtID() + " attrid="
                + catalogEntry.getAttrId() + " project=" + catalogEntry.getProject() + " dataType="
                + catalogEntry.getDataType());
      }
    } else if (cellType == CellType.MeasureTime) {
      fieldName = "date_" + getSolrID(catalogEntry) + "_MeasureTime";
    } else if (cellType == CellType.CaseID) {
      fieldName = "id";
    } else if (cellType == CellType.PID) {
      fieldName = "patient";
    } else if (cellType == CellType.DocID) {
      fieldName = "long_" + getSolrID(catalogEntry) + "_DocID";
      return formatFieldName(fieldName);
    } else {
      throw new IllegalArgumentException("Unknown CellType");
    }
    return formatFieldName(fieldName);
  }

  public static String getSolrFieldNameForPositiveTextSegments(CatalogEntry entry) {
    return "text_" + entry.getProject() + "_" + entry.getExtID() + "_positive";
  }

  public static String getSolrFieldNameForPositiveRecentTextSegments(CatalogEntry entry) {
    return "text_" + entry.getProject() + "_" + entry.getExtID()
            + "_positive_recent";
  }

  public static String getSolrFieldNameForString(CatalogEntry catalogEntry) {
    String fieldName = "string_" + getSolrID(catalogEntry);
    return formatFieldName(fieldName);
  }

  public static String getSolrID(CatalogEntry catalogEntry) {
    String id = catalogEntry.getProject() + "_" + catalogEntry.getExtID();
    id = formatFieldName(id);
    return id;
  }

  public static String formatFieldName(String name) {
    name = fieldFormatPattern.matcher(name).replaceAll("_");
    return name;
  }

  public static String format2SolrDate(Date date) {
    return TimeUtil.getDateFormat5().format(date);
  }

  public static String format2SolrDate(String value) {
    Date date = TimeUtil.parseDate(value);
    return format2SolrDate(date);
  }

  public static String join(String conjunction, String... parts) {
    return join(conjunction, Arrays.asList(parts));
  }

  public static String join(String conjunction, List<String> parts) {
    return parts.stream().filter(Objects::nonNull).filter(n -> !n.isEmpty())
            .collect(Collectors.joining(conjunction));
  }

  public static long getNumFound(SolrClient server, QueryRoot root)
          throws SolrServerException, QueryException, IOException {
    return getNumFound(server, root, null);
  }

  public static long getNumFound(SolrClient server, QueryRoot queryRoot, List<Group> groups)
          throws QueryException, SolrServerException, IOException {
    SolrQuery solrQuery = SolrUtil.getSolrQueryString(queryRoot);
    return getNumFound(server, queryRoot.getFilterIDTypeForDistinctCount(), queryRoot.isDistinct(), solrQuery, groups);
  }

  public static long getNumFound(SolrClient server, boolean isDistinct, String query)
          throws SolrServerException, IOException {
    return getNumFound(server, null, isDistinct, query, null);
  }

  public static long getNumFound(SolrClient server, QueryIDFilter.FilterIDType filterIDTypeForDistinctCount,
          boolean isDistinct, String query) throws SolrServerException, IOException {
    return getNumFound(server, filterIDTypeForDistinctCount, isDistinct, query, null);
  }

  public static long getNumFound(SolrClient server, boolean isDistinct, String query, List<Group> groups)
          throws SolrServerException, IOException {
    return getNumFound(server, null, isDistinct, query, groups);
  }

  public static long getNumFound(SolrClient server, QueryIDFilter.FilterIDType filterIDTypeForDistinctCount,
          boolean isDistinct, String query, List<Group> groups) throws SolrServerException, IOException {
    SolrQuery solrQuery = new SolrQuery(query);
    return getNumFound(server, filterIDTypeForDistinctCount, isDistinct, solrQuery, groups);
  }

  public static long getNumFound(SolrClient server, QueryIDFilter.FilterIDType filterIDTypeForDistinctCount,
          boolean isDistinct, SolrQuery solrQuery, List<Group> groups) throws SolrServerException, IOException {
    logger.debug("getNumFound for query: " + solrQuery.toQueryString());
    addGroupFilter2Query(groups, solrQuery);
    solrQuery.setRows(0);
    String countField = SOLR_FIELD_PATIENT_ID;
    if (isDistinct && DWQueryConfig.queryAlwaysGroupDistinctQueriesOnDocLevel() &&
            filterIDTypeForDistinctCount != null) {
      if (filterIDTypeForDistinctCount == QueryIDFilter.FilterIDType.CaseID) {
        countField = SOLR_FIELD_CASE_ID;
      } else if (filterIDTypeForDistinctCount == QueryIDFilter.FilterIDType.DocID) {
        countField = SOLR_FIELD_DOC_ID;
      }
    }
    if (isDistinct) {
      solrQuery.add("stats", "true");
      solrQuery.add("stats.field", "{!countDistinct=true}" + countField);
    }
    if (DWQueryConfig.queryUseCache()) {
      Long numFound = QueryCache.getInstance().getNumFound(solrQuery.toString());
      if (numFound != null)
        return numFound;
    }
    QueryResponse rsp = server.query(solrQuery, DWSolrConfig.getSolrMethodToUse());
    if (isDistinct)
      return rsp.getFieldStatsInfo().get(countField).getCountDistinct();
    else
      return rsp.getResults().getNumFound();
  }

  public static void addGroupFilter2Query(List<Group> groups, SolrQuery solrQuery) {
    if (groups != null && DWQueryConfig.getFilterDocumentsByGroup()) {
      String groupsFilter = CatalogIndexer.getGroupsAsValueString(groups, " OR ");
      solrQuery.addFilterQuery(ISolrConstants.SOLR_FIELD_GROUPS + ":" + groupsFilter);
    }
  }

  public static String getSolrFieldName(QueryAttribute queryElem) {
    String fieldName = getSolrFieldName(queryElem.getCatalogEntry());
    if (queryElem.getContentOperator() == ContentOperator.CONTAINS_POSITIVE
            || queryElem.getContentOperator() == ContentOperator.CONTAINS_NOT_POSITIVE) {
      fieldName = fieldName + "_positive";
    }
    return SolrUtil.formatFieldName(fieldName);
  }

  public static String getSolrDisplayFieldName(QueryAttribute queryElem) {
    if (queryElem.getCatalogEntry().getDataType() == CatalogEntryType.Bool
            || queryElem.getCatalogEntry().getDataType() == CatalogEntryType.Structure) {
      return formatFieldName(
              "string_" + SolrUtil.getSolrID(queryElem.getCatalogEntry()) + "_leaf_text");
    } else if (queryElem.getCatalogEntry().getDataType() == CatalogEntryType.Text
            || queryElem.getCatalogEntry().getDataType() == CatalogEntryType.SingleChoice
            || queryElem.getCatalogEntry().getDataType() == CatalogEntryType.Number) {
      return getSolrFieldName(queryElem.getCatalogEntry());
    }
    return getSolrFieldName(queryElem);
  }

  public static SolrQuery getSolrQueryString(QueryElem queryElem, DOC_TYPE docType)
          throws QueryException {
    if (queryElem == null)
      return new SolrQuery();
    Solr7ParentQueryStringVisitor visitor = new Solr7ParentQueryStringVisitor(docType);
    String queryString = queryElem.accept(visitor);
    SolrQuery query = new SolrQuery(queryString.trim());
    Map<String, String> params = visitor.getParams();
    for (Entry<String, String> param : params.entrySet()) {
      query.setParam(param.getKey(), param.getValue());
    }
    return query;
  }

  public static SolrQuery getSolrQueryString(QueryRoot queryRoot) throws QueryException {
    DOC_TYPE docType = getDocType(queryRoot);
    return getSolrQueryString(queryRoot, docType);
  }

  public static DOC_TYPE getDocType(QueryRoot queryRoot) {
    if (queryRoot.getFilterIDTypeToUseForCount() == QueryIDFilter.FilterIDType.PID ||
            (queryRoot.getIDFilter().size() == 1 && queryRoot.getIDFilter().get(0).getFilterIDType().equals(
                    QueryIDFilter.FilterIDType.PID) && queryRoot.getAttributesRecursive().size() == 0))
      return DOC_TYPE.PATIENT;
    else
      return DOC_TYPE.CASE;
  }

  public static String createDocTypeQuery(DOC_TYPE docType) {
    return SOLR_FIELD_DOC_TYPE + ":" + docType.docType;
  }
}
