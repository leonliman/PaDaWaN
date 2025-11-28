package de.uniwue.dw.query.solr.model.manager;

import de.uniwue.dw.core.client.authentication.CountType;
import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.client.authentication.group.Group;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.manager.AbstractCatalogClientManager;
import de.uniwue.dw.core.model.manager.DataSourceException;
import de.uniwue.dw.query.model.DWQueryConfig;
import de.uniwue.dw.query.solr.SolrUtil;
import de.uniwue.dw.query.solr.client.SolrManager;
import de.uniwue.dw.query.solr.suggest.CatalogIndexer;
import de.uniwue.dw.solr.api.DWSolrConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

import static de.uniwue.dw.query.solr.suggest.CatalogIndexer.*;

public class SolrCatalogClientManager extends AbstractCatalogClientManager {

  private static final Logger logger = LogManager.getLogger(SolrCatalogClientManager.class);

  private static SolrCatalogClientManager inst;

  private final SolrCatalogEntry root = createRoot();

  private SolrManager solrManager;

  private SolrCatalogClientManager() {

    solrManager = DWSolrConfig.getInstance().getSolrManager();
  }

  private static SolrCatalogEntry createRoot() {
    CatalogEntry rootAsCE = CatalogEntry.createRoot();
    return new SolrCatalogEntry(rootAsCE);
  }

  public static SolrCatalogClientManager getInst() {
    if (inst == null) {
      inst = new SolrCatalogClientManager();
    }
    return inst;
  }

  public static CatalogFilter newFilter() {
    return new CatalogFilter();
  }

  private static void buildTree(CatalogEntry parent,
          HashMap<String, List<CatalogEntry>> partent2childs) {
    List<CatalogEntry> originalTreechilds = partent2childs.get(parent.toStringID());
    if (originalTreechilds != null) {
      List<CatalogEntry> newTreeChilds = originalTreechilds.stream()
              .map(n -> n.copyWihtoutReferences()).collect(Collectors.toList());
      newTreeChilds.forEach(n -> n.setParent(parent));
      parent.setChildren(newTreeChilds);
      newTreeChilds.forEach(n -> buildTree(n, partent2childs));
    } else {
      parent.setChildren(new ArrayList<>());
    }
  }

  private static void addAllAncestorsToHM(CatalogEntry entry,
          HashMap<String, List<CatalogEntry>> partent2childs) {
    CatalogEntry parent = entry.getParent();
    if (parent != null) {
      List<CatalogEntry> filteredChilds = Optional
              .ofNullable(partent2childs.get(parent.toStringID())).orElse(new ArrayList<>());
      if (!filteredChilds.contains(entry))
        filteredChilds.add(entry);
      partent2childs.put(parent.toStringID(), filteredChilds);
      addAllAncestorsToHM(parent, partent2childs);
    }
  }

  public boolean isRoot(CatalogEntry entry) {
    return (root.getExtID().equals(entry.getExtID())
            && root.getProject().equals(entry.getProject()));
  }

  @Override
  public SolrCatalogEntry getRoot() {
    return root;
  }

  public CatalogEntry getParent(SolrCatalogEntry entry) {
    int parentID = entry.getParentID();
    if (parentID > 0) {
      CatalogFilter filter = newFilter().setAttrid(parentID);
      try {
        Optional<CatalogEntry> parent = getFirstEntries(filter);
        if (parent.isPresent())
          return parent.get();
        else
          // must be root
          return null;
      } catch (SolrServerException | IOException e) {
        e.printStackTrace();
      }
    } else if (parentID == 0)
      return getRoot();
    else if (entry.isRoot())
      return null;
    return null;
  }

  public Optional<CatalogEntry> getFirstEntries(CatalogFilter filter)
          throws SolrServerException, IOException {
    List<CatalogEntry> entries = getEntries(filter);
    if (entries.size() > 0)
      return Optional.of(entries.get(0));
    return Optional.empty();
  }

  public List<CatalogEntry> getEntries(CatalogFilter filter)
          throws SolrServerException, IOException {
    SolrQuery query = buildQuery(filter);
    return executeQuery(query);
  }

  private List<CatalogEntry> executeQuery(SolrQuery query) throws SolrServerException, IOException {
    List<CatalogEntry> result = new ArrayList<>();
    logger.debug(query);
    QueryResponse response = solrManager.getServer().query(query, DWSolrConfig.getSolrMethodToUse());
    SolrDocumentList results = response.getResults();
    for (SolrDocument doc : results) {
      SolrCatalogEntry entry = createCatalogEntry(doc);
      result.add(entry);
    }
    return result;
  }

  private SolrCatalogEntry createCatalogEntry(SolrDocument doc) {
    Object oAttrId = doc.getFieldValue(FIELD_CATALOG_ATTRIBUTE_ID);
    Object oName = doc.getFieldValue(FIELD_CATALOG_NAME);
    Object oCountDocument = doc.getFieldValue(FIELD_CATALOG_COUNT_DOCUMENT);
    Object oCountDocumentGroup = doc.getFieldValue(FIELD_CATALOG_COUNT_DOCUMENT_GROUP);
    Object oCountInfo = doc.getFieldValue(FIELD_CATALOG_COUNT_INFO);
    Object oUserGroups = doc.getFieldValue(FIELD_CATALOG_USER_GROUPS);
    Object oDataType = doc.getFieldValue(FIELD_CATALOG_DATA_TYPE);
    Object oExtID = doc.getFieldValue(FIELD_CATALOG_EXT_ID);
    Object oProject = doc.getFieldValue(FIELD_CATALOG_PROJECT);
    Object oUniqueName = doc.getFieldValue(FIELD_CATALOG_UNIQUE_NAME);
    Object oParentId = doc.getFieldValue(FIELD_CATALOG_PARENT_ID);
    Object oOrderValue = doc.getFieldValue(FIELD_CATALOG_ORDER_VALUE);
    Object oDescription = doc.getFieldValue(FIELD_CATALOG_DESCRIPTION);
    Object oCreationTime = doc.getFieldValue(FIELD_CATALOG_CREATION_TIME);
    Object olowBound = doc.getFieldValue(FIELD_CATALOG_LOW_BOUND);
    Object ohighBound = doc.getFieldValue(FIELD_CATALOG_HIGH_BOUND);

    int anAttrID = CatalogIndexer.solrDocumentID2CatalogEntryID(toString(oAttrId));
    String aName = toString(oName);
    CatalogEntryType aDataType = toCatalogEntryType(oDataType);
    String anExtID = toString(oExtID);
    int aParentID = toInt(oParentId);
    double anOrderValue = toDouble(oOrderValue);
    String aProject = toString(oProject);
    String aUniqueName = toString(oUniqueName);
    String aDescription = toString(oDescription);
    Timestamp aCreationTime = toTimeStamp(oCreationTime);

    int countDocument = toInt(oCountDocument);
    int countDocumentGroup = toInt(oCountDocumentGroup);
    int countInfo = toInt(oCountInfo);

    double lowBound = toDouble(olowBound);
    double highBound = toDouble(ohighBound);

    List<String> userGroups = toStringList(oUserGroups);

    return new SolrCatalogEntry(anAttrID, aName, aDataType, anExtID, aParentID, anOrderValue,
            aProject, aUniqueName, aDescription, aCreationTime, countDocument, countDocumentGroup,
            countInfo, userGroups, lowBound, highBound);
  }

  private List<String> toStringList(Object o) {
    List<String> result = new ArrayList<>();
    if (o == null)
      return result;
    if (o instanceof Iterable) {
      Iterable iterable = (Iterable) o;
      for (Object io : iterable) {
        String s = toString(io);
        result.add(s);
      }
    }
    return result;
  }

  private Timestamp toTimeStamp(Object o) {
    if (o == null)
      return null;
    o = reduceList(o);
    if (o instanceof Date) {
      Date d = (Date) o;
      return new Timestamp(d.getTime());
    }
    return null;
  }

  private double toDouble(Object o) {
    if (o == null)
      return -1;
    o = reduceList(o);
    String s = o.toString();
    try {
      double d = Double.parseDouble(s);
      return d;
    } catch (NumberFormatException e) {
      e.printStackTrace();
    }
    return -1;
  }

  private CatalogEntryType toCatalogEntryType(Object o) {
    if (o == null)
      return null;
    o = reduceList(o);
    return CatalogEntryType.parse(o.toString());
  }

  private String toString(Object o) {
    if (o == null)
      return "";
    o = reduceList(o);
    String s = o.toString();
    return s;
  }

  private int toInt(Object o) {
    if (o == null)
      return -1;
    o = reduceList(o);
    String s = o.toString();
    try {
      int i = Integer.parseInt(s);
      return i;
    } catch (NumberFormatException e) {
      e.printStackTrace();
    }
    return -1;
  }

  private Object reduceList(Object o) {
    if (o != null) {
      if (o instanceof List) {
        for (Object lo : (List) o) {
          return lo;
        }
      }
    }
    return o;
  }

  private SolrQuery buildQuery(CatalogFilter filter) {
    SolrQuery q = buildQuery(filter, "*:*");
    setOrderBy(q, filter.getOrderByField(), filter.getOrderByOrientation());

    return q;
  }

  private SolrQuery buildQuery(CatalogFilter filter, String query) {
    SolrQuery q = new SolrQuery(query);

    addEqualsFilter(q, CatalogIndexer.FIELD_TYPE, CatalogIndexer.TYPE_CATALOG_ENTRY);

    addEqualsFilter(q, FIELD_CATALOG_ATTRIBUTE_ID,
            CatalogIndexer.catalogEntryID2SolrDocumentID(filter.getAttrid()));
    addEqualsFilter(q, FIELD_CATALOG_NAME_AS_STRING, filter.getName());
    addEqualsFilter(q, FIELD_CATALOG_EXT_ID, filter.getExtID());
    addEqualsFilter(q, FIELD_CATALOG_PARENT_ID, filter.getParentID());
    addEqualsFilter(q, FIELD_CATALOG_DATA_TYPE, filter.getDataType());
    addEqualsFilter(q, FIELD_CATALOG_PROJECT, filter.getProject());
    addEqualsFilter(q, FIELD_CATALOG_UNIQUE_NAME, filter.getUniqueName());
    addEqualsFilter(q, FIELD_CATALOG_DESCRIPTION, filter.getDescription());
    addEqualsFilter(q, FIELD_CATALOG_COUNT_DOCUMENT, filter.getCountDocument());
    addEqualsFilter(q, FIELD_CATALOG_COUNT_INFO, filter.getCountInfo());
    addEqualsFilter(q, FIELD_CATALOG_COUNT_DOCUMENT_GROUP, filter.getCountDocumentGroup());
    addEqualsFilter(q, FIELD_CATALOG_NAME, filter.getNamePrefixAsValue(), false);

    addAnyOfSetEqualsFilter(q, FIELD_CATALOG_USER_GROUPS, filter.getUserGroupsAsStringList());

    addMoreFilter(q, FIELD_CATALOG_COUNT_DOCUMENT, filter.getCountDocumentMin());
    addMoreFilter(q, FIELD_CATALOG_COUNT_INFO, filter.getCountInfoMin());
    addMoreFilter(q, FIELD_CATALOG_COUNT_DOCUMENT_GROUP, filter.getCountDocumentGroupMin());

    setRowLimit(q, filter.getLimitResults());

    return q;
  }

  private void addAnyOfSetEqualsFilter(SolrQuery query, String solrField,
          Collection<String> valueList) {
    if (valueList != null && !valueList.isEmpty()) {
      String value = valueList.stream().collect(Collectors.joining(" OR "));
      value = "(" + value + ")";
      query.addFilterQuery(solrField + ":" + value);
    }
  }

  private void addMoreFilter(SolrQuery query, String solrField, int value) {
    if (value != -1) {
      query.addFilterQuery(solrField + ":[" + value + " TO *]");
      System.out.println(solrField);
    }
  }

  private void setRowLimit(SolrQuery q, int limit) {
    if (limit > 0)
      q.setRows(limit);
  }

  private void setOrderBy(SolrQuery q, String orderByField, ORDER orientation) {
    if (orderByField == null) {
      orderByField = FIELD_CATALOG_COUNT_DOCUMENT;
      orientation = ORDER.desc;
    } else {
      if (orientation == null)
        orientation = ORDER.asc;
    }
    q.setSort(orderByField, orientation);
  }

  private void addEqualsFilter(SolrQuery query, String solrField, int value) {
    if (value != -1) {
      query.addFilterQuery(solrField + ":" + value);
    }
  }

  private void addEqualsFilter(SolrQuery query, String solrField, Object value) {
    addEqualsFilter(query, solrField, value, true);
  }

  private void addEqualsFilter(SolrQuery query, String solrField, Object value, boolean escape) {
    if (value != null) {
      String valueString = value.toString();
      // valueString = valueString.toLowerCase();
      if (escape)
        valueString = ClientUtils.escapeQueryChars(valueString);
      query.addFilterQuery(solrField + ":" + valueString);
    }
  }

  public List<CatalogEntry> getChildsOf(CatalogEntry parent) {
    return getChildsOf(parent, null);
  }

  @Override
  public List<CatalogEntry> getChildsOf(CatalogEntry parent, User user) {
    return getChildsOf(parent, user, null, -1);
  }

  @Override
  public List<CatalogEntry> getChildsOf(CatalogEntry parent, User user, CountType countType,
          int minOccurrence) {
    int attrId = parent.getAttrId();
    CatalogFilter filter = newFilter().setParentID(attrId).setUser(user)
            .setMinOccurrence(countType, minOccurrence).setOrderByField(FIELD_CATALOG_ORDER_VALUE);
    return queryAndCatchExceptions(filter);
  }

  private List<CatalogEntry> queryAndCatchExceptions(CatalogFilter filter) {
    try {
      List<CatalogEntry> entries = getEntries(filter);
      return entries;
    } catch (SolrServerException | IOException e) {
      e.printStackTrace();
    }
    return new ArrayList<CatalogEntry>();
  }

  @Override
  public List<CatalogEntry> getEntriesByWordFilter(String word, User user, int limit) {
    return getEntriesByWordFilter(word, null, null, -1, limit);
  }

  @Override
  public List<CatalogEntry> getEntriesByWordFilter(String word, User user, CountType countType,
          int minOccurrence, int limit) {
    // CatalogFilter filter = newFilter().setNamePrefix(word).setUser(user)
    // .setMinOccurrence(countType, minOccurrence).setLimitResuls(limit);
    // return queryAndCatchExceptions(filter);

    String queryString = createBoostedNameQueryString(word);
    CatalogFilter filter = newFilter().setUser(user).setMinOccurrence(countType, minOccurrence)
            .setLimitResuls(limit);
    SolrQuery query = buildQuery(filter, queryString);
    try {
      List<CatalogEntry> results = executeQuery(query);
      return results;
    } catch (SolrServerException | IOException e) {
      e.printStackTrace();
    }
    return new ArrayList<>();

  }

  public List<CatalogEntry> getEntriesByWordAndDataTypeFilter(String word, User user,
          CatalogEntryType dataType, int limit) {
    String queryString = createBoostedNameQueryString(word);
    CatalogFilter filter = newFilter().setUser(user).setDataType(dataType).setLimitResuls(limit);
    SolrQuery query = buildQuery(filter, queryString);
    try {
      List<CatalogEntry> results = executeQuery(query);
      return results;
    } catch (SolrServerException | IOException e) {
      e.printStackTrace();
    }
    return new ArrayList<>();
  }

  private String createBoostedNameQueryString(String word) {
    // String queryStri/ng = "*:*";
    if (word != null && !word.isEmpty()) {
      String wordQueryToken = CatalogIndexer.aliaseTokenizer(word).stream()
              .map(s -> ClientUtils.escapeQueryChars(s)).map(s -> "*" + s + "*")
              .collect(Collectors.joining(" AND "));
      // String[] split = word.split(" ");
      // String wordQueryToken = Arrays.asList(split).stream()
      // .map(s -> "*" + ClientUtils.escapeQueryChars(s) + "*")
      // .collect(Collectors.joining(" AND "));
      wordQueryToken = "(" + wordQueryToken + ")";
      String boostField = CatalogIndexer.FIELD_CATALOG_COUNT_DOCUMENT;
      String textquery = CatalogIndexer.FIELD_CATALOG_ALIASES + ":" + wordQueryToken;
      String queryString = "{!boost b=" + boostField + "}" + textquery;
      return queryString;
    }

    return "-id:[* TO *]";
  }

  @Override
  public CatalogEntry getTreeByWordFilter(String searchPhrase, User user) {
    return getTreeByWordFilter(searchPhrase, user, null, -1);
  }

  @Override
  public CatalogEntry getEntryByRefID(String extID, String project, User user)
          throws DataSourceException {
    return getEntryByRefID(extID, project, user, true);
  }

  @Override
  public CatalogEntry getEntryByRefID(String extID, String project, User user,
          boolean throwExceptionIfNotExists) throws DataSourceException {
    CatalogFilter filter = newFilter().setExtID(extID).setProject(project).setUser(user)
            .setLimitResuls(1);
    try {
      List<CatalogEntry> entries = getEntries(filter);
      if (!entries.isEmpty()) {
        return entries.get(0);
      } else {
        if (throwExceptionIfNotExists) {
          throw new DataSourceException("Entry does not exist. Extid=" + extID + ", project="
                  + project + " user=" + user);
        } else {
          return null;
        }
      }
    } catch (SolrServerException | IOException e) {
      e.printStackTrace();
      throw new DataSourceException(e);
    }
  }

  @Override
  public CatalogEntry getTreeByWordFilter(String searchPhrase, User user, CountType countType,
          int minOccurrence) {
    CatalogFilter filter = newFilter().setNamePrefix(searchPhrase).setUser(user)
            .setMinOccurrence(countType, minOccurrence);
    List<CatalogEntry> hits = queryAndCatchExceptions(filter);
    return getTreeByFilters(hits);
  }

  @Override
  public CatalogEntry getAllAncestorsAndSiblings(CatalogEntry entry, User user, CountType countType,
          int minOccurrence) {
    HashMap<String, List<CatalogEntry>> partent2childs = new HashMap<>();
    CatalogEntry parent = entry.getParent();
    while (parent != null) {
      List<CatalogEntry> sons = getChildsOf(parent, user, countType, minOccurrence);
      partent2childs.put(parent.toStringID(), sons);
      parent = parent.getParent();
    }
    CatalogEntry filteredTree = getRoot().copyWihtoutReferences();
    buildTree(filteredTree, partent2childs);
    return filteredTree;
  }

  private CatalogEntry getTreeByFilters(List<CatalogEntry> hits) {
    HashMap<String, List<CatalogEntry>> partent2childs = new HashMap<>();
    hits.forEach(c -> addAllAncestorsToHM(c, partent2childs));
    CatalogEntry filteredTree = getRoot().copyWihtoutReferences();
    buildTree(filteredTree, partent2childs);
    return filteredTree;
  }

  @Override
  public void dispose() {
    solrManager = null;
    inst = null;
    super.dispose();
  }

  @Override
  public CatalogEntry getEntryByID(int attrId, User user) throws DataSourceException {
    if (attrId == 0)
      return getRoot();
    CatalogFilter filter = newFilter().setAttrid(attrId).setUser(user).setLimitResuls(1);
    try {
      List<CatalogEntry> entries = getEntries(filter);
      if (!entries.isEmpty())
        return entries.get(0);
      else
        throw new DataSourceException(
                "Entrie does not exist or user is not autherized. attrId=" + attrId);
    } catch (SolrServerException | IOException e) {
      e.printStackTrace();
      throw new DataSourceException(e);
    }
  }

  public CatalogEntry getCatalogEntryByNameOrUniqueName(String uniqueName, User user)
          throws SolrServerException, IOException {
    if (uniqueName == null || uniqueName.isEmpty())
      return null;
    // uniqueName = uniqueName.toLowerCase();

    String q = CatalogIndexer.FIELD_CATALOG_UNIQUE_NAME + ":\"" + uniqueName + "\" OR "
            + CatalogIndexer.FIELD_CATALOG_NAME_AS_STRING + ":\"" + uniqueName + "\"";
    CatalogFilter filter = newFilter().setLimitResuls(1).setUser(user)
            .setOrderByField(FIELD_CATALOG_COUNT_DOCUMENT).setOrderByOrientation(ORDER.desc);
    SolrQuery query = buildQuery(filter, q);
    List<CatalogEntry> results = executeQuery(query);
    if (results.isEmpty())
      return null;
    else
      return results.get(0);
  }

  public List<CatalogEntry> getSiblings(SolrCatalogEntry entry) {
    int parentID = entry.getParentID();
    CatalogFilter filter = newFilter().setParentID(parentID);
    try {
      List<CatalogEntry> siblings = getEntries(filter);
      siblings.remove(entry);
      return siblings;
    } catch (SolrServerException | IOException e) {
      e.printStackTrace();
    }
    return new ArrayList<>();
  }

  public void personaliseCounts(List<CatalogEntry> entries, User user, CountType countType) {
    entries.forEach(n -> personaliseCounts(n, user, countType));
  }

  public void personaliseCounts(CatalogEntry entry, User user, CountType countType) {
    personaliseCounts(entry, user.getGroups(), countType);
  }

  public void personaliseCounts(CatalogEntry entry, List<Group> groups, CountType countType) {
    if (groups != null && !groups.isEmpty() && DWQueryConfig.getFilterDocumentsByGroup() &&
            DWQueryConfig.getDoCatalogCountPersonalisation()) {
      long count = queryCount(entry, groups, countType);
      switch (countType) {
        case absolute:
          entry.setCountAbsolute(count);
          break;
        case distinctCaseID:
          entry.setCountDistinctCaseID(count);
          break;
        case distinctPID:
          entry.setCountDistinctPID(count);
          break;
      }
    }
  }

  private long queryCount(CatalogEntry entry, List<Group> groups, CountType countType) {
    try {
      long count = 0;
      String sorlFieldName = SolrUtil.getSolrID(entry);
      String queryString = "containing_fields:" + sorlFieldName;
      if (countType == CountType.distinctPID) {
        queryString += " AND string_doc_type:patient";
      } else if (countType == CountType.distinctCaseID) {
        queryString += " AND string_doc_type:case";
      } else {
        queryString += " AND string_doc_type:case";
      }
      count = SolrUtil.getNumFound(solrManager.getServer(), false, queryString,
              groups);
      return count;
    } catch (SolrServerException | IOException e) {
      logger.error("Could not get count for " + entry + " and " + groups + " and groups " + groups,
              e);
      return entry.getCount(countType);
    }
  }

  public void personaliseCountsForTree(CatalogEntry tree, User user, CountType countType) {
    personaliseCounts(tree, user, countType);
    tree.getChildren().forEach(n -> personaliseCountsForTree(n, user, countType));
  }

  @Override
  public void reinitialize() throws DataSourceException {
  }

}
