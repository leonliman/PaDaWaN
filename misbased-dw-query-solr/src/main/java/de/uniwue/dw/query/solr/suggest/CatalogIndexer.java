package de.uniwue.dw.query.solr.suggest;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.client.authentication.group.AuthManager;
import de.uniwue.dw.core.client.authentication.group.Group;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.model.manager.CompleteCatalogClientManager;
import de.uniwue.dw.solr.api.DWSolrConfig;

public class CatalogIndexer {

  private static final String CATALOG_ENTRY_ID_PREFIX = "ce";

  private static final int COMMIT_AFTER_ENTRIES = 1000;

  public static final String FIELD_CATALOG_ATTRIBUTE_ID = "id";

  public static final String FIELD_CATALOG_NAME = "text_catalog_name";

  public static final String FIELD_CATALOG_NAME_AS_STRING = "string_catalog_name";

  public static final String FIELD_CATALOG_COUNT_DOCUMENT = "sv_int_catalog_count_document";

  public static final String FIELD_CATALOG_COUNT_DOCUMENT_GROUP = "sv_int_catalog_count_document_group";

  public static final String FIELD_CATALOG_COUNT_INFO = "sv_int_catalog_count_document_info";

  public static final String FIELD_CATALOG_USER_GROUPS = "string_catalog_user_groups";

  public static final String FIELD_CATALOG_DATA_TYPE = "string_catalog_data_type";

  public static final String FIELD_CATALOG_EXT_ID = "string_catalog_ext_id";

  public static final String FIELD_CATALOG_PROJECT = "string_catalog_project";

  public static final String FIELD_CATALOG_UNIQUE_NAME = "string_catalog_unique_name";

  public static final String FIELD_CATALOG_ALIASES = "string_catalog_aliases";

  public static final String FIELD_CATALOG_PARENT_ID = "int_catalog_parent_id";

  public static final String FIELD_CATALOG_ORDER_VALUE = "sv_float_catalog_oder_value";

  public static final String FIELD_CATALOG_DESCRIPTION = "text_catalog_description";

  public static final String FIELD_CATALOG_CREATION_TIME = "date_catalog_creatione_time";

  public static final String FIELD_TYPE = "string_field_type";

  public static final String TYPE_CATALOG_ENTRY = "catalog_entry";

  public static final String FIELD_CATALOG_LOW_BOUND = "float_entry_low_bound";

  public static final String FIELD_CATALOG_HIGH_BOUND = "float_entry_high_bound";;

  private static Logger logger = LogManager.getLogger(CatalogIndexer.class);

  private CatalogManager catalogManager;

  private CompleteCatalogClientManager catalogClientManager;

  private int numberIndexed = 0;

  public CatalogIndexer(CatalogManager catalogManager, AuthManager groupManager)
          throws SQLException {
    this.catalogManager = catalogManager;
    catalogClientManager = new CompleteCatalogClientManager(catalogManager, groupManager);
  }

  public void indexAllCatalogEntries() throws SQLException, SolrServerException, IOException {
    for (CatalogEntry entry : catalogManager.getEntries()) {
      indexCatalogEntry(entry);
    }
    DWSolrConfig.getInstance().getSolrManager().getServer().commit();
    logger.info("All catalog entries (" + numberIndexed + ") indexed");
  }

  public void indexCatalogEntry(CatalogEntry entry) throws SolrServerException, IOException {
    if (entry.getAttrId() != 0) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(FIELD_TYPE, TYPE_CATALOG_ENTRY);
      doc.addField(FIELD_CATALOG_ATTRIBUTE_ID, catalogEntryID2SolrDocumentID(entry.getAttrId()));
      doc.addField(FIELD_CATALOG_NAME, entry.getName());
      doc.addField(FIELD_CATALOG_NAME_AS_STRING, entry.getName().toLowerCase());
      doc.addField(FIELD_CATALOG_DATA_TYPE, entry.getDataType().toString());
      doc.addField(FIELD_CATALOG_EXT_ID, entry.getExtID());
      doc.addField(FIELD_CATALOG_PROJECT, entry.getProject());
      doc.addField(FIELD_CATALOG_COUNT_DOCUMENT, entry.getCountDistinctCaseID());
      doc.addField(FIELD_CATALOG_COUNT_DOCUMENT_GROUP, entry.getCountDistinctPID());
      doc.addField(FIELD_CATALOG_COUNT_INFO, entry.getCountAbsolute());
      doc.addField(FIELD_CATALOG_UNIQUE_NAME, entry.getUniqueName());
      doc.addField(FIELD_CATALOG_PARENT_ID, entry.getParentID());
      doc.addField(FIELD_CATALOG_ORDER_VALUE, entry.getOrderValue());
      doc.addField(FIELD_CATALOG_DESCRIPTION, entry.getDescription());
      doc.addField(FIELD_CATALOG_CREATION_TIME, entry.getCreationTime());
      doc.addField(FIELD_CATALOG_LOW_BOUND, entry.getLowBound());
      doc.addField(FIELD_CATALOG_HIGH_BOUND, entry.getHighBound());

      doc.addField(FIELD_CATALOG_ALIASES, generateAliases(entry.getName(), entry.getDescription(),
              entry.getUniqueName(), entry.getExtID(), entry.getProject()));

      if (isDiagnosis(entry))
        doc.addField(FIELD_CATALOG_NAME_AS_STRING, getICDCode(entry.getName()));
      for (String group : getUserGroups(entry)) {
        doc.addField(FIELD_CATALOG_USER_GROUPS, group);
      }
      DWSolrConfig.getInstance().getSolrManager().getServer().add(doc);
      if (++numberIndexed % COMMIT_AFTER_ENTRIES == 0) {
        DWSolrConfig.getInstance().getSolrManager().getServer().commit();
        logger.info("Indexed " + numberIndexed + " catalog entries");
      }
    }
  }

  private List<String> generateAliases(String name, String description, String uniqueName,
          String extId, String project) {
    List<String> values = new ArrayList<String>();
    values.addAll(aliaseTokenizer(name));
    values.addAll(aliaseTokenizer(description));
    values.addAll(aliaseTokenizer(uniqueName));
    values.add(extId.toLowerCase());
    values.add(project.toLowerCase());
    return values;
  }

  public static List<String> aliaseTokenizer(String token) {
    List<String> values = new ArrayList<String>();
    if (token != null) {
      token = token.toLowerCase();
      StringTokenizer st = new StringTokenizer(token, " -_\t\n\f\r");
      while (st.hasMoreTokens()) {
        values.add(st.nextToken());
      }
    }
    return values;
  }

  public static int solrDocumentID2CatalogEntryID(String solrId) {
    if (solrId.contains(CATALOG_ENTRY_ID_PREFIX))
      solrId = solrId.substring(CATALOG_ENTRY_ID_PREFIX.length());
    return Integer.valueOf(solrId);
  }

  public static String catalogEntryID2SolrDocumentID(Integer catalogEntry) {
    if (catalogEntry == null)
      return null;
    return CATALOG_ENTRY_ID_PREFIX + catalogEntry;
  }

  private static String getICDCode(String name) {
    int index = name.indexOf(":");
    if (index > 0) {
      name = name.substring(0, index).trim();
    }
    return name;
  }

  private static boolean isDiagnosis(CatalogEntry entry) {
    if (entry.getProject() != null) {
      return entry.getProject().equals("Diagnose");
    } else {
      return false;
    }
  }

  public List<String> getUserGroups(CatalogEntry entry) {
    Optional<Set<Group>> groupsOp = catalogClientManager.getEntitledGroupsForCatalogEntry(entry);
    Set<Group> groups = groupsOp.orElse(new HashSet<>());
    return groups.stream().map(Group::getName).collect(Collectors.toList());
  }

  public static String getUserFilterStringForQuery(User user) {
    String group = getUserGroupsAsValueString(user);
    String query = FIELD_CATALOG_USER_GROUPS + ":" + group;
    return query;
  }

  public static String getGroupsAsValueString(List<Group> groups) {
    return getGroupsAsValueString(groups, ",");
  }

  public static String getUserGroupsAsValueString(User user) {
    if (user == null)
      return null;
    return getGroupsAsValueString(user.getGroups());
  }

  public static List<String> getUserGroupsAsStringList(User user) {
    if (user == null)
      return null;
    return user.getGroups().stream().map(Group::getName).collect(Collectors.toList());
  }

  public static String getGroupsAsValueString(List<Group> groups, String delimiter) {
    return groups.stream().map(Group::getName).collect(Collectors.joining(" " + delimiter + " "));

  }

}
