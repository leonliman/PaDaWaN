package de.uniwue.dw.query.solr.preprocess;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.data.InfoGroup;
import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.query.model.DWQueryConfig;
import de.uniwue.dw.query.solr.SolrUtil;
import de.uniwue.dw.query.solr.client.ISolrConfigKeys;
import de.uniwue.dw.query.solr.client.ISolrConstants;
import de.uniwue.dw.solr.api.DWSolrConfig;
import de.uniwue.dw.text.context.ContextAnnotator;
import de.uniwue.dw.text.context.ContextResult;
import de.uniwue.misc.util.TimeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.resource.ResourceConfigurationException;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.InvalidXMLException;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class NestedDocumentCreator implements ISolrConstants {

  private static final Logger logger = LogManager.getLogger(NestedDocumentCreator.class);

  private static final Pattern largeBlanks = Pattern.compile("\\s{1000,}");

  private final CatalogManager catalogManager;

  private final CatalogEntry caseIdEntry;

  private final CatalogEntry pidEntry;

  public NestedDocumentCreator(CatalogManager catalogManager) throws SQLException {
    this.catalogManager = catalogManager;

    caseIdEntry = getCaseIDCatalogEntry();
    pidEntry = getPIDCatalogEntry();
  }

  public static String infoID2SolrDocumentID(String identID, DOC_TYPE docType) {
    if (identID == null)
      return null;
    return INFO_GROUP_PREFIX + identID + docType.prefix;
  }

  private CatalogEntry getCaseIDCatalogEntry() throws SQLException {
    CatalogEntry caseID = catalogManager.getEntryByRefID(
            DwClientConfiguration.getInstance().getDocumentExtID(),
            DwClientConfiguration.getInstance().getDocumentProject(), false);
    return caseID;
  }

  private CatalogEntry getPIDCatalogEntry() throws SQLException {
    CatalogEntry caseID = catalogManager.getEntryByRefID(
            DwClientConfiguration.getInstance().getDocumentGroupExtID(),
            DwClientConfiguration.getInstance().getDocumentGroupProject(), false);
    return caseID;
  }

  public void addInfoGroups(SolrInputDocument parentDoc, SolrInputDocument doc, DOC_TYPE docType, Long pid, Long caseID,
          List<InfoGroup> infoGroups, Map<Integer, Information[]> attrID2extremeValues) throws SQLException {
    ArrayList<SolrInputDocument> infoGroupDocuments = new ArrayList<>();
    for (InfoGroup infoGroup : infoGroups) {
      infoGroupDocuments.add(
              buildInfoGroupDocument(parentDoc, doc, docType, pid, caseID, infoGroup, attrID2extremeValues));
    }
    if (DWQueryConfig.doIncrementalUpdate()) {
      HashMap<String, Object> fieldModifier = new HashMap<>();
      fieldModifier.put("add", infoGroupDocuments);
      doc.addField("infoGroups", fieldModifier);

      Collection<Object> existenceFieldValues = doc.getFieldValues(FIELD_EXISTANCE_FIELD);
      doc.removeField(FIELD_EXISTANCE_FIELD);
      HashMap<String, Object> fieldModifier2 = new HashMap<>();
      fieldModifier2.put("add", existenceFieldValues);
      doc.addField(FIELD_EXISTANCE_FIELD, fieldModifier2);
    } else {
      doc.addField("infoGroups", infoGroupDocuments);
    }
  }

  public Map<Integer, Information[]> computeExtremeValues(List<InfoGroup> infoGroups)
          throws SQLException {
    List<Information> infos = infoGroups.stream().flatMap(n -> n.getAllContainingInfos().stream())
            .collect(Collectors.toList());
    Map<Integer, List<Information>> attrID2Infos = infos.stream()
            .collect(Collectors.groupingBy(Information::getAttrID, Collectors.toList()));
    Map<Integer, Information[]> attrID2extremeValues = new HashMap<>();
    for (Entry<Integer, List<Information>> entry : attrID2Infos.entrySet()) {
      Information first = null;
      Information last = null;
      Information min = null;
      Information max = null;
      for (Information info : entry.getValue()) {
        CatalogEntry catalogEntry = catalogManager.getEntryByID(info.getAttrID());
        if (first == null || info.getMeasureTime().before(first.getMeasureTime()))
          first = info;
        if (last == null || info.getMeasureTime().after(last.getMeasureTime()))
          last = info;
        if (catalogEntry != null && catalogEntry.getDataType() == CatalogEntryType.Number) {
          if (min == null ||
                  (info.getValueDec() != null && min.getValueDec() != null && info.getValueDec() < min.getValueDec()))
            min = info;
          if (max == null ||
                  (info.getValueDec() != null && max.getValueDec() != null && info.getValueDec() > max.getValueDec()))
            max = info;
        }
      }
      Information[] firstLastMinMax = { first, last, min, max };
      attrID2extremeValues.put(entry.getKey(), firstLastMinMax);
    }
    return attrID2extremeValues;
  }

  public SolrInputDocument buildInfoGroupDocument(SolrInputDocument parentDoc, SolrInputDocument doc, DOC_TYPE docType,
          Long pid, Long caseID, InfoGroup infoGroup, Map<Integer, Information[]> attrID2extremeValues)
          throws SQLException {
    SolrInputDocument groupDoc = new SolrInputDocument();
    String id;
    if (infoGroup.getCompoundID().equals(Information.COMPOUNDID_DEFAULT_VALUE)) {
      id = UUID.randomUUID().toString();
    } else {
      id = infoID2SolrDocumentID(infoGroup.getCompoundID(), docType);
    }
    groupDoc.setField(SOLR_FIELD_ID, id);
    groupDoc.addField(SOLR_FIELD_PARENT_CHILD_LAYER, CHILD_LAYER);
    groupDoc.addField(SOLR_FIELD_DOC_TYPE, docType.docType);
    groupDoc.addField(SOLR_FIELD_PATIENT_ID, pid);
    groupDoc.addField(SOLR_FIELD_CASE_ID, caseID);
    for (Information info : infoGroup.getAllContainingInfos()) {
      if (info != null) {
        CatalogEntry catalogEntry = catalogManager.getEntryByID(info.getAttrID());
        if (catalogEntry == null) {
          System.err.println("There exist infos to the non existing catalog entry with attr-id: "
                  + info.getAttrID());
        } else {
          if (!catalogEntry.isRoot()) {
            addInfo(parentDoc, doc, groupDoc, info, catalogEntry, attrID2extremeValues);
          }
        }
      }
    }
    return groupDoc;
  }

  private boolean isValidId(String id) {
    return !(id == null || id.isEmpty() || id.equals("ig0c"));
  }

  private void addInfo(SolrInputDocument parentDoc, SolrInputDocument doc, SolrInputDocument groupDoc, Information info,
          CatalogEntry catalogEntry, Map<Integer, Information[]> attrID2extremeValues)
          throws SQLException {

    if (parentDoc != null) {
      addFieldExistence(parentDoc, catalogEntry);
      addFieldExistenceOfAncestors(parentDoc, catalogEntry);
    }

    addFieldExistence(doc, catalogEntry);
    addFieldExistenceOfAncestors(doc, catalogEntry);

    addFieldExistence(groupDoc, catalogEntry);
    addFieldExistenceOfAncestors(groupDoc, catalogEntry);

    String solrFieldName = SolrUtil.getSolrFieldName(catalogEntry);
    String measuretimeString = TimeUtil.getDateFormat5().format(info.getMeasureTime());
    groupDoc.setField(SOLR_FIELD_MEASURE_TIME, measuretimeString);
//    groupDoc.setField(SOLR_FIELD_REF_ID, info.getRef());
    groupDoc.setField(SOLR_FIELD_DOC_ID, info.getDocID());
    addValue(groupDoc, solrFieldName, info, attrID2extremeValues);
    groupDoc.setField(SOLR_FIELD_CASE_ID, info.getCaseID());
    // doc.addChildDocument(groupDoc);
  }

  private void addValue(SolrInputDocument infoDoc, String solrFieldName, Information info,
          Map<Integer, Information[]> attrID2extremeValues) throws SQLException {
    CatalogEntry entry = catalogManager.getEntryByID(info.getAttrID());
    if (entry.getDataType() == CatalogEntryType.Number) {
      addField(infoDoc, solrFieldName, info.getValueDec());
    } else if (entry.getDataType() == CatalogEntryType.DateTime) {
      addField(infoDoc, solrFieldName, formatDate(info.getValue()));
    } else if (entry.getDataType() == CatalogEntryType.Text) {
      // if (isAnID(entry) && doc.containsKey(solrFieldName)) {
      // return;
      // }
      addField(infoDoc, solrFieldName, info.getValue());
      if (info.getValue() != null && !info.getValue().isEmpty() && !isAnID(entry)) {
        if (DWSolrConfig.indexTextForRegexQueries()) {
          String stringValue = info.getValue();
          // Index text as string, that regex query works across token boundaries
          if (stringValue.length() > STRING_FIELD_MAX_LENGTH) {
            stringValue = stringValue.substring(0, STRING_FIELD_MAX_LENGTH);
          }
          String stringField = "string" + solrFieldName.substring(4);
          addField(infoDoc, stringField, stringValue);
        }
      }
      if (DwClientConfiguration.getInstance().getBooleanParameter(ISolrConfigKeys.PARAM_INDEX_NEGEX,
              true)) {
        addNegexInstances(infoDoc, solrFieldName, info, entry);
      }
    } else if (entry.getDataType() == CatalogEntryType.SingleChoice) {
      addField(infoDoc, solrFieldName, info.getValue());
    } else if (entry.getDataType() == CatalogEntryType.Bool
            || entry.getDataType() == CatalogEntryType.Structure) {
      solrFieldName = SOLR_FIELD_BOOLEAN_STRUCTURE_VALUE;
      // Structure-Entries should not have data at all ! This should be already checked during
      // import !
      // String stringValue = info.getValue();
      String stringValue = entry.getName();
      if ((stringValue != null) && (stringValue.length() > STRING_FIELD_MAX_LENGTH)) {
        stringValue = stringValue.substring(0, STRING_FIELD_MAX_LENGTH);
      }
      if (stringValue == null) {
        stringValue = "x";
      }
      addField(infoDoc, solrFieldName, stringValue);
    } else {
      throw new SQLException("unknown data-type: " + entry.getDataType());
    }
    handleExtremeValues(infoDoc, info, attrID2extremeValues);
    // if (!isExtremeValue) {
    // addAdditionalFactData(entry, info);
    // }
  }

  private void handleExtremeValues(SolrInputDocument infoDoc, Information info,
          Map<Integer, Information[]> attrID2extremeValues) throws SQLException {
    Information[] firstLastMinMax = attrID2extremeValues.get(info.getAttrID());
    CatalogEntry catalogEntry = catalogManager.getEntryByID(info.getAttrID());
    String catalogEntrySolrID = SolrUtil.getSolrID(catalogEntry);
    // if (info.equals(firstLastMinMax[0]))
    // infoDoc.addField(SOLR_FIELD_EXTREME_VALUE, FIRST);
    // if (info.equals(firstLastMinMax[1]))
    // infoDoc.addField(SOLR_FIELD_EXTREME_VALUE, LAST);
    // if (info.equals(firstLastMinMax[2]))
    // infoDoc.addField(SOLR_FIELD_EXTREME_VALUE, MIN);
    // if (info.equals(firstLastMinMax[3]))
    // infoDoc.addField(SOLR_FIELD_EXTREME_VALUE, MAX);

    if (info.equals(firstLastMinMax[0])) {
      infoDoc.addField(SOLR_FIELD_FIRST_VALUE, catalogEntrySolrID);
      addFieldFirstLastMinMaxForAncestors(infoDoc, catalogEntry, SOLR_FIELD_FIRST_VALUE);
    }
    if (info.equals(firstLastMinMax[1])) {
      infoDoc.addField(SOLR_FIELD_LAST_VALUE, catalogEntrySolrID);
      addFieldFirstLastMinMaxForAncestors(infoDoc, catalogEntry, SOLR_FIELD_LAST_VALUE);
    }
    if (info.equals(firstLastMinMax[2])) {
      infoDoc.addField(SOLR_FIELD_MIN_VALUE, catalogEntrySolrID);
      addFieldFirstLastMinMaxForAncestors(infoDoc, catalogEntry, SOLR_FIELD_MIN_VALUE);
    }
    if (info.equals(firstLastMinMax[3])) {
      infoDoc.addField(SOLR_FIELD_MAX_VALUE, catalogEntrySolrID);
      addFieldFirstLastMinMaxForAncestors(infoDoc, catalogEntry, SOLR_FIELD_MAX_VALUE);
    }

  }

  private void addFieldFirstLastMinMaxForAncestors(SolrInputDocument solrDoc, CatalogEntry catalogEntry,
          String fieldName) {
    for (CatalogEntry ancestor : catalogEntry.getAncestors()) {
      if (!ancestor.isRoot()) {
        String solrFieldName = SolrUtil.getSolrID(ancestor);
        solrDoc.addField(fieldName, solrFieldName);
      }
    }
  }

  private String formatDate(String value) {
    return SolrUtil.format2SolrDate(value);
  }

  private boolean isAnID(CatalogEntry entry) {
    if (caseIdEntry != null)
      if (caseIdEntry.getAttrId() == entry.getAttrId())
        return true;
    if (pidEntry != null)
      return pidEntry.getAttrId() == entry.getAttrId();
    return false;
  }

  private void addFieldExistenceOfAncestors(SolrInputDocument infoDoc, CatalogEntry catalogEntry) {
    for (CatalogEntry ancestor : catalogEntry.getAncestors()) {
      addFieldExistence(infoDoc, ancestor);
    }
  }

  private void addFieldExistence(SolrInputDocument solrDoc, CatalogEntry catalogEntry) {
    if (!catalogEntry.isRoot()) {
      String solrFieldName = SolrUtil.getSolrID(catalogEntry);
      addField(solrDoc, FIELD_EXISTANCE_FIELD, solrFieldName);
    }
  }

  private void addField(SolrInputDocument infoDoc, String name, Object value) {
    name = SolrUtil.formatFieldName(name);
    if (value instanceof String) {
      // value = ((String) value).replaceAll("\\s+", " ");
    }
    infoDoc.addField(name, value);
  }

  private void addNegexInstances(SolrInputDocument infoDoc, String solrFieldName, Information info,
          CatalogEntry entry) {
    String solrFieldNameAffiremdSements = SolrUtil.getSolrFieldNameForPositiveTextSegments(entry);
    String solrFieldNameAffiremdRecentSegments = SolrUtil
            .getSolrFieldNameForPositiveTextSegments(entry);

    try {
      if (info.getValue() == null)
        return;
      String value = info.getValue();
      Matcher matcher = largeBlanks.matcher(value);
      if (matcher.find()) {
        int preLength = value.length();
        value = matcher.replaceAll(" ");
        int numRemovedWhitespaces = preLength - value.length();
        logger.warn(numRemovedWhitespaces + " whitespaces have been removed from the information with id " +
                info.getInfoID() + " before running NegEx");
      }
      ContextResult analyzedText = ContextAnnotator.annotate(value);
      List<String> affirmedPatientSegments = analyzedText.getAffirmedPatientSegments();
      List<String> affirmedRecentPatientSegments = analyzedText.getAffirmedRecentPatientSegments();

      for (String segment : affirmedPatientSegments) {
        addField(infoDoc, solrFieldNameAffiremdSements, segment);
      }

      for (String segment : affirmedRecentPatientSegments) {
        addField(infoDoc, solrFieldNameAffiremdRecentSegments, segment);
      }

    } catch (AnalysisEngineProcessException | InvalidXMLException | ResourceInitializationException
            | ResourceConfigurationException | IOException | URISyntaxException | SAXException e) {
      e.printStackTrace();
    }

  }

  // private List<String> getPostiveSegmentsWithContext(String text) {
  // try {
  // return ContextAnnotator.getAffirmedRecentPatientSegments(text);
  // } catch (AnalysisEngineProcessException | InvalidXMLException | ResourceInitializationException
  // | ResourceConfigurationException | IOException | URISyntaxException | SAXException e) {
  // e.printStackTrace();
  // }
  // return new ArrayList<>();
  // }

  // private List<String> getPostiveSegments(String text) {
  // AnalysedText analysedText = new TextIndexerHelper().getNegationsInText(text);
  // List<String> postiveSegments = analysedText.getNotNegatedSegments().stream()
  // .map(n -> n.getText()).collect(Collectors.toList());
  // return postiveSegments;
  // }
}
