package de.uniwue.dw.query.solr.preprocess.model;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.solr.common.SolrInputDocument;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.query.model.index.tree.INodeVisitor;
import de.uniwue.dw.query.model.index.tree.Node;
import de.uniwue.dw.query.model.result.Cell.CellType;
//import de.uniwue.dw3.solr.preprocess.util.SolrUtil;
import de.uniwue.dw.query.solr.SolrUtil;
import de.uniwue.dw.query.solr.client.ISolrConstants;
import de.uniwue.dw.query.solr.preprocess.util.AnalysedText;
import de.uniwue.dw.query.solr.preprocess.util.Segment;
import de.uniwue.dw.query.solr.preprocess.util.TextIndexerHelper;
import de.uniwue.dw.solr.api.DWSolrConfig;
import de.uniwue.misc.util.TimeUtil;

/**
 * The IndexVisitor visits a tree and stores all nodes in the Solr index.
 * 
 * @author e_dietrich_g
 *
 */
public class IndexVisitor implements INodeVisitor, ISolrConstants {

  public static final String MIN = "min";

  public static final String MAX = "max";

  public static final String FIRST = "first";

  public static final String LAST = "last";

  public static final String LEAF_COUNT = "leaf_count";

  public static final String LEAF_TEXT = "leaf_text";

  public static final String MAX_SON_COUNT = "max_son_count";

  public static final String SONS_TEXT = "sons_text";

  public static final String SONS_COUNT = "sons_count";

  public static final int STRING_FIELD_MAX_LENGTH = 30000;

  private static SimpleDateFormat sqlDateFormat1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

  private static SimpleDateFormat sqlDateFormat2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.0");

  private static SimpleDateFormat solrDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");

  private CatalogManager catalogManager;

  private SolrInputDocument doc;

  private CatalogEntry caseIdEntry;

  private CatalogEntry pidEntry;

  public IndexVisitor(CatalogManager catalogManager, SolrInputDocument doc) throws SQLException {
    this.catalogManager = catalogManager;
    this.doc = doc;
    caseIdEntry = getCaseIDCatalogEntry();
    pidEntry = getPIDCatalogEntry();
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

  @Override
  public void visit(Node n) {
    try {
      // if (n.getCatalogEntry().getAttrId() == 51106)
      // System.out.println();
      index(n);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private void index(Node n) throws SQLException {
    if (!n.getCatalogEntry().isRoot()) {
      indexInfos(n);
      indexFieldExistance(n);
      indexExtremeValue(n);
      indexHierarchyInformation(n);
    }
  }

  private boolean isAnID(CatalogEntry entry) {
    if (caseIdEntry != null)
      if (caseIdEntry.getAttrId() == entry.getAttrId())
        return true;
    if (pidEntry != null)
      if (pidEntry.getAttrId() == entry.getAttrId())
        return true;
    return false;
  }

  private void indexFieldExistance(Node n) {
    indexFieldExistance(n.getCatalogEntry());
  }

  private void indexFieldExistance(CatalogEntry catalogEntry) {
    String sorlFieldName = SolrUtil.getSolrID(catalogEntry);
    addField(FIELD_EXISTANCE_FIELD, sorlFieldName);
  }

  private void indexHierarchyInformation(Node n) {
    String name = SolrUtil.getSolrID(n.getCatalogEntry());

    String leafCountName = "int_" + name + "_" + LEAF_COUNT;
    String leafsTextName = "string_" + name + "_" + LEAF_TEXT;
    String maxSonCountName = "int_" + name + "_" + MAX_SON_COUNT;
    String sonsTextName = "text_" + name + "_" + SONS_TEXT;
    String sonsCountName = "int_" + name + "_" + SONS_COUNT;

    // addField(leafCountName, n.getLeafCount());
    String leafsText = n.getLeafsText();
    if (leafsText.length() > STRING_FIELD_MAX_LENGTH) {
      leafsText = leafsText.substring(0, STRING_FIELD_MAX_LENGTH);
    }
    addField(leafsTextName, leafsText);
    // addField(maxSonCountName, n.getMaxSonCount());
    // addField(sonsTextName, n.getSonsText());
    // addField(sonsCountName, n.getSons().size());
  }

  private void indexExtremeValue(Node n) throws SQLException {
    // if (!(n.getCatalogEntry().dataType.equals(CatalogEntryType.Bool) ||
    // n.getCatalogEntry().dataType
    // .equals(CatalogEntryType.Text))) {
    if (n.getCatalogEntry().getDataType() == CatalogEntryType.Number) {
      String sorlFieldName = SolrUtil.getSolrFieldName(n.getCatalogEntry());

      List<Information> infos = n.getInstances();
      Information first = null;
      Information last = null;
      Information min = null;
      Information max = null;

      for (Information info : infos) {
        if (first == null || info.getMeasureTime().before(first.getMeasureTime()))
          first = info;
        if (last == null || info.getMeasureTime().after(last.getMeasureTime()))
          last = info;
        if (n.getCatalogEntry().getDataType() == CatalogEntryType.Number) {
          if (min == null || info.getValueDec() < min.getValueDec())
            min = info;
          if (max == null || info.getValueDec() > max.getValueDec())
            max = info;
        }
      }
      addField(sorlFieldName + "_" + FIRST, first, true);
      addField(sorlFieldName + "_" + LAST, last, true);
      addField(sorlFieldName + "_" + MIN, min, true);
      addField(sorlFieldName + "_" + MAX, max, true);
    }

  }

  public void indexIfNotNull(String solrFieldName, String extention, Information info)
          throws SQLException {
    if (info != null) {
      solrFieldName += extention;
      solrFieldName = SolrUtil.formatFieldName(solrFieldName);
      CatalogEntry entry = catalogManager.getEntryByID(info.getAttrID());
      if (entry.getDataType() == CatalogEntryType.Number)
        addField(solrFieldName, info.getValueDec());
      else
        addField(solrFieldName, info.getValue());
    }
  }

  private void addField(String solrFieldName, Information info, boolean isExtremeValue)
          throws SQLException {
    if (info != null) {
      CatalogEntry entry = catalogManager.getEntryByID(info.getAttrID());
      if (entry.getDataType() == CatalogEntryType.Number) {
        addField(solrFieldName, info.getValueDec());
      } else if (entry.getDataType() == CatalogEntryType.DateTime) {
        addField(solrFieldName, formatDate(info.getValue()));
      } else if (entry.getDataType() == CatalogEntryType.Text) {
        if (isAnID(entry) && doc.containsKey(solrFieldName)) {
          return;
        }
        addField(solrFieldName, info.getValue());
        if (info.getValue() != null && !info.getValue().isEmpty() && !isAnID(entry)) {
          if (DWSolrConfig.indexTextForRegexQueries()) {
            String stringValue = info.getValue();
            // Index text as string, that regex query works across token boundaries
            if (stringValue.length() > STRING_FIELD_MAX_LENGTH) {
              stringValue = stringValue.substring(0, STRING_FIELD_MAX_LENGTH);
            }
            String stringField = "string" + solrFieldName.substring(4);
            addField(stringField, stringValue);
          }
        }
        addNegexInstances(solrFieldName, info, entry);
      } else if ((entry.getDataType() == CatalogEntryType.Bool)
              || (entry.getDataType() == CatalogEntryType.Structure)) {
        // Structure-Entries should not have data at all ! This should be already checked during
        // import !
        String stringValue = info.getValue();
        if ((stringValue != null) && (stringValue.length() > STRING_FIELD_MAX_LENGTH)) {
          stringValue = stringValue.substring(0, STRING_FIELD_MAX_LENGTH);
        }
        if (stringValue == null) {
          stringValue = "x";
        }
        addField(solrFieldName, stringValue);
      } else
        throw new SQLException("unknown type");
      if (!isExtremeValue) {
        addAdditionalFactData(entry, info);
      }
    }
  }

  private void addAdditionalFactData(CatalogEntry entry, Information info) {
    String measureTimeSolrFieldName = SolrUtil.getSolrFieldName(entry, CellType.MeasureTime);
    String measuretimeString = TimeUtil.getDateFormat5().format(info.getMeasureTime());
    addField(measureTimeSolrFieldName, measuretimeString);
    String docIDSolrFieldName = SolrUtil.getSolrFieldName(entry, CellType.DocID);
    addField(docIDSolrFieldName, info.getDocID());
  }

  private void addNegexInstances(String solrFieldName, Information info, CatalogEntry entry) {
//    List<List<String>> negInText = TextIndexerHelper.getNegInText(info.getValue());
     AnalysedText text =new TextIndexerHelper().getNegationsInText(info.getValue());
    solrFieldName =SolrUtil. getSolrFieldNameForPositiveTextSegments(entry);
    for (Segment segment : text.getNotNegatedSegments()) {
      addField(solrFieldName, segment.getText());
    }
    // solrFieldName = "text_" + entry.project + "_" + entry.extID + "_negative";
    // for (String segment : negInText.get(1)) {
    // addField(solrFieldName, segment);
    // }
  }



  private void indexInfo(Information info) throws SQLException {
    String solrFieldName = getSolrName(info);
    addField(solrFieldName, info, false);
  }

  private void indexInfos(Node n) throws SQLException {
    for (Information info : n.getInstances())
      indexInfo(info);
  }

  private String getSolrName(Information info) throws SQLException {
    CatalogEntry entry = catalogManager.getEntryByID(info.getAttrID());
    return getSolrName(entry);
  }

  private String getSolrName(CatalogEntry entry) throws SQLException {
    String sorlFieldName = SolrUtil.getSolrFieldName(entry);
    return sorlFieldName;
  }

  private String formatDate(String value) {
    return SolrUtil.format2SolrDate(value);
  }

  private void addField(String name, Object value) {
    name = SolrUtil.formatFieldName(name);
    if (value instanceof String) {
      // value = ((String) value).replaceAll("\\s+", " ");
    }
    doc.addField(name, value);
  }

}
