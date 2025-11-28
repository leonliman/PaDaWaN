package de.uniwue.dw.query.model;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.manager.DataSourceException;
import de.uniwue.dw.core.model.manager.ICatalogClientManager;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.*;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;
import de.uniwue.misc.util.TimeUtil;
import de.uniwue.misc.util.XMLUtil;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

public class QueryReader {

  private final ICatalogClientManager catalogClientManager;

  public QueryReader(ICatalogClientManager catalogClientManager) {
    this.catalogClientManager = catalogClientManager;
  }

  public static QueryRoot read(ICatalogClientManager catalogClientManager, String xmlQuery)
          throws QueryException {
    QueryReader reader = new QueryReader(catalogClientManager);
    return reader.readQuery(xmlQuery);
  }

  public static QueryRoot read(String xmlQuery) throws QueryException, SQLException {
    return QueryReader.read(DWQueryConfig.getInstance().getCatalogClientManager(), xmlQuery);
  }

  public static QueryStructureElem readStructureElem(ICatalogClientManager catalogClientManager,
          String xml) throws QueryException {
    QueryReader reader = new QueryReader(catalogClientManager);
    NodeList nodeList;
    try {
      nodeList = XMLUtil.getNodeList(xml);
    } catch (ParserConfigurationException e) {
      throw new QueryException(e);
    } catch (SAXException e) {
      throw new QueryException(e);
    } catch (IOException e) {
      throw new QueryException(e);
    }
    if (nodeList.getLength() != 1) {
      throw new QueryException("Not exactly one element !");
    }
    Node aSubNode = nodeList.item(0);
    QueryStructureElem result = reader.readStructureElem(aSubNode, null);
    return result;
  }

  public QueryRoot readQuery(String xmlQuery) throws QueryException {
    QueryRoot query = null;
    NodeList nodeList;
    try {
      nodeList = XMLUtil.getNodeList(xmlQuery);
    } catch (ParserConfigurationException e) {
      throw new QueryException(e);
    } catch (SAXException e) {
      throw new QueryException(e);
    } catch (IOException e) {
      throw new QueryException(e);
    }
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node aNode = nodeList.item(i);
      if (aNode.getNodeName().equals("Query")) {
        // sonst ist es einfach nur der DOCTYPE-Knoten
        if (aNode.getAttributes() != null) {
          query = readRoot(aNode);
        }
      }
    }
    return query;
  }

  private QueryRoot readRoot(Node rootNode) throws QueryException {
    QueryRoot root = new QueryRoot();

    root.setDisplayPID(XMLUtil.getBoolean(rootNode, "pid", false));
    root.setOnlyCount(XMLUtil.getBoolean(rootNode, "onlyCount", false));
    root.setLimitResult(XMLUtil.getInt(rootNode, "limitResult", QueryRoot.limitResultDefault));
    root.setDistinct(XMLUtil.getBoolean(rootNode, "distinct", false));
    root.setVersion(XMLUtil.getString(rootNode, "version", QueryRoot.currentVersion));
    readStructureContainingElem(rootNode, root);
    // after all attributes have been completely instantiated the
    // relative temporal operators can get their correct referenced attribute instance
    List<QueryAttribute> attributes = root.getAttributesRecursive();
    for (QueryAttribute anAttr : attributes) {
      for (QueryTempOpRel anOp : anAttr.getTemporalOpsRel()) {
        QueryAttribute elem = (QueryAttribute) root.getElem(anOp.getRefElemID());
        anOp.setRefElem(elem);
      }
      for (QueryValueCompare anOp : anAttr.getValueCompares()) {
        anOp.refElem = (QueryAttribute) root.getElem(anOp.refElemID);
      }
    }
    return root;
  }

  private QueryIDFilter readIDFilter(Node aNode, QueryStructureContainingElem parent)
          throws QueryException {
    QueryIDFilter filter = new QueryIDFilter(parent);
    filter.setDistinct(XMLUtil.getBoolean(aNode, "distinct", false));
    String filterIDTypeString = XMLUtil.getString(aNode, "filterIDType");
    // TODO: Abwaerts-Kompatibilitaet !! irgendwann wieder ausnehmen
    if (filterIDTypeString == null) {
      filterIDTypeString = XMLUtil.getString(aNode, "filterIDType");
      if ((filterIDTypeString != null) && (filterIDTypeString.equals("case"))) {
        filterIDTypeString = "CaseID";
      }
    }
    if (filterIDTypeString != null) {
      FilterIDType type = FilterIDType.valueOf(filterIDTypeString);
      filter.setFilterIDType(type);
    }
    NodeList nodeList = aNode.getChildNodes();
    for (int j = 0; j < nodeList.getLength(); j++) {
      Node aSubNode = nodeList.item(j);
      if (aSubNode.getNodeName().equals("ID")) {
        Long pid = XMLUtil.getLong(aSubNode, "value", 0);
        filter.addID(pid);
        String timeString = XMLUtil.getString(aSubNode, "time");
        if (timeString != null) {
          filter.addTimeString(pid, timeString);
        }
      }
    }
    readStructureContainingElem(aNode, filter);
    return filter;
  }

  private QueryNTrue readNTrue(Node aNode, QueryStructureContainingElem parent)
          throws QueryException {
    QueryNTrue nTrue = new QueryNTrue(parent);
    nTrue.n = XMLUtil.getInt(aNode, "n", Integer.MIN_VALUE);
    if (nTrue.n == Integer.MIN_VALUE) {
      throw new QueryException("No n given for NTrue.");
    }
    readStructureContainingElem(aNode, nTrue);
    return nTrue;
  }

  private QueryOr readOr(Node aNode, QueryStructureContainingElem parent) throws QueryException {
    QueryOr or = new QueryOr(parent);
    or.setPowerSet(XMLUtil.getBoolean(aNode, "powerSet", false));
    or.setName(XMLUtil.getString(aNode, "name", null));
    readStructureContainingElem(aNode, or);
    return or;
  }

  private QueryAnd readAnd(Node aNode, QueryStructureContainingElem parent) throws QueryException {
    QueryAnd and = new QueryAnd(parent);
    and.setPowerSet(XMLUtil.getBoolean(aNode, "powerSet", false));
    and.setName(XMLUtil.getString(aNode, "name", null));
    readStructureContainingElem(aNode, and);
    return and;
  }

  private QueryNot readNot(Node aNode, QueryStructureContainingElem parent) throws QueryException {
    QueryNot not = new QueryNot(parent);
    readStructureContainingElem(aNode, not);
    return not;
  }

  private void readStructureElemProperties(Node aNode, QueryStructureElem op)
          throws QueryException {
    op.setComment(XMLUtil.getString(aNode, "comment"));
    op.active = XMLUtil.getBoolean(aNode, "active", true);
    op.setOptional(XMLUtil.getBoolean(aNode, "optional", false));
    op.setFilterUnkown(XMLUtil.getBoolean(aNode, "filterUnknown", true));
    NodeList nodeList = aNode.getChildNodes();
    for (int j = 0; j < nodeList.getLength(); j++) {
      Node aSubNode = nodeList.item(j);
      if (aSubNode.getNodeName().equals("TempOpRel")) {
        QueryTempOpRel tempOp = readTemporalOpRel(aSubNode, op);
        op.getTemporalOpsRel().add(tempOp);
      } else if (aSubNode.getNodeName().equals("TempOpAbs")) {
        QueryTempOpAbs tempOp = readTemporalOpAbs(aSubNode, op);
        op.addTempOpAbs(tempOp);
      }
    }
  }

  private void readStructureContainingElem(Node aNode, QueryStructureContainingElem op)
          throws QueryException {
    NodeList nodeList = aNode.getChildNodes();
    for (int j = 0; j < nodeList.getLength(); j++) {
      Node aSubNode = nodeList.item(j);
      readStructureElem(aSubNode, op);
    }
  }

  private QueryStructureElem readStructureElem(Node aNode, QueryStructureContainingElem aContainer)
          throws QueryException {
    QueryStructureElem anElem = null;
    if (aNode.getNodeName().equals("Attribute")) {
      anElem = readAttribute(aNode, aContainer);
    } else if (aNode.getNodeName().equals("Or")) {
      anElem = readOr(aNode, aContainer);
    } else if (aNode.getNodeName().equals("And")) {
      anElem = readAnd(aNode, aContainer);
    } else if (aNode.getNodeName().equals("NTrue")) {
      anElem = readNTrue(aNode, aContainer);
    } else if (aNode.getNodeName().equals("Not")) {
      anElem = readNot(aNode, aContainer);
    } else if (aNode.getNodeName().equals("Query")) {
      anElem = readRoot(aNode);
    } else if (aNode.getNodeName().equals("IDFilter")) {
      anElem = readIDFilter(aNode, aContainer);
    } else if (aNode.getNodeName().equals("SubQuery")) {
      anElem = readSubQuery(aNode, aContainer);
    } else if (aNode.getNodeName().equals("DistributionColumn")) {
      anElem = readDistributionColumn(aNode, aContainer);
    } else if (aNode.getNodeName().equals("DistributionRow")) {
      anElem = readDistributionRow(aNode, aContainer);
    } else if (aNode.getNodeName().equals("DistributionFilter")) {
      anElem = readDistributionFilter(aNode, aContainer);
    }
    if (anElem != null) {
      readStructureElemProperties(aNode, anElem);
    }
    return anElem;
  }

  private CatalogEntry getCatalogEntry(String refID, String domain) throws QueryException {
    return getCatalogEntry(refID, domain, null);
  }

  private CatalogEntry getCatalogEntry(String refID, String domain, User user)
          throws QueryException {
    CatalogEntry anEntry = null;
    try {
      anEntry = catalogClientManager.getEntryByRefID(refID, domain, user);
    } catch (DataSourceException e) {
      e.printStackTrace();
    }
    if (anEntry == null) {
      String errorMsg = "Attribute with extID '" + refID + "' and domain '" + domain
              + "' does not exist or is not visible for user " + user.getUsername() + ".";
      throw new QueryException(errorMsg);
    }
    return anEntry;
  }

  private QueryStructureElem readAttribute(Node attributeNode, QueryStructureContainingElem parent)
          throws QueryException {
    NodeList nodeList;

    String extID = XMLUtil.getString(attributeNode, "extID");
    String domain = XMLUtil.getString(attributeNode, "domain");
    CatalogEntry anEntry = null;
    if ((extID == null) || (domain == null)) {
      throw new QueryException("ExtID and domain have to be given !");
    }
    anEntry = getCatalogEntry(extID, domain);
    QueryAttribute attribute = new QueryAttribute(parent, anEntry);

    if (attributeNode.getAttributes().getNamedItem("searchSubEntries") != null) {
      boolean searchSubEntries = XMLUtil.getBoolean(attributeNode, "searchSubEntries", false);
      attribute.setSearchSubEntries(searchSubEntries);
    } else {
      attribute.setSearchSubEntries(anEntry.getDataType() == CatalogEntryType.Bool
              || anEntry.getDataType() == CatalogEntryType.SingleChoice);
    }

    String idString = XMLUtil.getString(attributeNode, "elementID");
    if (idString != null) {
      attribute.setId(idString);
    }

    int width = XMLUtil.getInt(attributeNode, "width", 0);
    attribute.setWidth(width);

    String desiredContent = XMLUtil.getString(attributeNode, "desiredContent", "");
    attribute.setDesiredContent(desiredContent);

    String op = XMLUtil.getString(attributeNode, "contentOperator");
    if ((op != null) && !op.isEmpty()) {
      attribute.setContentOperator(ContentOperator.parse(op));
    }

    String extractionMode = XMLUtil.getString(attributeNode, "extractionMode");
    if ((extractionMode != null) && !extractionMode.isEmpty()) {
      attribute.setExtractionMode(ExtractionMode.valueOf(extractionMode));
    }

    String reductionOp = XMLUtil.getString(attributeNode, "reductionOp",
            ReductionOperator.NONE.toString());
    attribute.setReductionOperator(ReductionOperator.parse(reductionOp));

    boolean valueInFile = XMLUtil.getBoolean(attributeNode, "valueInFile", false);
    attribute.setValueInFile(valueInFile);

    boolean infoDate = XMLUtil.getBoolean(attributeNode, "infoDate", false);
    attribute.setDisplayInfoDate(infoDate);

    boolean caseID = XMLUtil.getBoolean(attributeNode, "caseID", false);
    attribute.setDisplayCaseID(caseID);

    boolean docID = XMLUtil.getBoolean(attributeNode, "docID", false);
    attribute.setDisplayDocID(docID);

    attribute.setDisplayValue(XMLUtil.getBoolean(attributeNode, "displayValue", true));

    attribute.setMultipleRows(XMLUtil.getBoolean(attributeNode, "multipleRows", false));

    String displayName = XMLUtil.getString(attributeNode, "displayName");
    attribute.setDisplayName(displayName);

    attribute.setOnlyDisplayExistence(
            XMLUtil.getBoolean(attributeNode, "onlyDisplayExistence", false));

    int minCount = XMLUtil.getInt(attributeNode, "minCount", 0);
    attribute.setMinCount(minCount);

    int maxCount = XMLUtil.getInt(attributeNode, "maxCount", Integer.MAX_VALUE);
    attribute.setWidth(maxCount);

    String countTypeString = XMLUtil.getString(attributeNode, "countType");
    if (countTypeString != null) {
      FilterIDType countType = FilterIDType.valueOf(countTypeString);
      attribute.setCountType(countType);
    }

    nodeList = attributeNode.getChildNodes();
    for (int j = 0; j < nodeList.getLength(); j++) {
      Node aNode = nodeList.item(j);
      if (aNode.getNodeName().equals("HeaderFormula")) {
      } else if (aNode.getNodeName().equals("ListSelector")) {
      } else if (aNode.getNodeName().equals("ValueCompare")) {
        QueryValueCompare compare = readValueCompare(aNode, attribute);
        attribute.getValueCompares().add(compare);
      }
    }
    return attribute;
  }

  private QueryValueCompare readValueCompare(Node valueCompareNode, QueryElem parent)
          throws QueryException {
    QueryValueCompare valueCompare = new QueryValueCompare(parent);
    valueCompare.refElemID = XMLUtil.getString(valueCompareNode, "refElemID");
    String op = XMLUtil.getString(valueCompareNode, "contentOperator");
    if ((op != null) && !op.isEmpty()) {
      valueCompare.setContentOperator(ContentOperator.parse(op));
    } else {
      String errorMsg = "ValueCompare requires a contentOperator being set";
      throw new QueryException(errorMsg);
    }
    return valueCompare;
  }

  private QueryTempOpRel readTemporalOpRel(Node temporalOpNode, QueryElem parent)
          throws QueryException {
    QueryTempOpRel tempOp = new QueryTempOpRel(parent);
    boolean before = XMLUtil.getBoolean(temporalOpNode, "before", false);
    boolean after = XMLUtil.getBoolean(temporalOpNode, "after", false);
    if (before) {
      tempOp.setDayShiftMin(-Integer.MIN_VALUE);
      tempOp.setDayShiftMax(-1);
    } else if (after) {
      tempOp.setDayShiftMin(1);
      tempOp.setDayShiftMax(Integer.MAX_VALUE);
    } else {
      tempOp.setDayShiftMin(XMLUtil.getInt(temporalOpNode, "dayShiftMin", Integer.MAX_VALUE));
      tempOp.setDayShiftMax(XMLUtil.getInt(temporalOpNode, "dayShiftMax", Integer.MIN_VALUE));
      tempOp.setMonthShiftMin(XMLUtil.getInt(temporalOpNode, "monthShiftMin", Integer.MAX_VALUE));
      tempOp.setMonthShiftMax(XMLUtil.getInt(temporalOpNode, "monthShiftMax", Integer.MIN_VALUE));
      tempOp.setYearShiftMin(XMLUtil.getInt(temporalOpNode, "yearShiftMin", Integer.MAX_VALUE));
      tempOp.setYearShiftMax(XMLUtil.getInt(temporalOpNode, "yearShiftMax", Integer.MIN_VALUE));
    }
    tempOp.setRefElemID(XMLUtil.getString(temporalOpNode, "refElemID"));
    return tempOp;
  }

  private QuerySubQuery readSubQuery(Node subQueryNode, QueryStructureContainingElem parent)
          throws QueryException {
    String aName = XMLUtil.getString(subQueryNode, "name");
    int queryID = XMLUtil.getInt(subQueryNode, "queryID", -1);
    boolean displayColumns = XMLUtil.getBoolean(subQueryNode, "displayColumns", true);
    QuerySubQuery subQuery = new QuerySubQuery(parent, aName, queryID);
    subQuery.displayAnyColumns = displayColumns;
    return subQuery;
  }

  private QueryStatisticRow readDistributionRow(Node aNode, QueryStructureContainingElem parent)
          throws QueryException {
    QueryStatisticRow row = new QueryStatisticRow(parent);
    readStructureContainingElem(aNode, row);
    return row;
  }

  private QueryStatisticColumn readDistributionColumn(Node aNode,
          QueryStructureContainingElem parent) throws QueryException {
    QueryStatisticColumn column = new QueryStatisticColumn(parent);
    readStructureContainingElem(aNode, column);
    return column;
  }

  private QueryStatisticFilter readDistributionFilter(Node aNode,
          QueryStructureContainingElem parent) throws QueryException {
    QueryStatisticFilter filter = new QueryStatisticFilter(parent);
    readStructureContainingElem(aNode, filter);
    return filter;
  }

  private QueryTempOpAbs readTemporalOpAbs(Node temporalOpNode, QueryElem aContainer)
          throws QueryException {
    QueryTempOpAbs tempOp = new QueryTempOpAbs(aContainer);

    String absoluteDateMinString = XMLUtil.getString(temporalOpNode, "minDate");
    if (absoluteDateMinString != null) {
      if (absoluteDateMinString.matches("^..\\...\\.....$")) {
        absoluteDateMinString += " 00:00:00";
      }
      try {
        Date aMinDate = TimeUtil.getSdfWithTime().parse(absoluteDateMinString);
        tempOp.absMinDate = new Timestamp(aMinDate.getTime());
      } catch (ParseException e) {
        throw new QueryException(e);
      }
    }
    String absoluteDateMaxString = XMLUtil.getString(temporalOpNode, "maxDate");
    if (absoluteDateMaxString != null) {
      if (absoluteDateMaxString.matches("^..\\...\\.....$")) {
        absoluteDateMaxString += " 23:59:59";
      }
      try {
        Date aMaxDate = TimeUtil.getSdfWithTime().parse(absoluteDateMaxString);
        tempOp.absMaxDate = new Timestamp(aMaxDate.getTime());
      } catch (ParseException e) {
        throw new QueryException(e);
      }
    }
    return tempOp;
  }

}
