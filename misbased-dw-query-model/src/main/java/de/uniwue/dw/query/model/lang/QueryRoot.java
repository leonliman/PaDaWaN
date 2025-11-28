package de.uniwue.dw.query.model.lang;

import de.uniwue.dw.core.model.manager.ICatalogClientManager;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;
import de.uniwue.misc.util.XMLUtil;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

// The root is the top query element in a query. It is derived from a queryAnd because all
// containing elements have to be fulfilled
public class QueryRoot extends QueryAnd {

  public static int limitResultDefault = 0;

  public static String currentVersion = "0.8";

  // should the pid appear as a result column ?
  private boolean displayPID = false;

  // should the result be a list of result rows or only the count of the result rows ?
  private boolean onlyCount = false;

  // how many result rows should at maximum be displayed ?
  // if limitResult is set to zero, all results wil be returned
  private int limitResult = limitResultDefault;

  // only Attributes and the root can have an ID
  private String id;

  // should the final query be wrapped in an additional distinct ?
  private boolean distinct = false;

  // the version of MXML that the model of this query belongs to
  private String version = currentVersion;

  // original FilterIDType, if the parameter query.alwaysGroupDistinctQueriesOnDocLevel is set to true
  private FilterIDType filterIDTypeForDistinctCount = null;

  public QueryRoot() {
    super(null);
    setId("root");
  }

  public QueryRoot(QueryStructureContainingElem aContainer) throws QueryException {
    this();
    throw new QueryException("Should not happen. Nobody contains a root.");
  }

  @Override
  public boolean isRoot() {
    return true;
  }

  protected String getNewAlias() {
    int x = 1;
    String paramBaseName = "Param";
    String result = paramBaseName + x;
    boolean nameIsNew = false;
    while (!nameIsNew) {
      nameIsNew = true;
      for (QueryAttribute anAttr : getAttributesRecursive()) {
        if ((anAttr.getId() != null) && anAttr.getId().equals(result)) {
          nameIsNew = false;
          x++;
          result = paramBaseName + x;
        }
      }
    }
    return result;
  }

  public FilterIDType getFilterIDTypeToUseForCount() {
    List<QueryAttribute> attrs = getAttributesRecursive();
    if (attrs.isEmpty()) {
      List<QueryIDFilter> idFilters = getIDFilterRecursive();
      for (QueryIDFilter aFilter : idFilters) {
        if (aFilter.getFilterIDType() != FilterIDType.Year && aFilter.getFilterIDType() != FilterIDType.GROUP) {
          return aFilter.getFilterIDType();
        }
      }
    } else {
      QueryAttribute firstAttribute = attrs.get(0);
      List<QueryIDFilter> idFilters = firstAttribute.getAncestorIDFilters();
      for (QueryIDFilter aFilter : idFilters) {
        if (aFilter.getFilterIDType() != FilterIDType.Year && aFilter.getFilterIDType() != FilterIDType.GROUP) {
          return aFilter.getFilterIDType();
        }
      }
    }
    return FilterIDType.PID;
  }

  public boolean hasToAddDocIDForQuery() {
    for (QueryIDFilter aFilter : getIDFilterRecursive()) {
      if (aFilter.getFilterIDType() == FilterIDType.DocID) {
        return true;
      }
    }
    return containsAttributeWithDisplayDocID();
  }

  public boolean isStatisticQuery() {
    for (QueryStructureElem aChild : getSiblings()) {
      if ((aChild instanceof QueryStatisticColumn) || (aChild instanceof QueryStatisticRow)
              || (aChild instanceof QueryStatisticFilter)) {
        return true;
      }
    }
    return false;
  }

  public boolean containsAttributeWithDisplayDocID() {
    for (QueryAttribute anAttr : getAttributesRecursive()) {
      if (anAttr.displayDocID()) {
        return true;
      }
    }
    return false;
  }

  // TODO: das sollte lieber ohne reflection gemacht werden, und stattdessen auf dei generischen und
  // die konkreten Klassen verteilt werden
  public List<QueryIDFilter> getExistingPIDsFilters() {
    List<QueryIDFilter> result = new ArrayList<QueryIDFilter>();
    for (QueryStructureElem anElem : getSiblings()) {
      if (anElem instanceof QueryIDFilter) {
        result.add((QueryIDFilter) anElem);
      }
    }
    return result;
  }

  /*
   * Does at least one attribute in this query export its values into separate files ?
   */
  public boolean containsAttributesWithValueInFile() {
    for (QueryAttribute anAttribute : getAttributesRecursive()) {
      if (anAttribute.getValueInFile()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public QueryStructureElem getElem(String aRefId) throws QueryException {
    if ((getId() != null) && (getId().equals(aRefId))) {
      return this;
    }
    for (QueryAttribute anAttr : getAttributesRecursive()) {
      if ((anAttr.getId() != null) && anAttr.getId().equals(aRefId)) {
        return anAttr;
      }
    }
    throw new QueryException("undefined query element '" + aRefId + "'");
  }

  @Override
  public String getXMLName() {
    return "Query";
  }

  @Override
  public void generateXML(XMLStreamWriter writer) throws QueryException {
    try {
      writer.writeStartElement(getXMLName());
      generateXMLAttributes(writer);
      for (QueryStructureElem anAttr : getChildren()) {
        anAttr.generateXML(writer);
      }
      writer.writeEndElement();
    } catch (XMLStreamException e) {
      throw new QueryException(e);
    }
  }

  @Override
  public void generateXMLAttributes(XMLStreamWriter writer) throws XMLStreamException {
    if (isOnlyCount()) {
      writer.writeAttribute("onlyCount", Boolean.toString(isOnlyCount()));
    }
    if (isDisplayPID()) {
      writer.writeAttribute("pid", Boolean.toString(isDisplayPID()));
    }
    if (getLimitResult() != limitResultDefault) {
      writer.writeAttribute("limitResult", Integer.toString(getLimitResult()));
    }
    if (isDistinct()) {
      writer.writeAttribute("distinct", Boolean.toString(isDistinct()));
    }
    if ((getComment() != null) && !getComment().isEmpty()) {
      writer.writeAttribute("comment", getComment());
    }
    writer.writeAttribute("version", getVersion());
  }

  // so that the query can be arbitrarily manipulated during the query (optimizations, etc.) the
  // root is copied so that the original one remains as it is
  public QueryRoot copyForQuery(ICatalogClientManager catalogClientManager) throws QueryException {
    try {
      QueryRoot result = (QueryRoot) QueryStructureElem.copyElem(catalogClientManager, this);
      return result;
    } catch (XMLStreamException e) {
      throw new QueryException(e);
    } catch (SQLException e) {
      throw new QueryException(e);
    } catch (ParserConfigurationException e) {
      throw new QueryException(e);
    } catch (SAXException e) {
      throw new QueryException(e);
    } catch (IOException e) {
      throw new QueryException(e);
    }
  }

  // // allow only at least K rows in the result, else clear the result
  // // and add an error message
  // public void checkKAnonymity(List<List<Object>> result) {
  // List<Object> errorHeaderList = new ArrayList<Object>();
  // errorHeaderList.add("result");
  // List<Object> errorResultList = new ArrayList<Object>();
  // errorResultList.add("fewer than " + DWQueryConfig.getKAnonymity() + " in result. \n"
  // + "No results may be returned due to data privacy reasons");
  // if (isOnlyCount()) {
  // int count = Integer.parseInt(result.get(1).get(0).toString());
  // if ((count > 0) && (count < DWQueryConfig.getKAnonymity())) {
  // result.clear();
  // result.add(errorHeaderList);
  // result.add(errorResultList);
  // }
  // } else {
  // if (result.size() < DWQueryConfig.getKAnonymity()) {
  // result.clear();
  // result.add(errorHeaderList);
  // result.add(errorResultList);
  // }
  // }
  // }

  public String generateXML() throws QueryException {
    XMLStreamWriter writer = XMLUtil.createStreamWriter();
    generateXML(writer);
    try {
      String xml = XMLUtil.writeWriterContent(writer);
      return xml;
    } catch (XMLStreamException e) {
      throw new QueryException(e);
    } catch (ParserConfigurationException e) {
      throw new QueryException(e);
    } catch (SAXException e) {
      throw new QueryException(e);
    } catch (IOException e) {
      throw new QueryException(e);
    }
  }

  @Override
  public <T> T accept(IQueryElementVisitor<T> visitor) throws QueryException {
    return visitor.visit(this);
  }

  public boolean isDisplayPID() {
    return displayPID;
  }

  public void setDisplayPID(boolean displayPID) {
    this.displayPID = displayPID;
  }

  public boolean isOnlyCount() {
    return onlyCount;
  }

  public void setOnlyCount(boolean onlyCount) {
    this.onlyCount = onlyCount;
  }

  public int getLimitResult() {
    return limitResult;
  }

  public void setLimitResult(int limitResult) {
    this.limitResult = limitResult;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public boolean isDistinct() {
    return distinct;
  }

  public void setDistinct(boolean distinct) {
    this.distinct = distinct;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public FilterIDType getFilterIDTypeForDistinctCount() {
    return filterIDTypeForDistinctCount;
  }

  public void setFilterIDTypeForDistinctCount(FilterIDType filterIDTypeForDistinctCount) {
    this.filterIDTypeForDistinctCount = filterIDTypeForDistinctCount;
  }
}
