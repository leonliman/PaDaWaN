package de.uniwue.dw.query.model.lang;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.xml.sax.SAXException;

import de.uniwue.dw.core.model.manager.ICatalogClientManager;
import de.uniwue.dw.query.model.QueryReader;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;
import de.uniwue.dw.query.model.manager.IQueryIOManager;
import de.uniwue.dw.query.model.manager.QueryManipulationManager;
import de.uniwue.misc.util.XMLUtil;

public abstract class QueryStructureElem extends QueryElem {

  // the parent container
  private QueryStructureContainingElem container;

  // is the attribute needed or only optional for the result (JOIN or LEFT JOIN in sql)
  // if false, the result will be filtered by this query element.
  // TODO: ContentOperator.EMPTY has an equal functionality
  protected boolean optional;

  // This defines if entries with an unobserved value will be filtered out.
  // Note: This variable has only an impact, if this query element is a filter.
  // If true, entries, where one or more attributes of this query element have no value (or don't
  // match), will be filtered out.
  // If false, a missing value of an attribute is no constraint violation.
  private boolean filterUnkown = true;

  // an aditional name, comment or something which is displayed on the GUI
  private String comment = "";

  // is set to FALSE the element is disabled and acts as if it weren't included in the query
  public boolean active = true;

  // a list of relative temporal relations which hold for the element itself as well as all elements
  // possibly contained in the element
  private List<QueryTempOpRel> temporalOpsRel = new ArrayList<QueryTempOpRel>();

  // a list of absolute temporal relations which hold for the element itself as well as all elements
  // possibly contained in the element
  private List<QueryTempOpAbs> temporalOpsAbs = new ArrayList<QueryTempOpAbs>();

  public abstract String getXMLName();

  // does the element create an output columns in the result ?
  public boolean displayColumns() {
    return false;
  }

  public void sortChildrenForQuery() {
  }

  public void removeInactiveChildren() {
  }

  // empty AND, OR and NOT-elements are removed
  public void shrink(QueryManipulationManager manager) throws QueryException {
  }

  // empty AND, OR and NOT-elements are removed
  public void shrink() throws QueryException {
    QueryManipulationManager manager = new QueryManipulationManager();
    manager.setQuery(getRoot());
    shrink(manager);
  }

  public boolean isRoot() {
    return false;
  }

  // get all siblings, i.e. the children and recursively their children.
  // Non-active siblings are not included in the result !
  public List<QueryStructureElem> getSiblings() {
    List<QueryStructureElem> result = new ArrayList<QueryStructureElem>();
    return result;
  }

  // get only the direct children (not recursively all siblings)
  public List<QueryStructureElem> getChildren() {
    List<QueryStructureElem> result = new ArrayList<QueryStructureElem>();
    return result;
  }

  public List<QueryTempOpRel> getTempOpsRelRecursive() {
    List<QueryTempOpRel> result = new ArrayList<QueryTempOpRel>();
    for (QueryStructureElem anElem : getSiblings()) {
      result.addAll(anElem.getTemporalOpsRel());
    }
    return result;
  }

  public List<QueryTempOpAbs> getTempOpsAbsRecursive() {
    List<QueryTempOpAbs> result = new ArrayList<QueryTempOpAbs>();
    for (QueryStructureElem anElem : getSiblings()) {
      result.addAll(anElem.getTempOpsAbs());
    }
    return result;
  }

  public List<QueryTempOpAbs> getTempOpsAbs() {
    return temporalOpsAbs;
  }

  public List<QueryValueCompare> getValueComparesRecursive() {
    List<QueryValueCompare> result = new ArrayList<QueryValueCompare>();
    for (QueryAttribute anElem : getAttributesRecursive()) {
      result.addAll(anElem.getValueCompares());
    }
    return result;
  }

  // get all siblings which are QueryAttributes.
  public List<QueryAttribute> getAttributesRecursive() {
    List<QueryAttribute> result = new ArrayList<QueryAttribute>();
    return result;
  }

  // get all siblings which are QueryAttributes.
  public List<QueryIDFilter> getIDFilterRecursive() {
    List<QueryIDFilter> result = new ArrayList<QueryIDFilter>();
    return result;
  }

  public List<QueryStructureElem> getReducingElemsRecursive() {
    List<QueryStructureElem> result = new ArrayList<QueryStructureElem>();
    return result;
  }

  // so that not every Element has to implement a copy constructor this is done by
  // serializing to XML and immediate deserialization
  public static QueryStructureElem copyElem(ICatalogClientManager catalogClientManager,
          QueryStructureElem anElem) throws XMLStreamException, QueryException, SQLException,
          ParserConfigurationException, SAXException, IOException {
    String xml = anElem.toXML();
    QueryStructureElem result = QueryReader.readStructureElem(catalogClientManager, xml);
    result.copyFinalize(anElem);
    return result;
  }

  // so that not every Element has to implement a clone constructor this is done by
  // serializing to XML and immediate deserialization
  public static QueryStructureElem cloneElem(ICatalogClientManager catalogClientManager,
          QueryStructureElem anElem) throws XMLStreamException, QueryException, SQLException,
          ParserConfigurationException, SAXException, IOException {
    String xml = anElem.toXML();
    QueryStructureElem result = QueryReader.readStructureElem(catalogClientManager, xml);
    result.copyFinalize(anElem);
    return result;
  }

  // this method is for possible after effects after a copy from a source
  // element has been created
  public void copyFinalize(QueryStructureElem sourceElem) {
  }

  public QueryStructureElem(QueryStructureContainingElem aContainer) {
    this(aContainer, (aContainer == null) ? 0 : aContainer.getChildren().size());
  }

  public QueryStructureElem(QueryStructureContainingElem aContainer, int position) {
    super(aContainer);
    if (aContainer != null) {
      aContainer.addChild(this, position);
    }
  }

  public boolean canContainElements() {
    return false;
  }

  public String toXML() throws QueryException {
    XMLStreamWriter writer = XMLUtil.createStreamWriter();
    generateXML(writer);
    try {
      String xml = XMLUtil.writeWriterContent(writer);
      return xml;
    } catch (XMLStreamException | ParserConfigurationException | SAXException | IOException e) {
      throw new QueryException(e);
    }
  }

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
    if (!active) {
      writer.writeAttribute("active", Boolean.toString(active));
    }
    if ((comment != null) && !comment.isEmpty()) {
      writer.writeAttribute("comment", comment);
    }
    if (optional) {
      writer.writeAttribute("optional", Boolean.toString(optional));
    }
    if (!filterUnkown) {
      writer.writeAttribute("filterUnknown", Boolean.toString(filterUnkown));
    }
  }

  public boolean isFirstDisplayedElemInContainer() throws QueryException {
    if (!displayColumns()) {
      return false;
    }
    for (QueryStructureElem aBrother : getContainer().getChildren()) {
      if (aBrother == this) {
        return true;
      } else if (aBrother.displayColumns()) {
        return false;
      }
    }
    throw new QueryException("This should not happen. Parent has no children ?!!!");
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  // public List<QueryStructureElem> getAncestors() {
  // List<QueryStructureElem> result = new ArrayList<QueryStructureElem>();
  // if (!isRoot()) {
  // result = container.getAncestors();
  // result.add(container);
  // }
  // return result;
  // }

  public List<QueryIDFilter> getAncestorIDFiltersWithType(FilterIDType aType) {
    List<QueryIDFilter> ancestorIDFilters = getAncestorIDFilters();
    for (QueryIDFilter aFilter : ancestorIDFilters.toArray(new QueryIDFilter[0])) {
      if (aFilter.getFilterIDType() != aType) {
        ancestorIDFilters.remove(aFilter);
      }
    }
    return ancestorIDFilters;
  }

  public List<QueryIDFilter> getAncestorIDFilters() {
    List<QueryIDFilter> result = new ArrayList<QueryIDFilter>();
    if (this instanceof QueryIDFilter) {
      result.add((QueryIDFilter) this);
    }
    if (getContainer() != null) {
      result.addAll(getContainer().getAncestorIDFilters());
    }
    return result;
  }

  public QueryIDFilter getAncestorFilterWithTimes() {
    QueryIDFilter result = null;
    List<QueryIDFilter> ancestorIDFilters = getAncestorIDFilters();
    for (QueryIDFilter aFilter : ancestorIDFilters) {
      if (aFilter.hasTimeRestrictions()) {
        result = aFilter;
      }
    }
    return result;
  }

  public int getPosition() {
    return getContainer().getChildren().indexOf(this);
  }

  public QueryIDFilter getMostRestrictingAncestorFilters() {
    QueryIDFilter result = null;
    List<QueryIDFilter> ancestorIDFilters = getAncestorIDFilters();
    for (QueryIDFilter aFilter : ancestorIDFilters) {
      if (result == null) {
        result = aFilter;
      } else if (result.getFilterIDType() == FilterIDType.PID) {
        result = aFilter;
      } else if ((result.getFilterIDType() == FilterIDType.CaseID)
              && (aFilter.getFilterIDType() == FilterIDType.DocID)) {
        result = aFilter;
      }
    }
    return result;
  }

  public void expandSubQueries(QueryManipulationManager manager, IQueryIOManager allQueriesManager)
          throws QueryException {
  }

  public boolean isOptional() {
    return optional;
  }

  public void setOptional(boolean optional) {
    this.optional = optional;
  }

  public boolean isFilterUnkown() {
    return filterUnkown;
  }

  public void setFilterUnkown(boolean filterUnkown) {
    this.filterUnkown = filterUnkown;
  }

  public List<QueryTempOpRel> getTemporalOpsRel() {
    return temporalOpsRel;
  }

  public void addTempOpAbs(QueryTempOpAbs anOp) {
    temporalOpsAbs.add(anOp);
  }

  public void setTemporalOpsRel(List<QueryTempOpRel> temporalOpsRel) {
    this.temporalOpsRel = temporalOpsRel;
  }

  public QueryStructureContainingElem getContainer() {
    return container;
  }

  public void setContainer(QueryStructureContainingElem container) {
    this.container = container;
  }

}
