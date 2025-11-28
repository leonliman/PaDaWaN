package de.uniwue.dw.query.model.lang;

import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import de.uniwue.dw.query.model.exception.QueryException;

public abstract class QueryElem {

  private QueryElem parent;

  public abstract void generateXMLAttributes(XMLStreamWriter writer) throws XMLStreamException;

  public abstract <T> T accept(IQueryElementVisitor<T> visitor) throws QueryException;

  public QueryRoot getRoot() {
    if (getParent() == null) {
      if (this instanceof QueryRoot) {
        return (QueryRoot) this;
      } else {
        return null;
      }
    } else {
      return getParent().getRoot();
    }
  }

  public QueryElem(QueryElem parent) {
    this.setParent(parent);
  }

  public void initialize() {
  }

  public QueryElem getParent() {
    return parent;
  }

  public void setParent(QueryElem parent) {
    this.parent = parent;
  }

  public List<QueryElem> getAllParent() {
    return getAncestors();
  }

  private void getAncestors(List<QueryElem> result) {
    if (getParent() != null) {
      result.add(getParent());
      getParent().getAncestors(result);
    }
  }

  /**
   * Returns all ancestors of this entry including the root
   */
  public List<QueryElem> getAncestors() {
    List<QueryElem> result = new ArrayList<QueryElem>();
    getAncestors(result);
    return result;
  }

  @Override
  public String toString() {
    try {
      return this.accept(new QueryHierarchicalStringVisitor());
    } catch (QueryException e) {
      e.printStackTrace();
      return "";
    }
  }

}
