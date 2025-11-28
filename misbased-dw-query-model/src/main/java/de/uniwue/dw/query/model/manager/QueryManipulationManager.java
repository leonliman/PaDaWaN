package de.uniwue.dw.query.model.manager;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;

import org.xml.sax.SAXException;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.query.model.DWQueryConfig;
import de.uniwue.dw.query.model.data.RawQuery;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.ContentOperator;
import de.uniwue.dw.query.model.lang.QueryAnd;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryIDFilter;
import de.uniwue.dw.query.model.lang.QueryMiscOp;
import de.uniwue.dw.query.model.lang.QueryNTrue;
import de.uniwue.dw.query.model.lang.QueryNot;
import de.uniwue.dw.query.model.lang.QueryOr;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.lang.QueryStructureContainingElem;
import de.uniwue.dw.query.model.lang.QueryStructureElem;
import de.uniwue.dw.query.model.lang.QuerySubQuery;
import de.uniwue.dw.query.model.lang.QueryTempOpAbs;

/**
 * The QueryManager is used to manage a query data object. It manages the assembly and change of the
 * query and fires the respective events.
 */
public class QueryManipulationManager {

  private QueryRoot query;

  public static interface IQueryChangedListener {
    public void queryChanged();
  }

  public List<IQueryChangedListener> queryChangedListeners = new ArrayList<IQueryChangedListener>();

  private void callQueryChanged() {
    for (IQueryChangedListener aList : queryChangedListeners) {
      aList.queryChanged();
    }
  }

  public static interface INewQueryListener {
    public void newQueryCalled();
  }

  public List<INewQueryListener> newQueryListeners = new ArrayList<INewQueryListener>();

  private void callNewQuery() {
    for (INewQueryListener aList : newQueryListeners) {
      aList.newQueryCalled();
    }
  }

  public QueryManipulationManager() throws QueryException {
    refresh();
  }

  public void setQuery(QueryRoot aQuery) {
    query = aQuery;
    callNewQuery();
  }

  // If the query object is not yet set do it. This can happen when during the previous try to do so
  // some environment settings (like database connection) were not properly working
  public void refresh() throws QueryException {
    if (query == null) {
      QueryRoot newQuery = new QueryRoot();
      // newQuery.root.onlyCount = true;
      // newQuery.displayPID = true;
      setQuery(newQuery);
    }
  }

  public List<QueryStructureElem> cloneElem(List<QueryStructureElem> list)
          throws XMLStreamException, QueryException, ParserConfigurationException, SAXException,
          IOException {
    List<QueryStructureElem> deepCopy = new ArrayList<>();
    for (QueryStructureElem e : list) {
      QueryStructureElem clone = cloneElem(e);
      deepCopy.add(clone);
    }
    return deepCopy;
  }

  public QueryStructureElem cloneElem(QueryStructureElem anElem) throws XMLStreamException,
          QueryException, ParserConfigurationException, SAXException, IOException {
    try {
      return QueryStructureElem.copyElem(DWQueryConfig.getInstance().getCatalogClientManager(),
              anElem);
    } catch (SQLException e) {
      throw new QueryException(e);
    }
  }

  public QueryRoot getQuery() {
    return query;
  }

  public void deleteElement(QueryStructureElem anElem) {
    List<QueryStructureElem> elemsToDelete = new ArrayList<QueryStructureElem>();
    elemsToDelete.add(anElem);
    deleteElements(elemsToDelete);
  }

  public void deleteElements(List<QueryStructureElem> elems) {
    for (QueryStructureElem anElem : elems) {
      anElem.getContainer().removeChild(anElem);
    }
    callQueryChanged();
  }

  public QueryStructureContainingElem addMiscOp(QueryMiscOp logOp,
          QueryStructureContainingElem aContainer) throws QueryException {
    return addMiscOp(logOp, aContainer, aContainer.getChildren().size());
  }

  public QueryAnd addAnd(QueryStructureContainingElem aContainer, int position)
          throws QueryException {
    return (QueryAnd) addMiscOp(QueryMiscOp.And, aContainer, position);
  }

  public QueryAnd addAnd(QueryStructureContainingElem aContainer) throws QueryException {
    return (QueryAnd) addMiscOp(QueryMiscOp.And, aContainer);
  }

  public QueryIDFilter addIDFilter(QueryStructureContainingElem aContainer) throws QueryException {
    return (QueryIDFilter) addMiscOp(QueryMiscOp.IDFilter, aContainer);
  }

  public QueryTempOpAbs addTempOpAbs(QueryStructureElem aContainer) throws QueryException {
    QueryTempOpAbs result = new QueryTempOpAbs(aContainer);
    aContainer.addTempOpAbs(result);
    return result;
  }

  public QueryOr addOr(QueryStructureContainingElem aContainer, int position)
          throws QueryException {
    return (QueryOr) addMiscOp(QueryMiscOp.Or, aContainer, position);
  }

  public QueryOr addOr(QueryStructureContainingElem aContainer) throws QueryException {
    return (QueryOr) addMiscOp(QueryMiscOp.Or, aContainer);
  }

  public QueryStructureContainingElem addMiscOp(QueryMiscOp logOp,
          QueryStructureContainingElem aContainer, int position) throws QueryException {
    QueryStructureContainingElem result = null;
    if (logOp == QueryMiscOp.IDFilter) {
      result = new QueryIDFilter(aContainer, position);
    } else if (logOp == QueryMiscOp.And) {
      result = new QueryAnd(aContainer, position);
    } else if (logOp == QueryMiscOp.Or) {
      result = new QueryOr(aContainer, position);
    } else if (logOp == QueryMiscOp.Not) {
      result = new QueryNot(aContainer, position);
    } else if (logOp == QueryMiscOp.NTrue) {
      result = new QueryNTrue(aContainer, position);
    }
    callQueryChanged();
    return result;
  }

  public QueryAttribute addAttribute(CatalogEntry anEntry) throws QueryException {
    return addAttribute(anEntry, query);
  }

  public QuerySubQuery addSubQuery(RawQuery aRawSubQuery, QueryStructureContainingElem aContainer)
          throws QueryException {
    QuerySubQuery aSubQuery = new QuerySubQuery(aContainer, aRawSubQuery.getName(),
            aRawSubQuery.getId());
    return aSubQuery;
  }

  public QueryAttribute addAttribute(CatalogEntry anEntry, QueryStructureContainingElem aContainer)
          throws QueryException {
    return addAttribute(anEntry, aContainer, aContainer.getChildren().size());
  }

  public QueryAttribute addAttribute(CatalogEntry anEntry, QueryStructureContainingElem aContainer,
          int position) throws QueryException {
    QueryAttribute attr = new QueryAttribute(aContainer, anEntry, position);
    callQueryChanged();
    return attr;
  }

  public void addAttributes(List<CatalogEntry> entries, QueryStructureContainingElem aContainer,
          int position) throws QueryException {
    for (CatalogEntry anEntry : entries) {
      QueryAttribute attr = new QueryAttribute(aContainer, anEntry, position);
      attr.setOptional(true);
      if (anEntry.getDataType() == CatalogEntryType.Text) {
        attr.setValueInFile(true);
      }
    }
    callQueryChanged();
  }

  public void copySubElements(QueryStructureContainingElem source,
          QueryStructureContainingElem destination) throws QueryException, XMLStreamException,
          SQLException, ParserConfigurationException, SAXException, IOException {
    copyElems(source.getChildren(), destination, destination.getChildren().size());
  }

  public void copyElem(QueryStructureElem anElem, QueryStructureContainingElem newContainer)
          throws XMLStreamException, QueryException, SQLException, ParserConfigurationException,
          SAXException, IOException {
    copyElem(anElem, newContainer, newContainer.getChildren().size());
  }

  public void copyElem(QueryStructureElem anElem, QueryStructureContainingElem newContainer,
          int position) throws XMLStreamException, QueryException, SQLException,
          ParserConfigurationException, SAXException, IOException {
    List<QueryStructureElem> elems = new ArrayList<QueryStructureElem>();
    elems.add(anElem);
    copyElems(elems, newContainer, position);
  }

  public void copyElems(List<QueryStructureElem> elems, QueryStructureContainingElem newContainer,
          int position) throws XMLStreamException, QueryException, SQLException,
          ParserConfigurationException, SAXException, IOException {
    for (QueryStructureElem anElem : elems) {
      QueryStructureElem newElem = QueryStructureElem
              .copyElem(DWQueryConfig.getInstance().getCatalogClientManager(), anElem);
      newContainer.addChild(newElem, position);
      position++;
    }
    callQueryChanged();
  }

  public void moveElem(QueryStructureElem anElem, QueryStructureContainingElem newContainer)
          throws QueryException {
    anElem.getContainer().removeChild(anElem);
    newContainer.addChild(anElem);
    callQueryChanged();
  }

  public void moveElem(QueryStructureElem anElem, QueryStructureContainingElem newContainer,
          int position) {
    if ((anElem.getContainer() == newContainer)
            && (position >= anElem.getContainer().getChildren().indexOf(anElem))) {
      position--;
    }
    anElem.getContainer().removeChild(anElem);
    newContainer.addChild(anElem, position);
    callQueryChanged();
  }

  // TODO: replace code in "moveElem"-method by calling "moveElems"
  public void moveElems(List<QueryStructureElem> elems, QueryStructureContainingElem newContainer,
          int position) {
    for (QueryStructureElem anElem : elems) {
      if ((anElem.getContainer() == newContainer)
              && (position >= anElem.getContainer().getChildren().indexOf(anElem))) {
        position--;
      }
      anElem.getContainer().removeChild(anElem);
      newContainer.addChild(anElem, position);
      position++;
    }
    callQueryChanged();
  }

  public void setDesiredContent(QueryAttribute anAttr, String value) {
    anAttr.setDesiredContent(value);
    callQueryChanged();
  }

  public void setContentOp(QueryAttribute anAttr, ContentOperator value) {
    anAttr.setContentOperator(value);
    callQueryChanged();
  }

  public void setNTrue(QueryNTrue anElem, int aValue) {
    anElem.n = aValue;
    callQueryChanged();
  }

  public void setDisplayPID(QueryRoot anElem, boolean aValue) {
    anElem.setDisplayPID(aValue);
    callQueryChanged();
  }

  public static void shrinkQuery(QueryRoot query) {
    try {
      // System.out.println(query.generateXML());
      String lastXml = "";
      boolean queryHasChanged = true;
      while (queryHasChanged) {
        query.shrink();
        String curXml = query.generateXML();
        queryHasChanged = !lastXml.equals(curXml);
        lastXml = curXml;
      }
    } catch (QueryException e) {
      e.printStackTrace();
    }
  }

}
