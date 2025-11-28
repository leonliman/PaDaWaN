package de.uniwue.dw.query.model.lang;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import de.uniwue.dw.query.model.QueryReader;
import de.uniwue.dw.query.model.data.RawQuery;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.manager.IQueryIOManager;
import de.uniwue.dw.query.model.manager.QueryManipulationManager;

public class QuerySubQuery extends QueryStructureElem {

  public String name;

  public int queryID;

  public boolean displayAnyColumns = true;

  public QuerySubQuery(QueryStructureContainingElem aContainer, String aName, int aQueryID) {
    super(aContainer);
    this.name = aName;
    this.queryID = aQueryID;
  }

  @Override
  public String getXMLName() {
    return "SubQuery";
  }

  @Override
  public void generateXMLAttributes(XMLStreamWriter writer) throws XMLStreamException {
    super.generateXMLAttributes(writer);
    writer.writeAttribute("name", name);
    writer.writeAttribute("queryID", Integer.toString(queryID));
  }

  @Override
  public void expandSubQueries(QueryManipulationManager manager, IQueryIOManager allQueriesManager)
          throws QueryException {
    RawQuery rawQuery;

    if (queryID == -1) {
      rawQuery = allQueriesManager.getQuery(name);
    } else {
      rawQuery = allQueriesManager.getQuery(queryID);
    }
    QueryRoot queryRoot = QueryReader.read(allQueriesManager.getCatalogClientManager(),
            rawQuery.getXml());
    QueryAnd and = manager.addAnd(getContainer(), getPosition());
    and.setName(name);
    if (!displayAnyColumns) {
      for (QueryAttribute anAttr : queryRoot.getAttributesRecursive()) {
         anAttr.setDisplayCaseID(false);
         anAttr.setDisplayDocID(false);
         anAttr.setDisplayValue(false);
         anAttr.setDisplayInfoDate(false);
      }
    }
    for (QueryStructureElem aChild : queryRoot.getChildren()) {
      if (isOptional()) {
        aChild.setOptional(true);
      }
      and.addChild(aChild);
    }
    manager.deleteElement(this);
    and.expandSubQueries(manager, allQueriesManager);
  }

  @Override
  public <T> T accept(IQueryElementVisitor<T> visitor) throws QueryException {
    return visitor.visit(this);
  }

  public String getName() {
    return name;
  }

}
