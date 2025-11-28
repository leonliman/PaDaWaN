package de.uniwue.dw.query.model.lang;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.manager.QueryManipulationManager;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

public class QueryAnd extends QueryStructureContainingElem {

  // this is only for a statistic query
  private boolean powerSet = false;

  // ANDs can have a name because when a full query with named sub queries is expanded and the sub
  // queries are transformed into ANDs, those ANDs retain the name of the former sub query, so that
  // it can still be identified. Also QueryRoot is an AND and needs a name
  private String name;

  public QueryAnd(QueryStructureContainingElem aContainer) {
    super(aContainer);
  }

  public QueryAnd(QueryStructureContainingElem aContainer, int position) {
    super(aContainer, position);
  }

  @Override
  public String getXMLName() {
    return "And";
  }

  @Override
  public void generateXMLAttributes(XMLStreamWriter writer) throws XMLStreamException {
    super.generateXMLAttributes(writer);
    if (isPowerSet()) {
      writer.writeAttribute("powerSet", Boolean.toString(isPowerSet()));
    }
    if (getName() != null) {
      writer.writeAttribute("name", getName());
    }
  }

  @Override
  public void shrink(QueryManipulationManager manager) throws QueryException {
    super.shrink(manager);
    // Roots are also ANDs. Don't do anything with Roots
    if (getClass() == QueryAnd.class) {
      if ((getContainer() != null) && (getContainer() instanceof QueryAnd)) {
        for (QueryStructureElem aChild : getChildren()) {
          manager.moveElem(aChild, getContainer());
        }
        manager.deleteElement(this);
        return;
      }
      if (getChildren().size() == 1) {
        manager.moveElem(getChildren().get(0), getContainer());
      }
      if (getChildren().size() == 0) {
        getContainer().removeChild(this);
      }
    }
  }

  @Override
  public <T> T accept(IQueryElementVisitor<T> visitor) throws QueryException {
    return visitor.visit(this);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public boolean isPowerSet() {
    return powerSet;
  }

  public void setPowerSet(boolean powerSet) {
    this.powerSet = powerSet;
  }
}
