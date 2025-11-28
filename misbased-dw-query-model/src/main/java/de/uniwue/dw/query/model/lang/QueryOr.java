package de.uniwue.dw.query.model.lang;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.manager.QueryManipulationManager;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

public class QueryOr extends QueryStructureContainingElem {

  // this is only for a statistic query
  private boolean powerSet = false;

  private String name;

  public QueryOr(QueryStructureContainingElem aContainer) {
    super(aContainer);
  }

  public QueryOr(QueryStructureContainingElem aContainer, int position) throws QueryException {
    super(aContainer, position);
  }

  @Override
  public String getXMLName() {
    return "Or";
  }

  @Override
  public void generateXMLAttributes(XMLStreamWriter writer)
          throws XMLStreamException {
    super.generateXMLAttributes(writer);
    if (isPowerSet()) {
      writer.writeAttribute("powerSet", Boolean.toString(isPowerSet()));
    }
    if (getName() != null) {
      writer.writeAttribute("name", getName());
    }
  }

  @Override
  public <T> T accept(IQueryElementVisitor<T> visitor) throws QueryException {
    return visitor.visit(this);
  }

  @Override
  public void shrink(QueryManipulationManager manager) throws QueryException {
    super.shrink(manager);
    if ((getClass() == QueryOr.class) && (getContainer() != null) && (getContainer() instanceof QueryOr)) {
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
