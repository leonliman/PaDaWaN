package de.uniwue.dw.query.model.lang;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import de.uniwue.dw.query.model.exception.QueryException;

public class QueryNTrue extends QueryStructureContainingElem {

  public int n;

  public QueryNTrue(QueryStructureContainingElem aContainer) throws QueryException {
    super(aContainer);
  }

  public QueryNTrue(QueryStructureContainingElem aContainer, int position) throws QueryException {
    super(aContainer, position);
  }

  @Override
  public void generateXMLAttributes(XMLStreamWriter writer)
          throws XMLStreamException {
    super.generateXMLAttributes(writer);
    writer.writeAttribute("n", Integer.toString(n));
  }

  @Override
  public String getXMLName() {
    return "NTrue";
  }

  @Override
  public <T> T accept(IQueryElementVisitor<T> visitor) throws QueryException {
    return visitor.visit(this);
  }

}
