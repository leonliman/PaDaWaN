package de.uniwue.dw.query.model.lang;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import de.uniwue.dw.query.model.exception.QueryException;

public class QueryValueCompare extends QueryElem {

  public QueryAttribute refElem;

  public String refElemID;

  private ContentOperator contentOperator;

  public QueryValueCompare(QueryElem parent) {
    super(parent);
  }

  public void setContentOperator(ContentOperator contentOperator) throws QueryException {
    if (contentOperator == ContentOperator.MORE || contentOperator == ContentOperator.LESS
            || contentOperator == ContentOperator.EQUALS
            || contentOperator == ContentOperator.MORE_OR_EQUAL
            || contentOperator == ContentOperator.LESS_OR_EQUAL) {
      this.contentOperator = contentOperator;
    } else {
      String errorMsg = "ValueCompare is only supporting the follwing contentOperators: "
              + "MORE, LESS, EQUALS, MORE_OR_EQUAL, LESS_OR_EQUAL";
      throw new QueryException(errorMsg);
    }
  }

  public ContentOperator getContentOperator() {
    return contentOperator;
  }

  public void generateXML(XMLStreamWriter writer) throws XMLStreamException {
    writer.writeStartElement("ValueCompare");
    generateXMLAttributes(writer);
    writer.writeEndElement();
  }

  @Override
  public void generateXMLAttributes(XMLStreamWriter writer) throws XMLStreamException {
    writer.writeAttribute("refElemID", refElemID);
    writer.writeAttribute("contentOperator", contentOperator.toString());
  }

  public String getDisplayString() {
    String result = "[value compare rel to: " + refElem.getCatalogEntry().getName();
    result += " with contentOperator " + contentOperator + "]";
    return result;
  }

  @Override
  public <T> T accept(IQueryElementVisitor<T> visitor) throws QueryException {
    return visitor.visit(this);
  }

}
