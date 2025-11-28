package de.uniwue.dw.query.model.lang;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.misc.util.TimeUtil;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.sql.Timestamp;

public class QueryTempOpAbs extends QueryElem {

  public QueryTempOpAbs(QueryElem parent) {
    super(parent);
  }

  public Timestamp absMinDate, absMaxDate;

  public String getDisplayString() {
    String result = "";
    if ((absMinDate != null) && (absMaxDate == null)) {
      result = "( >= " + absMinDate.toString() + ")";
    } else if ((absMinDate == null) && (absMaxDate != null)) {
      result = "( <= " + absMaxDate.toString() + ")";
    } else {
      result = "( " + absMinDate.toString() + " <= x <= " + absMaxDate.toString() + ")";
    }
    return result;
  }

  public void generateXML(XMLStreamWriter writer) throws QueryException {
    try {
      writer.writeStartElement("TempOpAbs");
      generateXMLAttributes(writer);
      writer.writeEndElement();
    } catch (XMLStreamException e) {
      throw new QueryException(e);
    }
  }

  @Override
  public void generateXMLAttributes(XMLStreamWriter writer)
          throws XMLStreamException {
    if (absMinDate != null) {
      writer.writeAttribute("minDate", TimeUtil.getSdfWithTime().format(absMinDate));
    }
    if (absMaxDate != null) {
      writer.writeAttribute("maxDate", TimeUtil.getSdfWithTime().format(absMaxDate));
    }
  }

  @Override
  public <T> T accept(IQueryElementVisitor<T> visitor) throws QueryException {
    return visitor.visit(this);
  }

}
