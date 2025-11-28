package de.uniwue.dw.query.model.lang;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.misc.util.TimeUtil;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.util.Calendar;

public class QueryTempOpRel extends QueryElem {

  private int dayShiftMin;

  private int monthShiftMin;

  private int yearShiftMin;

  private int dayShiftMax;

  private int monthShiftMax;

  private int yearShiftMax;

  public boolean hasMinShiftBoundary = false;

  public boolean hasMaxShiftBoundary = false;

  private QueryAttribute refElem;

  private String refElemID;

  public QueryTempOpRel(QueryElem parent) {
    super(parent);
  }

  public void generateXML(XMLStreamWriter writer) throws XMLStreamException {
    writer.writeStartElement("TempOpRel");
    generateXMLAttributes(writer);
    writer.writeEndElement();
  }

  public boolean isMinShiftNegative() {
    long tenYear = 1000 * 60 * 60 * 24 * 30 * 12 * 10;
    Calendar cal = TimeUtil.cal;
    cal.setTimeInMillis(tenYear);
    cal.add(Calendar.DAY_OF_MONTH, getDayShiftMin());
    cal.add(Calendar.MONTH, getMonthShiftMin());
    cal.add(Calendar.YEAR, getYearShiftMin());
    return (cal.getTimeInMillis() < tenYear);
  }

  public boolean isMaxShiftNegative() {
    long tenYear = 1000 * 60 * 60 * 24 * 30 * 12 * 10;
    Calendar cal = TimeUtil.cal;
    cal.setTimeInMillis(tenYear);
    cal.add(Calendar.DAY_OF_MONTH, getDayShiftMax());
    cal.add(Calendar.MONTH, getMonthShiftMax());
    cal.add(Calendar.YEAR, getYearShiftMax());
    return (cal.getTimeInMillis() < tenYear);
  }

  @Override
  public void generateXMLAttributes(XMLStreamWriter writer) throws XMLStreamException {
    writer.writeAttribute("refElemID", getRefElemID());
    if (getDayShiftMin() != 0) {
      writer.writeAttribute("dayShiftMin", Integer.toString(getDayShiftMin()));
    }
    if (getMonthShiftMin() != 0) {
      writer.writeAttribute("monthShiftMin", Integer.toString(getMonthShiftMin()));
    }
    if (getYearShiftMin() != 0) {
      writer.writeAttribute("yearShiftMin", Integer.toString(getYearShiftMin()));
    }
    if (getDayShiftMax() != 0) {
      writer.writeAttribute("dayShiftMax", Integer.toString(getDayShiftMax()));
    }
    if (getMonthShiftMax() != 0) {
      writer.writeAttribute("monthShiftMax", Integer.toString(getMonthShiftMax()));
    }
    if (getYearShiftMax() != 0) {
      writer.writeAttribute("yearShiftMax", Integer.toString(getYearShiftMax()));
    }
  }

  public String getDisplayString() {
    String result = "[tmp rel to: " + getRefElem().getCatalogEntry().getName();
    Calendar cal = TimeUtil.cal;
    long tenYear = 1000 * 60 * 60 * 24 * 30 * 12 * 10;
    cal.setTimeInMillis(tenYear);
    cal.add(Calendar.DAY_OF_MONTH, getDayShiftMin());
    cal.add(Calendar.MONTH, getMonthShiftMin());
    cal.add(Calendar.YEAR, getYearShiftMin());
    if (hasMinShiftBoundary) {
      result += " at least ";
    }
    if (getYearShiftMin() != 0) {
      result += ", " + getYearShiftMin() + " years";
    }
    if (getMonthShiftMin() != 0) {
      result += ", " + getMonthShiftMin() + " months";
    }
    if (getDayShiftMin() != 0) {
      result += ", " + getDayShiftMin() + " days";
    }
    if (hasMinShiftBoundary) {
      if (cal.getTimeInMillis() < tenYear) {
        result += " earlier";
      } else {
        result += " later";
      }
    }
    if (hasMaxShiftBoundary) {
      result += " at most ";
    }
    cal.setTimeInMillis(tenYear);
    cal.add(Calendar.DAY_OF_MONTH, getDayShiftMax());
    cal.add(Calendar.MONTH, getMonthShiftMax());
    cal.add(Calendar.YEAR, getYearShiftMax());
    if (getYearShiftMax() != 0) {
      result += ", " + getYearShiftMax() + " years";
    }
    if (getMonthShiftMax() != 0) {
      result += ", " + getMonthShiftMax() + " months";
    }
    if (getDayShiftMax() != 0) {
      result += ", " + getDayShiftMax() + " days";
    }
    if (hasMaxShiftBoundary) {
      if (cal.getTimeInMillis() < tenYear) {
        result += " earlier";
      } else {
        result += " later";
      }
    }
    result += "]";
    return result;
  }

  public String getRefElemID() {
    return refElemID;
  }

  public void setRefElemID(String refElemID) {
    this.refElemID = refElemID;
  }

  public QueryAttribute getRefElem() {
    return refElem;
  }

  public boolean hasMinBoundary() {
    return hasMinShiftBoundary;
  }

  public boolean hasMaxBoundary() {
    return hasMaxShiftBoundary;
  }

  public void setRefElem(QueryAttribute refElem) {
    this.refElem = refElem;
    setRefElemID(refElem.getId());
  }

  @Override
  public <T> T accept(IQueryElementVisitor<T> visitor) throws QueryException {
    return visitor.visit(this);
  }

  public int getDayShiftMin() {
    return dayShiftMin;
  }

  public void setDayShiftMin(int dayShiftMin) {
    if (dayShiftMin == Integer.MAX_VALUE)
      return;
    this.dayShiftMin = dayShiftMin;
    hasMinShiftBoundary = true;
  }

  public int getMonthShiftMin() {
    return monthShiftMin;
  }

  public void setMonthShiftMin(int monthShiftMin) {
    if (monthShiftMin == Integer.MAX_VALUE)
      return;
    this.monthShiftMin = monthShiftMin;
    hasMinShiftBoundary = true;
  }

  public int getYearShiftMin() {
    return yearShiftMin;
  }

  public void setYearShiftMin(int yearShiftMin) {
    if (yearShiftMin == Integer.MAX_VALUE)
      return;
    this.yearShiftMin = yearShiftMin;
    hasMinShiftBoundary = true;
  }

  public int getDayShiftMax() {
    return dayShiftMax;
  }

  public void setDayShiftMax(int dayShiftMax) {
    if (dayShiftMax == Integer.MIN_VALUE)
      return;
    this.dayShiftMax = dayShiftMax;
    hasMaxShiftBoundary = true;
  }

  public int getMonthShiftMax() {
    return monthShiftMax;
  }

  public void setMonthShiftMax(int monthShiftMax) {
    if (monthShiftMax == Integer.MIN_VALUE)
      return;
    this.monthShiftMax = monthShiftMax;
    hasMaxShiftBoundary = true;
  }

  public int getYearShiftMax() {
    return yearShiftMax;
  }

  public void setYearShiftMax(int yearShiftMax) {
    if (yearShiftMax == Integer.MIN_VALUE)
      return;
    this.yearShiftMax = yearShiftMax;
    hasMaxShiftBoundary = true;
  }

}
