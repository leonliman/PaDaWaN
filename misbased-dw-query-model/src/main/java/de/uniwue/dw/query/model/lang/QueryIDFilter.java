package de.uniwue.dw.query.model.lang;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.manager.QueryManipulationManager;
import de.uniwue.misc.util.TimeUtil;

public class QueryIDFilter extends QueryAnd {

  private HashSet<Long> ids = new HashSet<Long>();

  private Map<Long, HashSet<Date>> restrictedTimes = new HashMap<Long, HashSet<Date>>();

  private boolean restrictingTimesAreDates = true;

  private FilterIDType filterIDType = FilterIDType.PID;

  // should the final query be wrapped in an additional distinct ?
  private boolean distinct = false;

  public enum FilterIDType {
    PID, CaseID, DocID, Year, GROUP
  }

  public QueryIDFilter(QueryStructureContainingElem aContainer) {
    super(aContainer);
  }

  public QueryIDFilter(QueryStructureContainingElem aContainer, int position) {
    super(aContainer, position);
  }

  public boolean somePIDsAppearMultipleTimesInFilter() {
    boolean result = false;
    if (hasTimeRestrictions()) {
      for (HashSet<Date> aDateSet : getRestrictedTimes().values()) {
        if (aDateSet.size() > 1) {
          result = true;
        }
      }
    }
    return result;
  }

  public void addID(Long anID) {
    if (!getIds().contains(anID)) {
      getIds().add(anID);
    }
  }

  public boolean hasTimeRestrictions() {
    return hasGivenTimeRestrictions() || (getFilterIDType() == FilterIDType.Year);
  }

  public boolean hasGivenTimeRestrictions() {
    return !getRestrictedTimes().isEmpty();
  }

  public boolean hasIDRestrictions() {
    return !getIds().isEmpty();
  }

  public boolean isDistinct() {
    return distinct;
  }

  @Override
  public void generateXMLAttributes(XMLStreamWriter writer) throws XMLStreamException {
    super.generateXMLAttributes(writer);
    writer.writeAttribute("filterIDType", getFilterIDType().toString());
    if (isDistinct()) {
      writer.writeAttribute("distinct", Boolean.toString(isDistinct()));
    }
  }

  @Override
  public void generateXML(XMLStreamWriter writer) throws QueryException {
    try {
      writer.writeStartElement(getXMLName());
      generateXMLAttributes(writer);
      for (Long aPid : getIds()) {
        if (getRestrictedTimes().containsKey(aPid)) {
          HashSet<Date> dates = getRestrictedTimes().get(aPid);
          for (Date aDate : dates) {
            writer.writeStartElement("ID");
            writer.writeAttribute("value", Long.toString(aPid));
            writer.writeAttribute("time", TimeUtil.getSdfWithTime().format(aDate));
            writer.writeEndElement();
          }
        } else {
          writer.writeStartElement("ID");
          writer.writeAttribute("value", Long.toString(aPid));
          writer.writeEndElement();
        }
      }
      for (QueryStructureElem anAttr : getChildren()) {
        anAttr.generateXML(writer);
      }
      writer.writeEndElement();
    } catch (XMLStreamException e) {
      throw new QueryException(e);
    }
  }

  public void addTime(Long id, Date time) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(time);
    int hours = calendar.get(Calendar.HOUR_OF_DAY);
    int minutes = calendar.get(Calendar.MINUTE);
    int seconds = calendar.get(Calendar.SECOND);
    if ((seconds != 0) || (minutes != 0) || (hours != 0)) {
      setRestrictingTimesAreDates(false);
    }
    HashSet<Date> dates = getRestrictedTimes().get(id);
    if (dates == null) {
      dates = new HashSet<Date>();
      getRestrictedTimes().put(id, dates);
    }
    dates.add(time);
  }

  public void addTimeString(Long id, String timeString) throws QueryException {
    try {
      Date date = null;
      if (timeString.matches(TimeUtil.sdf_withoutTimeStringRegex)) {
        date = TimeUtil.getSdfWithoutTime().parse(timeString);
      } else if (timeString.matches(TimeUtil.sdf_withTimeStringRegex)) {
        date = TimeUtil.getSdfWithTime().parse(timeString);
      } else {
        throw new QueryException("date not in correct format.");
      }
      if (date != null) {
        addTime(id, date);
      }
    } catch (ParseException e) {
      throw new QueryException("date not in correct format.");
    }
  }

  @Override
  public List<QueryStructureElem> getReducingElemsRecursive() {
    List<QueryStructureElem> result = super.getReducingElemsRecursive();
    if (isDistinct()) {
      result.add(this);
    }
    return result;
  }

  @Override
  public List<QueryIDFilter> getIDFilterRecursive() {
    List<QueryIDFilter> result = super.getIDFilterRecursive();
    result.add(this);
    return result;
  }

  @Override
  public String getXMLName() {
    return "IDFilter";
  }

  @Override
  public <T> T accept(IQueryElementVisitor<T> visitor) throws QueryException {
    return visitor.visit(this);
  }

  public HashSet<Long> getIds() {
    return ids;
  }

  public void setIds(HashSet<Long> ids) {
    this.ids = ids;
  }

  public Map<Long, HashSet<Date>> getRestrictedTimes() {
    return restrictedTimes;
  }

  public void setRestrictedTimes(Map<Long, HashSet<Date>> restrictedTimes) {
    this.restrictedTimes = restrictedTimes;
  }

  public boolean isRestrictingTimesAreDates() {
    return restrictingTimesAreDates;
  }

  public void setRestrictingTimesAreDates(boolean restrictingTimesAreDates) {
    this.restrictingTimesAreDates = restrictingTimesAreDates;
  }

  public FilterIDType getFilterIDType() {
    return filterIDType;
  }

  public void setFilterIDType(FilterIDType filterIDType) {
    this.filterIDType = filterIDType;
  }

  public void setDistinct(boolean distinct) {
    this.distinct = distinct;
  }

  @Override
  public void shrink(QueryManipulationManager manager) throws QueryException {
    super.shrink(manager);
    // better don't do the optimization for subclasses of IDFilters
    if (getClass() == QueryIDFilter.class) {
      if ((getContainer() != null) && (getContainer() instanceof QueryIDFilter)
              && (((QueryIDFilter) getContainer()).getFilterIDType() == getFilterIDType())
              && getFilterIDType() != FilterIDType.GROUP) {
        for (QueryStructureElem aChild : getChildren()) {
          manager.moveElem(aChild, getContainer());
        }
        manager.deleteElement(this);
        return;
      }
      if (getChildren().size() == 0) {
        getContainer().removeChild(this);
      }
      if (ancestorIsQueryStatisticElem()) {
        // in statistical queries IDFilters are only supported as childs of root.
        // they are converted to QueryAnds
        if (getFilterIDType() != FilterIDType.GROUP) {
          QueryAnd queryAnd = new QueryAnd(getContainer());
          for (QueryStructureElem aChild : getChildren()) {
            manager.moveElem(aChild, queryAnd);
          }
          manager.deleteElement(this);
        }
      }
    }
  }

  private boolean ancestorIsQueryStatisticElem() {
    Class[] classes = { QueryStatisticColumn.class, QueryStatisticFilter.class,
        QueryStatisticRow.class };
    HashSet<Class> queryStatisticElemClasses = new HashSet<>(Arrays.asList(classes));
    for (QueryElem elem : getAllParent()) {
      if (queryStatisticElemClasses.contains(elem.getClass()))
        return true;
    }
    return false;
  }

}
