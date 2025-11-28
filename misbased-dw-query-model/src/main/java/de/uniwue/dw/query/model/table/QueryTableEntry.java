package de.uniwue.dw.query.model.table;

import java.util.ArrayList;
import java.util.List;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.query.model.data.RawQuery;
import de.uniwue.dw.query.model.lang.ContentOperator;
import de.uniwue.dw.query.model.lang.ExtractionMode;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.ReductionOperator;
 
public class QueryTableEntry {

  private CatalogEntry entry;

  public RawQuery rawQuery;

  // private String name = "";
  private ContentOperator operator = ContentOperator.EXISTS;

  private String argument = "";

  private String orGroup = "";

  private ReductionOperator aggregateFuction = ReductionOperator.NONE;

  public String alias = "";

  private String group = "";

  private String caseGroup = "";

  private String docGroup = "";

  private GroupType groupType = GroupType.none;

  private Position position = Position.row;

  private boolean isFilter;

  private boolean isOptional = false;

  private boolean isActive = true;

  private boolean isValueInFile = false;

  private boolean addMeasureTime = false;

  private boolean addCaseID = false;

  private boolean addDocID = false;

  private boolean multRows = false;

  private boolean onlyDisplayExistance = false;

  private boolean displayColumn = true;

  private ExtractionMode extractionMode = ExtractionMode.None;

  public static enum GroupType {

    none(""), or("Oder"), and("Und"), orWithPowerSet("Oder mit Teilmengen"), andWithPowerSet(
            "Und mit Teilmengen");

    private String text;

    private GroupType(String text) {
      this.text = text;
    }

    public GroupType next() {
      GroupType[] values = GroupType.values();
      return values[(this.ordinal() + 1) % values.length];
    }

    public String getText() {
      return text;
    }

    public static GroupType parse(String s) {
      for (GroupType op : GroupType.values()) {
        if (s.equalsIgnoreCase(op.toString()) || s.equalsIgnoreCase(op.getText()))
          return op;
      }
      throw new IllegalArgumentException("GroupType unknown for arguement: " + s);
    }
  }

  public static enum Position {

    row("Zeile"), column("Spalte"), filter("Filter");

    private String text;

    private Position(String text) {
      this.text = text;
    }

    public Position next() {
      Position[] values = Position.values();
      return values[(this.ordinal() + 1) % values.length];
    }

    public String getText() {
      return text;
    }

    public static Position parse(String s) {
      for (Position op : Position.values()) {
        if (s.equalsIgnoreCase(op.toString()) || s.equalsIgnoreCase(op.getText()))
          return op;
      }
      throw new IllegalArgumentException("Position unknown for arguement: " + s);
    }
  }

  public QueryTableEntry(RawQuery aRawQuery) {
    rawQuery = aRawQuery;

  }

  public QueryTableEntry(CatalogEntry entry, String operator, String argument, String orGroup,
          String aggregateFunction, String alias, String optional) {
    this(entry, operator, argument);
    setOrGroup(orGroup);
    setAggregateFuction(aggregateFunction);
    setAlias(alias);
    setOptional(optional);
  }

  public QueryTableEntry(CatalogEntry entry, String operator, String argument, String gruop,
          String groupType, String position) {
    this(entry, operator, argument);
    setGroup(gruop);
    setGroupType(groupType);
    setPosition(position);
  }

  private QueryTableEntry(CatalogEntry entry, String operator, String argument) {
    initialize(entry, null);
    setOperator(operator);
    setArgument(argument);
  }

  public QueryTableEntry(CatalogEntry entry, String anAlias) {
    initialize(entry, anAlias);
  }

  public QueryTableEntry(QueryAttribute a) {
    initialize(a.getCatalogEntry(), a.getId());
    alias = a.getId();
    argument = a.getDesiredContent();
    if (argument == null) {
      argument = "";
    }
    operator = a.getContentOperator();
    if (a.isOptional()) {
      isOptional = true;
    }
    aggregateFuction = a.getReductionOperator();
    extractionMode = a.getExtractionMode();
    displayColumn = a.displayValue();
    addDocID = a.displayDocID();
    addCaseID = a.displayCaseID();
    addMeasureTime = a.displayInfoDate();
    multRows = a.isMultipleRows();
  }

  private void initialize(CatalogEntry anEntry, String anAlias) {
    entry = anEntry;
    if (entry.getDataType() == CatalogEntryType.Number
            || entry.getDataType() == CatalogEntryType.Text) {
      aggregateFuction = ReductionOperator.NONE;
    }
    alias = anAlias;
  }

  public String getName() {
    if (entry != null) {
      return entry.getName();
    } else if (rawQuery != null) {
      return rawQuery.getName();
    } else {
      return "";
    }
  }

  public ContentOperator getOperator() {
    return operator;
  }

  public String getArgument() {
    if (operator != null) {
      for (ContentOperator op : Operator.ALL_NUMERIC_OPERATORS_WITH_ARGUMENT) {
        if (op == operator)
          return argument.replaceAll(",", ".");
      }
    }
    return argument;
  }

  public String getOrGroup() {
    return this.orGroup;
  }

  public ReductionOperator getAggregateFuction() {
    return this.aggregateFuction;
  }

  public CatalogEntry getCatalogEntry() {
    return entry;
  }

  public void setOperator(String string) {
    setOperator(ContentOperator.parse(string));
  }

  public void setOperator(ContentOperator contentOperator) {
    operator = contentOperator;
  }

  public void setArgument(String string) {
    argument = string;
  }

  public void setOrGroup(String string) {
    orGroup = string;
  }

  public void setFilter(String string) {
    this.isFilter = string.equalsIgnoreCase("true");
  }

  public void setFilter(boolean isFilter) {
    this.isFilter = isFilter;
  }

  public void setAggregateFuction(ReductionOperator string) {
    aggregateFuction = string;
  }

  private void setAggregateFuction(String aggregateFunction) {
    setAggregateFuction(ReductionOperator.valueOf(aggregateFunction));
  }

  private void setAlias(String alias) {
    this.alias = alias;
  }

  public boolean isFilter() {
    return isFilter;
  }

  public void toogleFilter() {
    this.isFilter = !isFilter;
  }

  public void iterateGroupType() {
    groupType = groupType.next();
  }

  public String getGroup() {
    return group;
  }

  public String getCaseGroup() {
    return caseGroup;
  }

  public String getDocGroup() {
    return docGroup;
  }

  public void setCaseGroup(String caseGroup) {
    this.caseGroup = caseGroup;
  }

  public void setDocGroup(String docGroup) {
    this.docGroup = docGroup;
  }

  public void setGroup(String group) {
    if (this.group == null || this.group.isEmpty())
      iterateGroupType();
    if (group == null || group.isEmpty())
      groupType = GroupType.none;
    this.group = group;
  }

  public GroupType getGroupType() {
    return groupType;
  }

  public String getGroupTypeText() {
    return groupType.getText();
  }

  public Position getPosition() {
    return position;
  }

  public void iteratePosition() {
    position = position.next();
    if (position == Position.filter) {
      if (groupType == GroupType.andWithPowerSet)
        groupType = GroupType.and;
      if (groupType == GroupType.orWithPowerSet)
        groupType = GroupType.or;
    }
  }

  public void setGroupType(String groupType) {
    setGroupType(GroupType.parse(groupType));
  }

  public void setGroupType(GroupType groupType) {
    this.groupType = groupType;
  }

  private void setPosition(String position) {
    setPosition(Position.parse(position));
  }

  public void setPosition(Position position) {
    this.position = position;
  }

  public boolean isOptional() {
    return isOptional;
  }

  public boolean isActive() {
    return isActive;
  }

  public boolean isValueInFile() {
    return isValueInFile;
  }

  public boolean isAddMeasuerTime() {
    return addMeasureTime;
  }

  public boolean isAddCaseID() {
    return addCaseID;
  }

  public boolean isAddDocID() {
    return addDocID;
  }

  public boolean isMultRows() {
    return multRows;
  }

  public boolean isOnlyDisplayExistance() {
    return onlyDisplayExistance;
  }

  public boolean isDisplayColumn() {
    return displayColumn;
  }

  private void setOptional(String optional) {
    setOptional(Boolean.valueOf(optional));
  }

  public void setOptional(boolean optional) {
    this.isOptional = optional;
  }

  public void toogleOptional() {
    this.isOptional = !isOptional;
  }

  public void toogleActive() {
    this.isActive = !isActive;
  }

  public void toogleValueInFile() {
    this.isValueInFile = !isValueInFile;
  }

  public void toogleAddMeasureTime() {
    this.addMeasureTime = !addMeasureTime;
  }

  public void toogleAddCaseID() {
    this.addCaseID = !addCaseID;
  }

  public void toogleAddDocID() {
    this.addDocID = !addDocID;
  }

  public void toogleMultRows() {
    this.multRows = !multRows;
  }

  public void toogleOnlydisplayExistance() {
    this.onlyDisplayExistance = !onlyDisplayExistance;
  }

  public void toogleDisplayColumn() {
    this.displayColumn = !displayColumn;
  }

  public void setExtractionMode(ExtractionMode aMode) {
    this.extractionMode = aMode;
  }

  public ExtractionMode getExtractionMode() {
    return this.extractionMode;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    String delimiter = " ";
    sb.append(entry.getAttrId() + delimiter);
    sb.append(entry.getName() + delimiter);
    sb.append(operator + delimiter);
    sb.append(argument + delimiter);
    sb.append("or:" + orGroup + delimiter);
    sb.append("group:" + group + delimiter);
    sb.append("pos:" + position + delimiter);
    sb.append("aggr:" + aggregateFuction + delimiter);
    sb.append("opt:" + isOptional + delimiter);
    return sb.toString();
  }

  public List<String> toStatsticToolList() {
    List<String> values = new ArrayList<>();
    values.add(this.entry.getAttrId() + "");
    values.add(operator.getDisplayString());
    values.add(argument);
    values.add(group);
    values.add(groupType.getText());
    values.add(position.getText());
    values.add(entry.getDataType() + "");
    return values;
  }

}
