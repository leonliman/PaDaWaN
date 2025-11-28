package de.uniwue.dw.query.model.quickSearch.suggest;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.query.model.lang.ContentOperator;
import de.uniwue.dw.query.model.quickSearch.AttributeDuringTipping;

import java.text.DateFormat;
import java.util.Date;

public class SuggestObject {

  // The speaking name of the catalog entry
  private String name;

  // Attribut ID of the catalog entry
  private int attrId;

  // The string, which will be shown in the suggestion list.
  private String suggestText;

  // The dataType of the catalog entry
  private CatalogEntryType dataType;

  // The absolute amount of information instances for this entry in the database
  private long countAbsolute;

  // The amount of information instances for this entry in the database distincted on PIDs
  private long countDistinctPID;

  // The amount of information instances for this entry in the database distincted on Cases
  private long countDistinctCaseID;

  private boolean isCatalogEntry;

  // The position in the given input text, where the suggestion starts
  private int positioInInput = 0;

  // Operator for the given suggestion
  private String operator;

  // Value for the for the given parent_shell
  private String operatorValue;

  private int docId = 0;

  private boolean displayDocId = false;

  private boolean displayCaseId = false;

  private boolean displayInfoDate = false;

  public boolean isDisplayInfoDate() {
    return displayInfoDate;
  }

  public void setDisplayInfoDate(boolean displayInfoDate) {
    this.displayInfoDate = displayInfoDate;
  }

  public boolean isDisplayCaseId() {
    return displayCaseId;
  }

  public void setDisplayCaseId(boolean displayCaseId) {
    this.displayCaseId = displayCaseId;
  }

  public boolean isDisplayDocId() {
    return displayDocId;
  }

  public void setDisplayDocId(boolean displayDocId) {
    this.displayDocId = displayDocId;
  }

  public int getDocId() {
    return docId;
  }

  public void setDocId(int docId) {
    this.docId = docId;
  }

  public boolean isCatalogEntry() {
    return isCatalogEntry;
  }

  public void setIsCatalogEntry(boolean isCatalogEntry) {
    this.isCatalogEntry = isCatalogEntry;
  }

  public void setCatalogEntry(boolean isCatalogEntry) {
    this.isCatalogEntry = isCatalogEntry;
  }

  public SuggestObject() {
    super();
  }

  public SuggestObject(String suggestionText, CatalogEntry catalogEntry) {
    this(suggestionText, catalogEntry.getName(), catalogEntry.getDataType(),
            catalogEntry.getCountAbsolute(), catalogEntry.getCountDistinctPID(),
            catalogEntry.getCountDistinctCaseID(), true, 0, null, null, catalogEntry.getAttrId());
  }

  public SuggestObject(String suggestText, CatalogEntry catalogEntry,
          AttributeDuringTipping attribut) {
    this(suggestText, catalogEntry.getName(), catalogEntry.getDataType(),
            catalogEntry.getCountAbsolute(), catalogEntry.getCountDistinctPID(),
            catalogEntry.getCountDistinctCaseID(), true, 0, attribut.getNumericOperator(),
            attribut.getBound(), catalogEntry.getAttrId());
  }

  public SuggestObject(String suggestText, String name, CatalogEntryType dataType,
          long countAbsolute, long countDistinctPID, long countDistinctCaseID,
          boolean isCatalogEntry, String operator, String operatorValue, int attrId) {
    this(suggestText, name, dataType, countAbsolute, countDistinctPID, countDistinctCaseID,
            isCatalogEntry, 0, operator, operatorValue, attrId);
  }

  public SuggestObject(String suggestText, String name, CatalogEntryType dataType,
          long countAbsolute, long countDistinctPID, long countDistinctCaseID,
          boolean isCatalogEntry, int positioInInput, String operator, String operatorValue,
          int attrId) {
    super();
    this.suggestText = suggestText;
    this.name = name;
    this.attrId = attrId;
    this.dataType = dataType;
    this.countAbsolute = countAbsolute;
    this.countDistinctPID = countDistinctPID;
    this.countDistinctCaseID = countDistinctCaseID;
    this.isCatalogEntry = isCatalogEntry;
    this.positioInInput = positioInInput;
    this.operator = operator;
    this.operatorValue = operatorValue;
  }

  public SuggestObject(String name, boolean isCatalogEntry) {
    super();
    this.name = name;
    this.suggestText = name;
    this.isCatalogEntry = isCatalogEntry;
  }

  public int getAttrId() {
    return attrId;
  }

  public void setAttrId(int attrId) {
    this.attrId = attrId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public CatalogEntryType getDataType() {
    return dataType;
  }

  public void setDataType(CatalogEntryType dataType) {
    this.dataType = dataType;
  }

  public long getCountAbsolute() {
    return countAbsolute;
  }

  public void setCountAbsolute(long countAbsolute) {
    this.countAbsolute = countAbsolute;
  }

  public long getCountDistinctPID() {
    return countDistinctPID;
  }

  public void setCountDistinctPID(long countDistinctPID) {
    this.countDistinctPID = countDistinctPID;
  }

  public long getCountDistinctCaseID() {
    return countDistinctCaseID;
  }

  public void setCountDistinctCaseID(long countDistinctCaseID) {
    this.countDistinctCaseID = countDistinctCaseID;
  }

  public String getSuggestText() {
    return suggestText;
  }

  public void setSuggestText(String suggestText) {
    this.suggestText = suggestText;
  }

  public int getPositioInInput() {
    return positioInInput;
  }

  public void setPositioInInput(int positioInInput) {
    this.positioInInput = positioInInput;
  }

  public String getOperator() {
    return operator;
  }

  public void setOperator(String operator) {
    this.operator = operator;
  }

  public String getOperatorValue() {
    return operatorValue;
  }

  public void setOperatorValue(String operatorValue) {
    this.operatorValue = operatorValue;
  }

  /*
   * Generates Default Operator Values
   */
  public void setDefaults() {
    DateFormat df = DateFormat.getDateInstance(DateFormat.MEDIUM);
    switch (getDataType()) {
      case DateTime:
        setOperator(ContentOperator.LESS.name());
        setOperatorValue(df.format(new Date()));
        setSuggestText(getSuggestText() + " < " + df.format(new Date()));
        break;
      case Text:
        setOperator(ContentOperator.CONTAINS.name());
        break;
      case Number:
        setOperator(ContentOperator.LESS.name());
        setOperatorValue("10");
        setSuggestText(getSuggestText() + " < 10");
        break;
      case SingleChoice:
      case Structure:
      case isA:
      case Bool:
        break;
      default:
        break;
    }

  }
}
