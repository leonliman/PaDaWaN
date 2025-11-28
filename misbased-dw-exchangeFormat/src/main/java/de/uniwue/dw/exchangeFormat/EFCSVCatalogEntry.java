package de.uniwue.dw.exchangeFormat;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVRecord;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;

public class EFCSVCatalogEntry extends CatalogEntry
        implements EFDateTimeFormat, EFNumberFormat, EFMultipleValuesFormat {

  public enum ValidHeaders {
    ID, Name, Description, Project, ExtID, ParentID, Type, DateTimeFormat, SingleChoiceChoices, LowBound, HighBound, Unit;

    public static String[] getNames() {
      ValidHeaders[] headers = values();
      String[] names = new String[headers.length];

      for (int i = 0; i < names.length; i++) {
        names[i] = headers[i].toString();
      }

      return names;
    }
  }

  private EFCSVCatalogEntry(int anAttrID, String aName, CatalogEntryType aDataType, String anExtID,
          int aParentID, double anOrderValue, String aProject, String aUniqueName,
          String aDescription, Timestamp aCreationTime) {
    super(anAttrID, aName, aDataType, anExtID, aParentID, anOrderValue, aProject, aUniqueName,
            aDescription, aCreationTime);
    if (aDataType == null) {
      setDataType(CatalogEntryType.Text);
      setHasToValidateType(true);
    }
  }

  public EFCSVCatalogEntry(CatalogEntry entry, CatalogEntry parentEntry) {
    this(entry, parentEntry, buildStringIDFromProjectAndExtID(entry.getProject(), entry.getExtID()),
            buildStringIDFromProjectAndExtID(parentEntry.getProject(), parentEntry.getExtID()),
            defaultFormat, entry.getDataType() == null);
  }

  public EFCSVCatalogEntry(CatalogEntry entry, CatalogEntry parentEntry, String aStringID,
          String aParentStringID, SimpleDateFormat aDateFormat, boolean hasToValidateType) {
    this(entry.getAttrId(), entry.getName(), entry.getDataType(), entry.getExtID(),
            entry.getParentID(), entry.getOrderValue(), entry.getProject(), entry.getUniqueName(),
            entry.getUniqueName(), entry.getCreationTime());
    for (String choice : entry.getSingleChoiceChoice()) {
      addSingleChoiceChoice(choice);
    }
    setLowBound(entry.getLowBound());
    setHighBound(entry.getHighBound());
    setUnit(entry.getUnit());
    setStringID(aStringID);
    setParentStringID(aParentStringID);
    setDateTimeFormat(aDateFormat);
    setHasToValidateType(hasToValidateType);
  }

  public EFCSVCatalogEntry(CSVRecord csvRecord, String defaultProject)
          throws NumberFormatException, ParseException {
    this(-1, null, null, null, 0, 0, null, null, null, null);
    setStringID(csvRecord.get(ValidHeaders.ID));
    if (EFAbstractCSVProcessor.checkCSVContentIsSet(csvRecord, ValidHeaders.Name.toString())) {
      setName(csvRecord.get(ValidHeaders.Name));
    } else {
      setName(getStringID());
    }
    if (EFAbstractCSVProcessor.checkCSVContentIsSet(csvRecord,
            ValidHeaders.Description.toString())) {
      setDescrption(csvRecord.get(ValidHeaders.Description));
    }
    if (EFAbstractCSVProcessor.checkCSVContentIsSet(csvRecord, ValidHeaders.Project.toString())) {
      setProject(csvRecord.get(ValidHeaders.Project));
    } else {
      setProject(defaultProject);
    }
    if (EFAbstractCSVProcessor.checkCSVContentIsSet(csvRecord, ValidHeaders.ExtID.toString())) {
      setExtID(csvRecord.get(ValidHeaders.ExtID));
    } else {
      setExtID(getStringID());
    }
    if (EFAbstractCSVProcessor.checkCSVContentIsSet(csvRecord, ValidHeaders.ParentID.toString())) {
      setParentStringID(csvRecord.get(ValidHeaders.ParentID));
    }
    if (EFAbstractCSVProcessor.checkCSVContentIsSet(csvRecord, ValidHeaders.Type.toString())) {
      setDataType(CatalogEntryType.parse(csvRecord.get(ValidHeaders.Type)));
      setHasToValidateType(false);
    }
    if (EFAbstractCSVProcessor.checkCSVContentIsSet(csvRecord,
            ValidHeaders.DateTimeFormat.toString())) {
      setDateTimeFormat(csvRecord.get(ValidHeaders.DateTimeFormat));
    }
    if (EFAbstractCSVProcessor.checkCSVContentIsSet(csvRecord,
            ValidHeaders.SingleChoiceChoices.toString())) {
      for (EFSingleValue singleValue : getValuesFromString(
              csvRecord.get(ValidHeaders.SingleChoiceChoices))) {
        addSingleChoiceChoice(singleValue.getValue());
      }
    }
    if (EFAbstractCSVProcessor.checkCSVContentIsSet(csvRecord, ValidHeaders.LowBound.toString())) {
      setLowBound(parseNumberString(csvRecord.get(ValidHeaders.LowBound)));
    }
    if (EFAbstractCSVProcessor.checkCSVContentIsSet(csvRecord, ValidHeaders.HighBound.toString())) {
      setHighBound(parseNumberString(csvRecord.get(ValidHeaders.HighBound)));
    }
    if (EFAbstractCSVProcessor.checkCSVContentIsSet(csvRecord, ValidHeaders.Unit.toString())) {
      setUnit(csvRecord.get(ValidHeaders.Unit));
    }
  }

  public static EFCSVCatalogEntry getMinimalEFCSVCatalogEntry(String aStringID) {
    return getMinimalEFCSVCatalogEntry(aStringID, null);
  }

  public static EFCSVCatalogEntry getMinimalEFCSVCatalogEntry(String aStringID, String aProject) {
    return getMinimalEFCSVCatalogEntry(aStringID, aProject, null);
  }

  public static EFCSVCatalogEntry getMinimalEFCSVCatalogEntry(String aStringID, String aProject,
          String aExtID) {
    EFCSVCatalogEntry resultEntry = new EFCSVCatalogEntry(-1, null, null, null, 0, 0, null, null,
            null, null);
    resultEntry.setStringID(aStringID);
    resultEntry.setName(resultEntry.getStringID());
    if (aProject != null) {
      resultEntry.setProject(aProject);
    } else {
      resultEntry.setProject("");
    }
    if (aExtID != null) {
      resultEntry.setExtID(aExtID);
    } else {
      resultEntry.setExtID(resultEntry.getStringID());
    }
    return resultEntry;
  }

  private static String idSeparator = ":";

  private String stringID;

  private String parentStringID = null;

  private SimpleDateFormat dateTimeFormat;

  private boolean hasToValidateType;

  public String getStringID() {
    return stringID;
  }

  public void setStringID(String stringID) {
    this.stringID = stringID;
  }

  public static String buildStringIDFromProjectAndExtID(String project, String extID) {
    String projectToWorkWith = project.trim();
    String extIDToWorkWith = extID.trim();
    if (extIDToWorkWith.isEmpty() && projectToWorkWith.isEmpty()) {
      return idSeparator;
    }
    return projectToWorkWith.isEmpty() ? extIDToWorkWith
            : projectToWorkWith + idSeparator + extIDToWorkWith;
  }

  public String getParentStringID() {
    return parentStringID;
  }

  public void setParentStringID(String parentStringID) {
    this.parentStringID = parentStringID;
  }

  public SimpleDateFormat getDateTimeFormat() {
    return dateTimeFormat;
  }

  public String getDateTimeFormatAsString() {
    return dateTimeFormat != null ? dateTimeFormat.toPattern() : null;
  }

  public void setDateTimeFormat(String dateTimeFormatAsString) {
    this.dateTimeFormat = new SimpleDateFormat(dateTimeFormatAsString);
  }

  public void setDateTimeFormat(SimpleDateFormat dateTimeFormat) {
    this.dateTimeFormat = dateTimeFormat;
  }

  public boolean hasToValidateType() {
    return hasToValidateType;
  }

  public void setHasToValidateType(boolean hasToValidateType) {
    this.hasToValidateType = hasToValidateType;
  }

  public String getSingleChoiceChoicesAsString() {
    List<EFSingleValue> choiceValues = new ArrayList<EFSingleValue>();
    for (String choice : getSingleChoiceChoice()) {
      choiceValues.add(new EFSingleValue(choice));
    }
    return buildStringFromEFValues(choiceValues);
  }

  public String getLowBoundAsString() {
    return Double.toString(getLowBound()).replaceAll("\\.", ",");
  }

  public String getHighBoundAsString() {
    return Double.toString(getHighBound()).replaceAll("\\.", ",");
  }

  @Override
  public String toString() {
    return super.toString() + "; ID: " + getStringID() + "; ParentID: " + getParentStringID()
            + "; DateTimeFormat: " + getDateTimeFormatAsString();
  }

}