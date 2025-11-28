package de.uniwue.dw.exchangeFormat;

import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.data.Information;
import org.apache.commons.csv.CSVRecord;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public class EFFactDataRow implements EFDateTimeFormat, EFMultipleValuesFormat, EFNumberFormat {

  private static String combinedFactIDSeparator = ":";

  private String valueTrueString = "x";

  private String valueNotSetString = "<#NOT_SET#>";

  protected static String idColumnName = "ID";

  private String id1;

  private String id2;

  private String id3;

  private Map<String, List<EFSingleValue>> stringIDToValuesMap;

  private EFFactMetaDataCSVProcessor factMetaDataCSVProcessor;

  public EFFactDataRow(Information aInfo, String aStringID, CatalogEntryType aType) {
    id1 = Long.toString(aInfo.getPid());
    id2 = Long.toString(aInfo.getCaseID());
    id3 = Long.toString(aInfo.getDocID());
    stringIDToValuesMap = new HashMap<String, List<EFSingleValue>>();
    addInfoToStringID(aInfo, aStringID, aType);
  }

  public EFFactDataRow(CSVRecord aRecord, EFFactMetaDataCSVProcessor aFactMetaDataCSVProcessor)
          throws SQLException {
    this.factMetaDataCSVProcessor = aFactMetaDataCSVProcessor;
    Map<String, String> recordAsMap = aRecord.toMap();
    Iterator<Entry<String, String>> valuesIterator = recordAsMap.entrySet().iterator();
    while (valuesIterator.hasNext()) {
      Entry<String, String> curEntry = valuesIterator.next();
      if (curEntry.getValue().trim().isEmpty()) {
        valuesIterator.remove();
      }
    }

    id1 = getValueFromMapIfExistsAndDeleteItFromMap(recordAsMap, idColumnName + 1);
    id2 = getValueFromMapIfExistsAndDeleteItFromMap(recordAsMap, idColumnName + 2);
    id3 = getValueFromMapIfExistsAndDeleteItFromMap(recordAsMap, idColumnName + 3);
    checkForValidIDs();
    stringIDToValuesMap = new HashMap<String, List<EFSingleValue>>();
    for (String aKey : recordAsMap.keySet()) {
      stringIDToValuesMap.put(aKey, getValuesFromString(recordAsMap.get(aKey)));
    }
  }

  private String getValueFromMapIfExistsAndDeleteItFromMap(Map<String, String> aMap, String aKey) {
    String returnValue = null;
    if (aMap.containsKey(aKey)) {
      returnValue = aMap.get(aKey);
      aMap.remove(aKey);
    }
    return returnValue;
  }

  private void checkForValidIDs() throws SQLException {
    String baseColumnHeader = factMetaDataCSVProcessor.getBaseColumnHeader();
    if (baseColumnHeader != null) {
      String baseID = null;
      if (baseColumnHeader.equals(idColumnName + 1)) {
        baseID = id1;
      } else if (baseColumnHeader.equals(idColumnName + 2)) {
        baseID = id2;
      } else if (baseColumnHeader.equals(idColumnName + 3)) {
        baseID = id3;
      }
      Map<String, String> headersToValuesMap = factMetaDataCSVProcessor
              .getMappedColumnHeadersToValuesMap(baseID);
      for (String key : headersToValuesMap.keySet()) {
        if (key.equals(idColumnName + 1)) {
          id1 = headersToValuesMap.get(key);
        } else if (key.equals(idColumnName + 2)) {
          id2 = headersToValuesMap.get(key);
        } else if (key.equals(idColumnName + 3)) {
          id3 = headersToValuesMap.get(key);
        }
      }
    }
    if (id1 == null) {
      id1 = Long.toString(factMetaDataCSVProcessor.getNextUsablePID());
      if (id2 == null) {
        id2 = id1;
      }
    }
  }

  public void addInformation(Information aInfo, String aStringID, CatalogEntryType aType) {
    addInfoToStringID(aInfo, aStringID, aType);
  }

  private void addInfoToStringID(Information anInfo, String aStringID, CatalogEntryType aType) {
    String valueString = anInfo.getValue();
    if (aType.equals(CatalogEntryType.Number)) {
      if (anInfo.getValueDec() != null) {
        valueString = Double.toString(anInfo.getValueDec()).replaceAll("\\.", ",");
      }
    } else if (aType.equals(CatalogEntryType.Bool)) {
      valueString = valueTrueString;
    } else if (valueString == null || valueString.isEmpty()) {
      valueString = valueNotSetString;
    }
    EFSingleValue newValue = new EFSingleValue(valueString);
    if (anInfo.getMeasureTime() != null) {
      newValue.addMetaData(defaultFormat.format(anInfo.getMeasureTime()));
    }
    if (stringIDToValuesMap.containsKey(aStringID)) {
      stringIDToValuesMap.get(aStringID).add(newValue);
    } else {
      List<EFSingleValue> newValuesList = new ArrayList<EFSingleValue>();
      newValuesList.add(newValue);
      stringIDToValuesMap.put(aStringID, newValuesList);
    }
  }

  public static String buildCombinedFactID(Information aInfo) {
    return aInfo.getPid() + combinedFactIDSeparator + aInfo.getCaseID() + combinedFactIDSeparator
            + aInfo.getDocID();
  }

  public List<Object> transformToObjectList(String[] headers) {
    List<Object> resultList = new ArrayList<Object>();
    for (String curHeader : headers) {
      if (curHeader.equals(idColumnName + 1)) {
        resultList.add(id1);
      } else if (curHeader.equals(idColumnName + 2)) {
        resultList.add(id2);
      } else if (curHeader.equals(idColumnName + 3)) {
        resultList.add(id3);
      } else if (stringIDToValuesMap.containsKey(curHeader)) {
        resultList.add(buildStringFromEFValues(stringIDToValuesMap.get(curHeader)));
      } else {
        resultList.add(null);
      }
    }
    return resultList;
  }

  public String getId1() {
    return getIdOrZeroIfNotExists(id1);
  }

  public String getId2() {
    return getIdOrZeroIfNotExists(id2);
  }

  public String getId3() {
    return getIdOrZeroIfNotExists(id3);
  }

  private String getIdOrZeroIfNotExists(String aID) {
    return aID != null && !aID.trim().isEmpty() ? aID.trim() : Long.toString(0);
  }

  public Set<String> getStringIDs() {
    return stringIDToValuesMap.keySet();
  }

  public List<EFSingleValue> getValuesForStringID(String aStringID) {
    return stringIDToValuesMap.get(aStringID);
  }

  public Timestamp getMeasureTimeFromMetaDataString(String aMetaDataString,
          SimpleDateFormat additionalPossibleFormat) throws ParseException {
    Date resultDate = parseDateString(aMetaDataString, additionalPossibleFormat);
    if (resultDate != null) {
      return new Timestamp(resultDate.getTime());
    } else {
      return null;
    }
  }

  public String getValueToUse(String aValue) {
    String returnValue = aValue.trim();
    if (returnValue.equals(valueTrueString) || returnValue.equals(valueNotSetString)) {
      return null;
    } else {
      return returnValue;
    }
  }

  public double getDoubleValueToUse(String aValue) {
    try {
      Double resultValue = parseNumberString(aValue);
      return resultValue;
    } catch (Exception e) {
      return 0.0;
    }
  }

  public CatalogEntryType generateCatalogEntryTypeFromValue(String aValue,
          SimpleDateFormat additionalPossibleDateFormat) {
    if (aValue == null) {
      return CatalogEntryType.Bool;
    }
    try {
      parseDateString(aValue, additionalPossibleDateFormat);
      return CatalogEntryType.DateTime;
    } catch (Exception e1) {
      try {
        parseNumberString(aValue);
        return CatalogEntryType.Number;
      } catch (Exception e2) {
        return CatalogEntryType.Text;
      }
    }
  }

}
