package de.uniwue.dw.exchangeFormat;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVRecord;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.model.manager.InfoManager;

public class EFFactMetaDataCSVProcessor extends EFAbstractCSVProcessor {

  private String baseColumnHeader;

  private List<String> mappedColumnHeaders;

  private Map<String, List<String>> baseIDToMappedIDsMap;

  private InfoManager infoManager;

  private long nextUsablePID = -1;

  private long nextUsableCaseID = -1;

  private long nextUsableDocID = -1;

  public EFFactMetaDataCSVProcessor(String csvStringToParse) throws SQLException, IOException {
    super(csvStringToParse);
    loadCSV(csvStringToParse != null && !csvStringToParse.isEmpty());
  }

  public EFFactMetaDataCSVProcessor(File csvFileToUse) throws SQLException, IOException {
    super(csvFileToUse);
    loadCSV(csvFileToUse != null && csvFileToUse.exists());
  }

  public void loadCSV(boolean hasContent) throws IOException, SQLException {
    infoManager = DwClientConfiguration.getInstance().getInfoManager();
    mappedColumnHeaders = new ArrayList<String>();
    baseIDToMappedIDsMap = new HashMap<String, List<String>>();

    if (hasContent) {
      Map<String, Integer> csvHeaderToPositionMap = getCSVHeaderToPositionMap();
      for (String aKey : csvHeaderToPositionMap.keySet()) {
        if (csvHeaderToPositionMap.get(aKey) == 0) {
          baseColumnHeader = aKey;
        } else {
          mappedColumnHeaders.add(aKey);
        }
      }
      for (CSVRecord curRecord : getCSVRecords()) {
        String baseID = curRecord.get(baseColumnHeader);
        List<String> mappedIDs = new ArrayList<String>();
        for (String curColumn : mappedColumnHeaders) {
          if (checkCSVContentIsSet(curRecord, curColumn)) {
            mappedIDs.add(curRecord.get(curColumn));
          } else {
            mappedIDs.add(Long.toString(0));
          }
        }
        baseIDToMappedIDsMap.put(baseID, mappedIDs);
      }
    }
  }

  public String getBaseColumnHeader() {
    return baseColumnHeader;
  }

  public Map<String, String> getMappedColumnHeadersToValuesMap(String baseID) {
    Map<String, String> returnMap = new HashMap<String, String>();
    if (baseID != null && !baseID.trim().isEmpty()) {
      List<String> valuesList = baseIDToMappedIDsMap.get(baseID);
      if (valuesList != null && !valuesList.isEmpty()) {
        for (int i = 0; i < mappedColumnHeaders.size(); i++) {
          returnMap.put(mappedColumnHeaders.get(i), valuesList.get(i));
        }
      }
    }
    return returnMap;
  }

  public long getNextUsablePID() throws SQLException {
    if (nextUsablePID < 0) {
      nextUsablePID = infoManager.getMaxPID() + 1;
    }
    return nextUsablePID++;
  }

  public long getNextUsableCaseID() throws SQLException {
    if (nextUsableCaseID < 0) {
      nextUsableCaseID = infoManager.getMaxCaseID() + 1;
    }
    return nextUsableCaseID++;
  }

  public long getNextUsableDocID() throws SQLException {
    if (nextUsableDocID < 0) {
      nextUsableDocID = infoManager.getMaxRef() + 1;
    }
    return nextUsableDocID++;
  }

}
