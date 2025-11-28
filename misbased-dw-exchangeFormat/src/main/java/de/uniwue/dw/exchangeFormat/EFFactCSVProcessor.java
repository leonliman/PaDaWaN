package de.uniwue.dw.exchangeFormat;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.model.manager.DWIterException;
import de.uniwue.dw.core.model.manager.InfoIterator;
import de.uniwue.dw.core.model.manager.InfoManager;
import de.uniwue.dw.exchangeFormat.EFMultipleValuesFormat.EFSingleValue;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.*;

public class EFFactCSVProcessor extends EFAbstractCSVProcessor {

  private CatalogManager catalogManager;

  private InfoManager infoManager;

  private Map<Integer, String> attrIDToStringIDMap;

  private Map<String, Long> stringInfoIDToNumericInfoIDMap;

  public EFFactCSVProcessor(String csvStringToParse) throws SQLException {
    super(csvStringToParse);
    initialize();
  }

  public EFFactCSVProcessor(File csvFileToUse) throws SQLException {
    super(csvFileToUse);
    initialize();
  }

  private void initialize() throws SQLException {
    catalogManager = DwClientConfiguration.getInstance().getCatalogManager();
    infoManager = DwClientConfiguration.getInstance().getInfoManager();
    attrIDToStringIDMap = new HashMap<>();
    stringInfoIDToNumericInfoIDMap = new HashMap<>();
  }

  public void importFactsFromCSV(boolean withResultPrintedToConsole,
          EFCatalogCSVProcessor catalogProcessor, EFFactMetaDataCSVProcessor factMetaProcessor)
          throws IOException, SQLException, ParseException {
    infoManager.deleteAllInfos();
    int csvRowCounter = 0;
    int importCounter = 0;
    for (CSVRecord curRecord : getCSVRecords()) {
      csvRowCounter++;
      EFFactDataRow curRow = new EFFactDataRow(curRecord, factMetaProcessor);
      long zero = 0L;
      long pID = getValidNumericID(curRow.getId1(), "PID", null, zero, zero, catalogProcessor,
              factMetaProcessor);
      long caseID = getValidNumericID(curRow.getId2(), "CaseID", pID, null, zero, catalogProcessor,
              factMetaProcessor);
      long docID = getValidNumericID(curRow.getId3(), "DocID", pID, caseID, null, catalogProcessor,
              factMetaProcessor);
      for (String curStringID : curRow.getStringIDs()) {
        EFCSVCatalogEntry curEntry = catalogProcessor.getEntryByStringID(curStringID, true);
        for (EFSingleValue curSingleValue : curRow.getValuesForStringID(curStringID)) {
          double curValueDec = 0.0;
          String curValue = curRow.getValueToUse(curSingleValue.getValue());
          if (curEntry.hasToValidateType()) {
            catalogProcessor.updateTypeForEFCSVCatalogEntry(curEntry, curRow
                    .generateCatalogEntryTypeFromValue(curValue, curEntry.getDateTimeFormat()));
            curEntry = catalogProcessor.getEntryByStringID(curStringID, true);
          }
          if (curEntry.getDataType().equals(CatalogEntryType.Number) && curValue != null) {
            curValueDec = curRow.getDoubleValueToUse(curValue);
          } else if (curEntry.getDataType().equals(CatalogEntryType.Bool)) {
            curValue = null;
          }
          Timestamp measureTime = null;
          if (curSingleValue.getFirstMetaData() != null) {
            measureTime = curRow.getMeasureTimeFromMetaDataString(curSingleValue.getFirstMetaData(),
                    curEntry.getDateTimeFormat());
          }
          if (measureTime == null) {
            measureTime = generateMeasureTime();
          }
          infoManager.insert(curEntry, pID, curValueDec, curValue, measureTime, caseID, docID);
          importCounter++;
          if (importCounter % autoCommitAfterRows == 0) {
            infoManager.commit();
          }
        }
      }
    }
    infoManager.commit();
    if (withResultPrintedToConsole) {
      System.out.println(importCounter + " infos have been successfully imported out of the "
              + csvRowCounter + " provided CSV-FactDataRows");
    }
  }

  private long getValidNumericID(String id, String idType, Long aPID, Long aCaseID, Long aDocID,
          EFCatalogCSVProcessor catalogProcessor, EFFactMetaDataCSVProcessor factMetaProcessor)
          throws SQLException {
    String idToWorkWith = id.trim();
    String catalogEntryID = "ORIGINAL_" + idType;
    long result;
    try {
      result = Long.parseLong(idToWorkWith);
    } catch (NumberFormatException e) {
      String curStringInfoID = idType + "_" + idToWorkWith;
      if (!stringInfoIDToNumericInfoIDMap.containsKey(curStringInfoID)) {
        EFCSVCatalogEntry curEntry = catalogProcessor.getEntryByStringID(catalogEntryID, true);
        catalogProcessor.updateTypeForEFCSVCatalogEntry(curEntry, CatalogEntryType.Text);
        curEntry = catalogProcessor.getEntryByStringID(catalogEntryID, true);
        long pIDToUse = aPID != null ? aPID : factMetaProcessor.getNextUsablePID();
        long caseIDToUse = aCaseID != null ? aCaseID : factMetaProcessor.getNextUsableCaseID();
        long docIDToUse = aDocID != null ? aDocID : factMetaProcessor.getNextUsableDocID();
        infoManager.insert(curEntry, pIDToUse, 0.0, idToWorkWith, generateMeasureTime(),
                caseIDToUse, docIDToUse);
        long newID = aPID == null ? pIDToUse : aCaseID == null ? caseIDToUse : docIDToUse;
        stringInfoIDToNumericInfoIDMap.put(curStringInfoID, newID);
      }
      result = stringInfoIDToNumericInfoIDMap.get(curStringInfoID);
    }
    if (result == 0 && idType != null && idType.equals("CaseID"))
      result = aPID;
    return result;
  }

  private Timestamp generateMeasureTime() {
    return new Timestamp(new Date().getTime());
  }

  public void exportPaDaWaNFacts() throws SQLException, IOException {
    exportPaDaWaNFacts(false);
  }

  public void exportPaDaWaNFacts(boolean withResultPrintedToConsole) throws SQLException, IOException {
    exportPaDaWaNFacts(withResultPrintedToConsole, null, null, null);
  }

  public void exportPaDaWaNFacts(boolean withResultPrintedToConsole, Set<Long> pidsToExport, Set<Long> caseidsToExport,
          Set<Integer> attrIDsToUse)
          throws SQLException, IOException {
    attrIDToStringIDMap.clear();
    List<Integer> usedAttrIDs;
    if (attrIDsToUse == null) {
      usedAttrIDs = infoManager.getAttrIDsOfInfosAfterTime(null);
    } else {
      usedAttrIDs = new ArrayList<>(attrIDsToUse);
    }
    for (int curAttrID : usedAttrIDs) {
      CatalogEntry curEntry = catalogManager.getEntryByID(curAttrID);
      attrIDToStringIDMap.put(curAttrID, EFCSVCatalogEntry
              .buildStringIDFromProjectAndExtID(curEntry.getProject(), curEntry.getExtID()));
    }
    String[] exportHeaders = buildExportHeaders();
    setHeadersForSaving(exportHeaders);
    List<String> combinedFactIDOrderList = new ArrayList<>();
    Map<String, EFFactDataRow> combinedFactIDToDataMap = new HashMap<>();
    List<Long> patients;
    if (pidsToExport == null) {
      patients = infoManager.getPIDsAfterTime(null);
    } else {
      patients = new ArrayList<>(pidsToExport);
    }
    Collections.sort(patients);
    int numOfProcessedPatients = 0;
    for (Long curPatient : patients) {
      InfoIterator infosForPatient = infoManager.getInfosByPID(curPatient, true);
      for (Information curInfo : infosForPatient) {
        if (caseidsToExport != null && !caseidsToExport.contains(curInfo.getCaseID())) {
          continue;
        }
        String curCumbinedFactID = EFFactDataRow.buildCombinedFactID(curInfo);
        String curStringID = attrIDToStringIDMap.get(curInfo.getAttrID());
        CatalogEntryType curType = catalogManager.getEntryByID(curInfo.getAttrID()).getDataType();
        if (!curType.equals(CatalogEntryType.Structure)) {
          if (combinedFactIDToDataMap.containsKey(curCumbinedFactID)) {
            combinedFactIDToDataMap.get(curCumbinedFactID).addInformation(curInfo, curStringID, curType);
          } else {
            combinedFactIDOrderList.add(curCumbinedFactID);
            combinedFactIDToDataMap.put(curCumbinedFactID, new EFFactDataRow(curInfo, curStringID, curType));
          }
        }
      }
      try {
        infosForPatient.dispose();
      } catch (DWIterException e) {
        throw new SQLException(e);
      }
      Collections.sort(combinedFactIDOrderList);
      for (String curCombinedFactID : combinedFactIDOrderList) {
        saveSingleRow(combinedFactIDToDataMap.get(curCombinedFactID).transformToObjectList(exportHeaders));
      }
      combinedFactIDOrderList.clear();
      combinedFactIDToDataMap.clear();
      numOfProcessedPatients++;
      if (numOfProcessedPatients % autoCommitAfterRows == 0) {
        System.out.println(numOfProcessedPatients + " of " + patients.size() + " patients have already been processed");
      }
    }
    commit();
    if (withResultPrintedToConsole) {
      printCurrentSaveResult();
    }
  }

  private String[] buildExportHeaders() {
    String[] headers = new String[3 + attrIDToStringIDMap.size()];
    for (int i = 1; i <= 3; i++) {
      headers[i - 1] = EFFactDataRow.idColumnName + i;
    }
    int i = 3;
    for (String header : attrIDToStringIDMap.values()) {
      headers[i++] = header;
    }
    return headers;
  }
}
