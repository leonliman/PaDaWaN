package de.uniwue.dw.exchangeFormat;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.model.manager.UniqueNameUtil;
import de.uniwue.dw.exchangeFormat.EFCSVCatalogEntry.ValidHeaders;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;

public class EFCatalogCSVProcessor extends EFAbstractCSVProcessor {

  private Map<String, EFCSVCatalogEntry> stringIDToEntryMap;

  private CatalogManager catalogManager;

  private String rootStringID;

  private Map<String, Integer> stringIDToAttrIDMap;

  private String defaultProjectName;

  private boolean hasToDeleteCatalog;

  private EFCSVCatalogEntry parentEntryToAdd = null;

  public EFCatalogCSVProcessor(String csvStringToParse) throws SQLException {
    this(csvStringToParse, null);
  }

  public EFCatalogCSVProcessor(String csvStringToParse, String defaultProjectName)
          throws SQLException {
    super(csvStringToParse);
    initialize(defaultProjectName);
    hasToDeleteCatalog = csvStringToParse.isEmpty();
  }

  public EFCatalogCSVProcessor(File csvFileToUse) throws SQLException {
    this(csvFileToUse, null);
  }

  public EFCatalogCSVProcessor(File csvFileToUse, String defaultProjectName) throws SQLException {
    super(csvFileToUse);
    initialize(defaultProjectName);
  }

  private void initialize(String defaultProjectName) throws SQLException {
    stringIDToEntryMap = new HashMap<>();
    catalogManager = DwClientConfiguration.getInstance().getCatalogManager();
    rootStringID = EFCSVCatalogEntry.buildStringIDFromProjectAndExtID(
            catalogManager.getRoot().getProject(), catalogManager.getRoot().getExtID());
    stringIDToAttrIDMap = new HashMap<>();
    this.defaultProjectName = defaultProjectName != null && !defaultProjectName.trim().isEmpty()
            ? defaultProjectName.trim()
            : "";
  }

  private void loadCSV() throws IOException, NumberFormatException, ParseException {
    stringIDToEntryMap.clear();
    stringIDToAttrIDMap.clear();
    for (CSVRecord curRecord : getCSVRecords()) {
      addCSVCatalogEntryToMap(new EFCSVCatalogEntry(curRecord, defaultProjectName), false);
    }
  }

  public void importCatalogFromCSV()
          throws SQLException, NumberFormatException, IOException, ParseException {
    importCatalogFromCSV(false);
  }

  public void importCatalogFromCSV(boolean withResultPrintedToConsole)
          throws SQLException, NumberFormatException, IOException, ParseException {
    loadCSV();
    deleteCatalog();
    for (EFCSVCatalogEntry curEntry : stringIDToEntryMap.values()) {
      processSingleCSVEntry(curEntry);
    }
    if (parentEntryToAdd != null) {
      stringIDToEntryMap.put(parentEntryToAdd.getStringID(), parentEntryToAdd);
      parentEntryToAdd = null;
    }
    if (withResultPrintedToConsole) {
      if (stringIDToAttrIDMap.size() != stringIDToEntryMap.size()) {
        System.err.println("Only " + stringIDToAttrIDMap.size() + " of the "
                + stringIDToEntryMap.size() + " CSV-CatalogEntries could be successfully imported");
      } else {
        System.out.println("All " + stringIDToAttrIDMap.size()
                + " CSV-CatalogEntries have been imported successfully");
      }
    }
  }

  private void deleteCatalog() throws SQLException {
    catalogManager.deleteAllEntries(DwClientConfiguration.getInstance().getInfoManager());
    catalogManager.initializeData();
  }

  private void processSingleCSVEntry(EFCSVCatalogEntry curEntry) throws SQLException {
    if (hasToDeleteCatalog) {
      deleteCatalog();
      hasToDeleteCatalog = false;
    }
    if (!stringIDToAttrIDMap.containsKey(curEntry.getStringID())) {
      if (curEntry.getParentStringID() == null && !defaultProjectName.isEmpty()) {
        String parentStringID = EFCSVCatalogEntry
                .buildStringIDFromProjectAndExtID(defaultProjectName, defaultProjectName);
        curEntry.setParentStringID(parentStringID);
        if (!stringIDToAttrIDMap.containsKey(parentStringID)) {
          EFCSVCatalogEntry parentEntry = EFCSVCatalogEntry.getMinimalEFCSVCatalogEntry(
                  parentStringID, defaultProjectName, defaultProjectName);
          parentEntry.setParentStringID(rootStringID);
          parentEntry.setDataType(CatalogEntryType.Structure);
          parentEntry.setHasToValidateType(false);
          processSingleCSVEntry(parentEntry);
          parentEntryToAdd = parentEntry;
        }
      }

      String curParentStringID = curEntry.getParentStringID() == null ? rootStringID
              : curEntry.getParentStringID().trim();
      curEntry.setParentStringID(curParentStringID);
      if (curParentStringID.isEmpty() || curParentStringID.equals(rootStringID)) {
        insertEFCSVCatalogEntryIntoDatabase(curEntry, catalogManager.getRoot());
      } else if (stringIDToAttrIDMap.containsKey(curParentStringID)) {
        insertEFCSVCatalogEntryIntoDatabase(curEntry,
                catalogManager.getEntryByID(stringIDToAttrIDMap.get(curParentStringID)));
      } else {
        EFCSVCatalogEntry curParentEntry = getEntryByStringID(curParentStringID, false);
        if (curParentEntry != null) {
          processSingleCSVEntry(curParentEntry);
          insertEFCSVCatalogEntryIntoDatabase(curEntry,
                  catalogManager.getEntryByID(stringIDToAttrIDMap.get(curParentStringID)));
        } else {
          System.err.println("The entry with ID " + curEntry.getStringID()
                  + " is referencing the non existant ParentID " + curParentStringID);
        }
      }
    }
  }

  private void insertEFCSVCatalogEntryIntoDatabase(EFCSVCatalogEntry curEntry,
          CatalogEntry parentEntry) throws SQLException {
    if (curEntry.getDescription() == null) {
      curEntry.setDescrption(UniqueNameUtil.createOrRepairUniqueNameIfNecessary(curEntry.getName(),
              curEntry.getProject(), parentEntry, curEntry.getUniqueName()));
    }
    CatalogEntry newEntry = catalogManager.getOrCreateEntry(curEntry.getName(),
            curEntry.getDataType(), curEntry.getExtID(), parentEntry, curEntry.getOrderValue(),
            curEntry.getProject(), curEntry.getUniqueName(), curEntry.getDescription());
    for (String choice : curEntry.getSingleChoiceChoice()) {
      catalogManager.addChoice(newEntry, choice);
    }
    if ((curEntry.getUnit() != null && !curEntry.getUnit().isEmpty())
            || curEntry.getLowBound() != curEntry.getHighBound()) {
      catalogManager.insertNumericMetaData(newEntry,
              curEntry.getUnit() != null ? curEntry.getUnit() : "", curEntry.getLowBound(),
              curEntry.getHighBound());
    }
    stringIDToAttrIDMap.put(curEntry.getStringID(), newEntry.getAttrId());
  }

  private void loadPaDaWaNCatalog(Set<Integer> attrIDsToUse) throws SQLException {
    stringIDToEntryMap.clear();
    stringIDToAttrIDMap.clear();
    if (attrIDsToUse == null) {
      for (CatalogEntry curEntry : catalogManager.getEntries()) {
        if (!curEntry.isRoot()) {
          addCSVCatalogEntryToMap(new EFCSVCatalogEntry(curEntry, curEntry.getParent()), true);
        }
      }
    } else {
      Set<Integer> processedEntries = new HashSet<>();
      for (int anAttrID : attrIDsToUse) {
        if (processedEntries.contains(anAttrID)) {
          continue;
        }
        CatalogEntry curEntry = catalogManager.getEntryByID(anAttrID);
        List<Integer> parentEntryAttrIDsToAdd = new ArrayList<>();
        CatalogEntry aParentEntry = curEntry.getParent();
        while (!aParentEntry.isRoot() && !processedEntries.contains(aParentEntry.getAttrId())) {
          parentEntryAttrIDsToAdd.add(0, aParentEntry.getAttrId());
          aParentEntry = aParentEntry.getParent();
        }
        for (int aParentAttrID : parentEntryAttrIDsToAdd) {
          CatalogEntry curParentEntry = catalogManager.getEntryByID(aParentAttrID);
          addCSVCatalogEntryToMap(new EFCSVCatalogEntry(curParentEntry, curParentEntry.getParent()), true);
          processedEntries.add(aParentAttrID);
        }
        addCSVCatalogEntryToMap(new EFCSVCatalogEntry(curEntry, curEntry.getParent()), true);
        processedEntries.add(anAttrID);
      }
    }
  }

  private void addCSVCatalogEntryToMap(EFCSVCatalogEntry curCSVEntry, boolean withSavingAttrID) {
    stringIDToEntryMap.put(curCSVEntry.getStringID(), curCSVEntry);
    if (withSavingAttrID) {
      stringIDToAttrIDMap.put(curCSVEntry.getStringID(), curCSVEntry.getAttrId());
    }
  }

  public EFCSVCatalogEntry getEntryByStringID(String aStringID, boolean fromCatalogManager)
          throws SQLException {
    EFCSVCatalogEntry savedEntry = stringIDToEntryMap.get(aStringID);
    if (!fromCatalogManager) {
      return savedEntry;
    } else {
      if (savedEntry == null || !stringIDToAttrIDMap.containsKey(aStringID)) {
        return EFCSVCatalogEntry.getMinimalEFCSVCatalogEntry(aStringID, defaultProjectName);
      } else {
        int curAttrID = stringIDToAttrIDMap.get(savedEntry.getStringID());
        CatalogEntry curEntry = catalogManager.getEntryByID(curAttrID);
        CatalogEntry curParentEntry = catalogManager.getEntryByID(curEntry.getParentID());
        return new EFCSVCatalogEntry(curEntry, curParentEntry, savedEntry.getStringID(),
                savedEntry.getParentStringID(), savedEntry.getDateTimeFormat(),
                savedEntry.hasToValidateType());
      }
    }
  }

  public void updateTypeForEFCSVCatalogEntry(EFCSVCatalogEntry aEntry, CatalogEntryType aType)
          throws SQLException {
    aEntry.setDataType(aType);
    if (stringIDToAttrIDMap.containsKey(aEntry.getStringID())) {
      catalogManager.updateEntry(aEntry);
    } else {
      processSingleCSVEntry(aEntry);
      if (parentEntryToAdd != null) {
        stringIDToEntryMap.put(parentEntryToAdd.getStringID(), parentEntryToAdd);
        parentEntryToAdd = null;
      }
    }
    aEntry.setHasToValidateType(false);
    stringIDToEntryMap.put(aEntry.getStringID(), aEntry);
  }

  public void exportPaDaWaNCatalog() throws IOException, SQLException {
    exportPaDaWaNCatalog(false);
  }

  public void exportPaDaWaNCatalog(boolean withResultPrintedToConsole) throws IOException, SQLException {
    exportPaDaWaNCatalog(withResultPrintedToConsole, null);
  }

  public void exportPaDaWaNCatalog(boolean withResultPrintedToConsole, Set<Integer> attrIDsToUse)
          throws IOException, SQLException {
    loadPaDaWaNCatalog(attrIDsToUse);
    setHeadersForSaving(ValidHeaders.getNames());
    for (EFCSVCatalogEntry curCSVEntry : stringIDToEntryMap.values()) {
      saveSingleRow(transformCSVCatalogEntryToObjectList(curCSVEntry));
    }
    commit();
    if (withResultPrintedToConsole) {
      printCurrentSaveResult();
    }
  }

  private List<Object> transformCSVCatalogEntryToObjectList(EFCSVCatalogEntry curCSVEntry) {
    List<Object> returnList = new ArrayList<>();
    returnList.add(curCSVEntry.getStringID());
    returnList.add(curCSVEntry.getName());
    returnList.add(curCSVEntry.getDescription());
    returnList.add(curCSVEntry.getProject());
    returnList.add(curCSVEntry.getExtID());
    returnList.add(curCSVEntry.getParentStringID());
    returnList.add(curCSVEntry.getDataType());
    returnList.add(curCSVEntry.getDateTimeFormatAsString());
    returnList.add(curCSVEntry.getSingleChoiceChoicesAsString());
    returnList.add(curCSVEntry.getLowBoundAsString());
    returnList.add(curCSVEntry.getHighBoundAsString());
    returnList.add(curCSVEntry.getUnit());
    return returnList;
  }

}
