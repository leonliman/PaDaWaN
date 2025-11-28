package de.uniwue.dw.imports.configured;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.imports.DataElem;
import de.uniwue.dw.imports.IDataElemIterator;
import de.uniwue.dw.imports.ITableElem;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.configured.data.ConfigCatalogTable;
import de.uniwue.dw.imports.manager.ImportLogManager;
import de.uniwue.dw.imports.manager.ImporterManager;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;
import de.uniwue.misc.util.TimeUtil;

public class ConfiguredTableCatalogImporter {

  private ConfigCatalogTable csvConfig;

  private ImporterManager importManager;

  public ConfiguredTableCatalogImporter(ConfigCatalogTable aCsvConfig,
          ImporterManager anImportManager) {
    csvConfig = aCsvConfig;
    importManager = anImportManager;
  }

  private String getItem(String key, ITableElem tableElem) throws ImportException {
    return tableElem.getItem(key);
  }

  private CatalogManager getCatalogManager() {
    return importManager.catalogManager;
  }

  private String getProject() {
    return csvConfig.getProject();
  }

  // only the first entry has to be put under the given parentProject-node
  private boolean isFirstLine = true;

  protected void processImportInfoFileLine(ITableElem tableElem)
          throws ImportException, SQLException, IOException {
    String extID = getItem(csvConfig.extIDColumn, tableElem);
    String name = getItem(csvConfig.nameColumn, tableElem);
    String parentExtID;
    String parentProject;
    CatalogEntry parent;
    if (csvConfig.parentExtIDColumn != null) {
      parentExtID = getItem(csvConfig.parentExtIDColumn, tableElem);
    } else {
      parentExtID = csvConfig.getParentExtID();
    }
    if (csvConfig.parentProjectColumn != null) {
      parentProject = getItem(csvConfig.parentProjectColumn, tableElem);
    } else {
      if (isFirstLine) {
        parentProject = csvConfig.getParentProject();
      } else {
        parentProject = getProject();
      }
    }
    parent = getCatalogManager().getEntryByRefID(parentExtID, parentProject);
    CatalogEntryType dataType = CatalogEntryType.Bool;
    if (csvConfig.dataTypeColumn != null) {
      String dataTypeString = getItem(csvConfig.dataTypeColumn, tableElem);
      dataType = CatalogEntryType.parse(dataTypeString);
    }
    int orderValue = -1;
    if (csvConfig.orderValueColumn != null) {
      String orderValueString = getItem(csvConfig.orderValueColumn, tableElem);
      orderValue = Integer.valueOf(orderValueString);
    }
    String uniqueName = null;
    if (csvConfig.uniqueNameColumn != null) {
      uniqueName = getItem(csvConfig.uniqueNameColumn, tableElem);
    }
    String project;
    if (csvConfig.projectColumn != null) {
      project = getItem(csvConfig.projectColumn, tableElem);
    } else {
      project = getProject();
    }
    String description = null;
    if (csvConfig.descriptionColumn != null) {
      description = getItem(csvConfig.descriptionColumn, tableElem);
    }
    if (useBulkInsert()) {
      String attrIDString = getItem(csvConfig.attrIDColumn, tableElem);
      int attrID = Integer.valueOf(attrIDString);
      String parentAttrIDString = getItem(csvConfig.parentAttrIDColumn, tableElem);
      int parentAttrID = Integer.valueOf(parentAttrIDString);
      Timestamp creationTime = new Timestamp(new Date().getTime());
      getCatalogManager().catalogAdapter.insertByBulk(attrID, name, extID, parentAttrID, orderValue,
              dataType, project, creationTime, uniqueName, description);
      readNumericMetaData(null, tableElem);
    } else {
      CatalogEntry newEntry = getCatalogManager().getOrCreateEntry(name, dataType, extID, parent,
              orderValue, project, uniqueName, description);
      readNumericMetaData(newEntry, tableElem);
    }
    isFirstLine = false;
  }

  private void readNumericMetaData(CatalogEntry newEntry, ITableElem tableElem)
          throws ImportException, SQLException {
    String unit = null;
    if (csvConfig.unitColumn != null) {
      unit = getItem(csvConfig.unitColumn, tableElem);
    }
    String lowBoundString = null;
    if (csvConfig.lowerBoundColumn != null) {
      lowBoundString = getItem(csvConfig.lowerBoundColumn, tableElem);
    }
    String highBoundString = null;
    if (csvConfig.upperBoundColumn != null) {
      highBoundString = getItem(csvConfig.upperBoundColumn, tableElem);
    }
    if (((unit != null) && !unit.isEmpty())
            || ((lowBoundString != null) && !lowBoundString.isEmpty())
            || ((highBoundString != null) && !highBoundString.isEmpty())) {
      double lowBound = 0;
      if ((lowBoundString != null) && !lowBoundString.isEmpty() && !unit.equals("Date")) {
        lowBound = Double.valueOf(lowBoundString);
      } else if (unit.equals("Date")) {
        // convert to Unix timestamp
        lowBound = TimeUtil.getUnixTime(TimeUtil.parseDate(lowBoundString));
      }
      double highBound = 0;
      if ((highBoundString != null) && !highBoundString.isEmpty() && !unit.equals("Date")) {
        highBound = Double.valueOf(highBoundString);
      } else if (unit.equals("Date")) {
        // convert to Unix timestamp
        highBound = TimeUtil.getUnixTime(TimeUtil.parseDate(highBoundString));
      }
      if (useBulkInsert()) {
        String attrIDString = getItem(csvConfig.attrIDColumn, tableElem);
        int attrID = Integer.valueOf(attrIDString);
        getCatalogManager().numDataAdapter.insertByBulk(attrID, lowBound, highBound, unit);
      } else {
        getCatalogManager().insertNumericMetaData(newEntry, unit, lowBound, highBound);
      }
    }
  }

  private void reportError(ImportException e, long rowCounter, ITableElem tableElem) {
    e.setLine(rowCounter);
    e.setFile(tableElem);
    e.setProject(getProject());
    ImportLogManager.error(e);
  }

  private void checkCSVHeaders(ITableElem tableElem) throws ImportException {
    List<String> columns2Check = new ArrayList<String>();
    if (csvConfig.dataTypeColumn != null) {
      columns2Check.add(csvConfig.dataTypeColumn);
    }
    if (csvConfig.extIDColumn != null) {
      columns2Check.add(csvConfig.extIDColumn);
    }
    if (csvConfig.lowerBoundColumn != null) {
      columns2Check.add(csvConfig.lowerBoundColumn);
    }
    if (csvConfig.nameColumn != null) {
      columns2Check.add(csvConfig.nameColumn);
    }
    if (csvConfig.parentExtIDColumn != null) {
      columns2Check.add(csvConfig.parentExtIDColumn);
    }
    if (csvConfig.parentProjectColumn != null) {
      columns2Check.add(csvConfig.parentProjectColumn);
    }
    if (csvConfig.projectColumn != null) {
      columns2Check.add(csvConfig.projectColumn);
    }
    if (csvConfig.stornoColumn != null) {
      columns2Check.add(csvConfig.stornoColumn);
    }
    if (csvConfig.uniqueNameColumn != null) {
      columns2Check.add(csvConfig.uniqueNameColumn);
    }
    if (csvConfig.unitColumn != null) {
      columns2Check.add(csvConfig.unitColumn);
    }
    if (csvConfig.upperBoundColumn != null) {
      columns2Check.add(csvConfig.upperBoundColumn);
    }
    tableElem.checkRequiredHeaders(columns2Check);
  }

  public boolean doImport() throws ImportException {
    IDataElemIterator dataElemsToProcess = csvConfig.getDataSource()
            .getDataElemsToProcess(getProject(), false);
    DataElem next = dataElemsToProcess.next();
    boolean result = doImport((ITableElem) next);
    return result;
  }

  private boolean useBulkInsert() {
    if ((csvConfig.attrIDColumn != null)
            && (SQLPropertiesConfiguration.getSQLBulkImportDir() != null)) {
      return true;
    }
    return false;
  }

  public boolean doImport(ITableElem tableElem) throws ImportException {
    try {
      checkCSVHeaders(tableElem);
      long rowCounter = 0L;
      long successfulRows = 0L;
      if (useBulkInsert()) {
        getCatalogManager().catalogAdapter
                .setUseBulkInserts(SQLPropertiesConfiguration.getSQLBulkImportDir(), true);
        getCatalogManager().numDataAdapter
                .setUseBulkInserts(SQLPropertiesConfiguration.getSQLBulkImportDir(), true);
      }
      while (tableElem.moveToNextLine()) {
        rowCounter++;
        try {
          if (tableElem.isCurrentLineWellFormed()) {
            try {
              processImportInfoFileLine(tableElem);
              successfulRows++;
            } catch (SQLException e) {
              throw new ImportException(ImportExceptionType.SQL_ERROR, "", e);
            } catch (IOException e) {
              throw new ImportException(ImportExceptionType.IO_ERROR, "", e);
            }
          } else {
            throw new ImportException(ImportExceptionType.CSV_HEADER_ROW_LENGTH_MISMATCH,
                    "not same amount of tokans as its header");
          }
        } catch (ImportException e) {
          reportError(e, rowCounter, tableElem);
        }
      }
      if (useBulkInsert()) {
        getCatalogManager().catalogAdapter.commit();
        getCatalogManager().numDataAdapter.commit();
        getCatalogManager().catalogAdapter.setUseBulkInserts(null, false);
        getCatalogManager().numDataAdapter.setUseBulkInserts(null, false);
      }
      ImportLogManager.info(new ImportException(ImportExceptionType.IMPORT_PROCESS,
              getProject() + ": Successfully processed " + successfulRows
                      + " lines while finishing file '" + tableElem.getName() + "'"));
    } catch (SQLException e) {
      reportError(new ImportException(ImportExceptionType.SQL_ERROR,
              "cannot switch bulk insert mode.", e), 0, tableElem);
    }
    return true;
  }

}
