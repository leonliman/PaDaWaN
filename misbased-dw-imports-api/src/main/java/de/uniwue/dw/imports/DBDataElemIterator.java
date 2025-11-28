package de.uniwue.dw.imports;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;

import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.api.DWImportsConfig;
import de.uniwue.dw.imports.configured.data.ConfigDataSourceDatabase;
import de.uniwue.dw.imports.configured.data.ConfigDataTable;
import de.uniwue.misc.sql.SQLFactory;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.util.StringUtilsUniWue;
import de.uniwue.misc.util.TimeUtil;

public class DBDataElemIterator extends DataElem implements ITableElem, IDataElemIterator {

  // this has to be for empty result sets
  boolean hasBeenMoved = false;

  private ResultSet resultSet;

  private PreparedStatement st;

  private LinkedHashSet<String> headerColumnNames;

  private Long rowCounter = 0L;

  private Timestamp lastUpdateTimestamp, currentUpdateTimestamp;

  public DBDataElemIterator(ConfigDataSourceDatabase aDataSource) throws ImportException {
    super(aDataSource);
    initialize();
  }


  private void initialize() throws ImportException {
    try {
      SQLFactory sqlFactory = SQLFactory.getSQLFactory(DWImportsConfig.getHDP_SQLConfig(), null);
      SQLManager sqlManager = sqlFactory.getSQLManager(DWImportsConfig.getHDP_SQLConfig());
      String select = getDataSource().selectString;
      if (select.toLowerCase().contains("where")) {
        select += " AND ";
      } else {
        select += " WHERE ";
      }
      // this means that this is a continuation of an initial import that was interrupted. The
      // current importer did already finish his share of imports.
      // Do not import anything more until to the timstamp that was previously stored
      lastUpdateTimestamp = DWImportsConfig.getDBImportLogManager()
              .getLastUpdateTimestamp(getDataSource().getProject());
      if (lastUpdateTimestamp == null) {
        lastUpdateTimestamp = new Timestamp(System.currentTimeMillis());
      }
      currentUpdateTimestamp = new Timestamp(System.currentTimeMillis());
      if (DWImportsConfig.getTreatAsInitialImport()) {
//        select += "erstelltDatum > '2021-06-15' AND \n";
//        if (getConfigDataTable().stornoColumn != null) {
//          select += getConfigDataTable().stornoColumn + " = '' AND ";
//        }
        select += getDataSource().sourceTable + ".RecordID > ? AND \n" + getDataSource().sourceTable
                + ".RecordID <= ? \nORDER BY " + getDataSource().sourceTable + ".RecordID";
      } else {
        // @formatter:off
        select += getDataSource().sourceTable + ".RecordID IN\n" + 
                " (SELECT log.RecordID FROM \n" + 
                "  [hdp].[dbo].[HDP_Tables] AS tables, \n" + 
                "  HDP_DeltaLog AS log WHERE \n" + 
                "  tables.HDP_Area = 'HDP_ForCare' AND \n" + 
                "  tables.TableID = log.TableID AND \n" + 
                "  tables.TableName = '" + getDataSource().sourceTable + "' AND \n" + 
                "  log.Timestamp >= ? \n" + 
                "  GROUP BY log.RecordID\r\n" + ") AND \n" + 
                getDataSource().sourceTable + ".RecordID <= ? ORDER BY " + getDataSource().sourceTable + ".RecordID";
        // @formatter:on
      }
      st = sqlManager.createPreparedStatement(select);
      int offset = 1;
      long currentRecordID = DWImportsConfig.getDBImportLogManager().getCurrentRecordID(getDataSource().getProject());
      rowCounter = currentRecordID;
      long maxRecordID;
      if ((getDataSource().alternativeMaxRecordIDTable == null)
              || getDataSource().alternativeMaxRecordIDTable.isEmpty()) {
        maxRecordID = DWImportsConfig.getDBImportLogManager().getMaxRecordID(getDataSource().sourceTable);
      } else {
        maxRecordID = DWImportsConfig.getDBImportLogManager()
                .getMaxRecordID(getDataSource().alternativeMaxRecordIDTable);
      }
      String replaceAll = lastUpdateTimestamp.toString().replaceAll("\\.\\d*$", "").replaceAll("[^\\d]", "");
      if (DWImportsConfig.getTreatAsInitialImport()) {
        st.setLong(offset++, currentRecordID);
        st.setLong(offset++, maxRecordID);
      } else {
        st.setString(offset++, replaceAll);
        st.setLong(offset++, maxRecordID);
      }
      resultSet = st.executeQuery();
      int x = 0;
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, e);
    }
  }


  protected ConfigDataTable getConfigDataTable() {
    return (ConfigDataTable) getDataSource().getParent();
  }


  protected ConfigDataSourceDatabase getDataSource() {
    return (ConfigDataSourceDatabase) super.getDataSource();
  }


  @Override
  public boolean hasNext() {
    throw new RuntimeException("does not implement");
    // try {
    // // this has to be for empty result sets
    // if (resultSet.isBeforeFirst()) {
    // return true;
    // }
    // if (hasBeenMoved) {
    // boolean result = !resultSet.isLast();
    // return result;
    // }
    // return false;
    // } catch (SQLException e) {
    // throw new RuntimeException(e);
    // }
  }


  @Override
  public DataElem next() {
    try {
      Boolean moveToNextLine = moveToNextLine();
      if (!moveToNextLine) {
        return null;
      }
    } catch (ImportException e) {
      e.printStackTrace();
    }
    return this;
  }


  @Override
  public long getTimestamp() {
    try {
      if (getDataSource().timestampColumnName != null) {
        String timestampString = resultSet.getString(getDataSource().timestampColumnName);
        Date timestamp = TimeUtil.parseDate(timestampString);
        long timestampTicks = timestamp.getTime();
        return timestampTicks;
      } else {
        return 0;
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }


  @Override
  public String getName() {
    String nameString = null;
    try {
      if (getDataSource().nameColumnName != null) {
        nameString = resultSet.getString(getDataSource().nameColumnName);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return nameString;
  }


  @Override
  public String getContent() throws ImportException {
    String contentString = null;
    if (getDataSource().contentColumnName != null) {
      try {
        contentString = resultSet.getString(getDataSource().contentColumnName);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    return contentString;
  }


  @Override
  public InputStreamReader getInputStreamReader() throws ImportException {
    try {
      InputStream in = IOUtils.toInputStream(getContent(), getDataSource().encoding);
      return new InputStreamReader(in);
    } catch (Exception e) {
      throw new ImportException(ImportExceptionType.IO_ERROR, e);
    }
  }


  @Override
  public List<String> getHeaderColumnsAsList() {
    return new ArrayList<String>(getHeaderColumns());
  }


  @Override
  public Set<String> getHeaderColumns() {
    if (headerColumnNames == null) {
      headerColumnNames = new LinkedHashSet<String>();
      try {
        int colCount = resultSet.getMetaData().getColumnCount();
        for (int i = 1; i <= colCount; i++) {
          headerColumnNames.add(resultSet.getMetaData().getColumnName(i).toLowerCase());
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    return headerColumnNames;
  }


  @Override
  public Integer getColumnIndex(String key) throws ImportException {
    List<String> result = getHeaderColumnsAsList();
    return result.indexOf(key);
  }


  @Override
  public String getItem(String key) throws ImportException {
    try {
      if (!getHeaderColumns().contains(key.toLowerCase())) {
        throw new ImportException(ImportExceptionType.CSV_COLUMN_NON_EXISTANT,
                "Column '" + key + "' does not exists in table.");
      }
      String result = resultSet.getString(key);
      return result;
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, e);
    }
  }


  @Override
  public Boolean moveToNextLine() throws ImportException {
    try {
      boolean result = resultSet.next();
      if (!result) {
        return false;
      } else {
        // this has to be for empty result sets
        hasBeenMoved = true;
        String rowIDcolumn = "RecordID";
        if (getDataSource().rowIDColumn != null) {
          rowIDcolumn = getDataSource().rowIDColumn;
        }
        rowCounter = resultSet.getLong(rowIDcolumn);
        return result;
      }
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, e);
    }
  }


  @Override
  public Integer getTokenLength() {
    return getHeaderColumnsAsList().size();
  }


  @Override
  public Integer getHeaderColAmount() {
    return getHeaderColumnsAsList().size();
  }


  @Override
  public boolean initHeaderLine() throws ImportException {
    return true;
  }


  @Override
  public void checkRequiredHeaders(String[] necessaryColumnHeaders) throws ImportException {
    checkRequiredHeaders(Arrays.asList(necessaryColumnHeaders));
  }


  @Override
  public void checkRequiredHeaders(List<String> necessaryColumnHeaders) throws ImportException {
    for (String aNecHeader : necessaryColumnHeaders) {
      if (!getHeaderColumns().contains(aNecHeader.toLowerCase())) {
        throw new ImportException(ImportExceptionType.CSV_COLUMNS_MISSING,
                "does not contain column " + aNecHeader + ". Required: " + necessaryColumnHeaders + ". Included: "
                        + StringUtilsUniWue.concat(getHeaderColumns(), ",") + "");
      }
    }
  }


  @Override
  public void close() throws ImportException {
    try {
      DWImportsConfig.getDBImportLogManager().insertOrUpdateLastUpdateTimestamp(currentUpdateTimestamp,
              getDataSource().getProject());
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, e);
    }
  }


  @Override
  public String[] getCurrentLineTokens() {
    ArrayList<String> result = new ArrayList<String>();
    for (String aHeader : getHeaderColumnsAsList()) {
      String aCell;
      try {
        aCell = resultSet.getString(aHeader);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      result.add(aCell);
    }
    return result.toArray(new String[0]);
  }


  @Override
  public boolean isCurrentLineWellFormed() {
    return true;
  }


  @Override
  public void dispose() throws ImportException {
    try {
      resultSet.close();
      st.close();
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, e);
    }
  }


  @Override
  public Long getRowCounter() {
    return rowCounter;
  }


  @Override
  public void logLatestRowNumber() {
    try {
      DWImportsConfig.getDBImportLogManager().insertOrUpdateMaxRecordID(getRowCounter(), getDataSource().getProject());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

}
