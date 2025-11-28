package de.uniwue.dw.core.sql.mssql;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.sql.SQLInfoAdapter;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MSSQLInfoAdapter extends SQLInfoAdapter {

  public static String pkName = "PK_DWInfo_InfoID";

  public MSSQLInfoAdapter(SQLManager aSqlManager, File bulkInsertFolderIfNeeded)
          throws SQLException {
    super(aSqlManager, bulkInsertFolderIfNeeded);
  }

  @Override
  protected void createSQLTables() throws SQLException {
    super.createSQLTables();
    if (DwClientConfiguration.getInstance().createInfoIndices()) {
      createAdditionalIndicesIfNotExist(false);
    } else if (DwClientConfiguration.getInstance().createInfoIndexOnPID()) {
      createAdditionalIndicesIfNotExist(true);
    }
  }

  @Override
  // @formatter:off
  protected String getCreateTableString() {
    String command = getCreateTableStub();
    command += "InfoID BIGINT " + SQLTypes.incrementFlagStartingWith1(sqlManager.config) + " NOT NULL, \n"
            + "AttrID INT NOT NULL, \n"
            + "PID BIGINT NOT NULL, \n"
            + "MeasureTime " + SQLTypes.timestampType(sqlManager.config) + " NOT NULL, \n"
            + "ImportTime " + SQLTypes.timestampType(sqlManager.config) + ", \n"
            + "CaseID BIGINT NOT NULL, \n"
            + "Ref BIGINT NOT NULL, \n"
            + "DocID BIGINT NOT NULL, \n"
            + "GroupID BIGINT NOT NULL, \n"
            + "Value " + SQLTypes.bigTextType(sqlManager.config) + ", \n"
            + "ValueShort VARCHAR(" + VALUE_SHORT_LENGTH + "), \n"
            + "ValueDec " + SQLTypes.decimalType() + ", \n"
            + "Storno TINYINT, \n"
            + "UpdateTime " + SQLTypes.timestampType(sqlManager.config) + ", \n"
            + "CONSTRAINT [" + pkName + "] PRIMARY KEY CLUSTERED ([InfoID] ASC)) \n";
    String uniqueKeyName = getTableName() + "_AttrID_PID_Time_CaseID_Ref";
    String uniqueKeyColumns = "AttrID, PID, MeasureTime, CaseID, Ref";
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      uniqueKeyName += "DocID_GroupID";
      uniqueKeyColumns += ", DocID, GroupID";
    }
    command += "ALTER TABLE " + getTableName() + " ADD CONSTRAINT " + uniqueKeyName
              + " UNIQUE (" + uniqueKeyColumns + ") WITH ( IGNORE_DUP_KEY = ON ) \n";
    return command;
  } // @formatter:on

  private List<String> getExistingIndices() throws SQLException {
    String selectIndicesCommand = "SELECT name FROM sys.indexes WHERE object_id = OBJECT_ID('"
            + getTableName() + "')";
    PreparedStatement st = sqlManager.createPreparedStatement(selectIndicesCommand);
    ResultSet resultSet = st.executeQuery();
    List<String> existingIndices = new ArrayList<String>();
    while (resultSet.next()) {
      String aValue = resultSet.getString("name");
      if (aValue != null) {
        existingIndices.add(aValue.toLowerCase());
      }
    }
    resultSet.close();
    st.close();
    return existingIndices;
  }

  private void createFulltextIndex() throws SQLException {
    if (!DwClientConfiguration.getInstance().createMSSQLFulltextCatalog())
      return;
    String command;
    command = "SELECT 1 FROM sys.fulltext_catalogs WHERE [name] = 'ft'";
    boolean catalogExists = sqlManager.hasResults(command);
    if (!catalogExists) {
      executeCommand("CREATE FULLTEXT CATALOG ft AS DEFAULT");
    }
    command = "SELECT COLUMNPROPERTY(OBJECT_ID('" + getTableName()
            + "'), 'value', 'IsFulltextIndexed')";
    boolean ftExists = sqlManager.executeBoolResultQuery(command);
    if (!ftExists && DwClientConfiguration.getInstance().useFulltextIndex()) {
      executeCommand("CREATE FULLTEXT INDEX ON DWInfo(value) KEY INDEX " + pkName);
    }
  }

  protected void createAdditionalIndicesIfNotExist(boolean onlyOnPID) throws SQLException {
    List<String> existingIndices = getExistingIndices();

    String pid_indexName = (getTableName() + "_PID").toLowerCase();
    if (!existingIndices.contains(pid_indexName)) {
      System.out.println("creating " + pid_indexName + " index on DWInfo");
      String includeCommand = "INCLUDE (AttrID, MeasureTime, ImportTime, CaseID, Ref";
      if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
        includeCommand += ", DocID, GroupID";
      }
      includeCommand += ", Value, ValueDec";
      if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
        includeCommand += ", Storno, UpdateTime";
      }
      includeCommand += ")";
      executeCommand("CREATE INDEX " + pid_indexName + " ON " + getTableName() + " (PID) " + includeCommand);
    }

    if (onlyOnPID) {
      return;
    }

    String attrID_CaseID_indexName = (getTableName() + "_AttrID_CaseID").toLowerCase();
    if (!existingIndices.contains(attrID_CaseID_indexName)) {
      System.out.println("creating " + attrID_CaseID_indexName + "index on DWInfo");
      executeCommand("CREATE INDEX " + attrID_CaseID_indexName + " ON " + getTableName()
              + " (AttrID, CaseID)");
    }
    String attrID_ValueShort_indexName = (getTableName() + "_AttrID_ValueShort").toLowerCase();
    if (!existingIndices.contains(attrID_ValueShort_indexName)) {
      System.out.println("creating " + attrID_ValueShort_indexName + " index on DWInfo");
      executeCommand("CREATE INDEX " + attrID_ValueShort_indexName + " ON " + getTableName()
              + " (AttrID, ValueShort)");
    }
    String attrID_ValueDec_indexName = (getTableName() + "_AttrID_ValueDec").toLowerCase();
    if (!existingIndices.contains(attrID_ValueDec_indexName)) {
      System.out.println("creating " + attrID_ValueDec_indexName + " index on DWInfo");
      executeCommand("CREATE INDEX " + attrID_ValueDec_indexName + " ON " + getTableName()
              + " (AttrID, ValueDec)");
    }

    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() < 2) {
      String importTime_indexName = (getTableName() + "_ImportTime").toLowerCase();
      if (!existingIndices.contains(importTime_indexName)) {
        System.out.println("creating " + importTime_indexName + " index on DWInfo");
        executeCommand("CREATE INDEX " + importTime_indexName + " ON " + getTableName()
                + " (ImportTime) INCLUDE (PID, CaseID, AttrID)");
      }
    }
    String caseID_indexName = (getTableName() + "_CaseID").toLowerCase();
    if (!existingIndices.contains(caseID_indexName)) {
      System.out.println("creating " + caseID_indexName + " index on DWInfo");
      executeCommand("CREATE INDEX " + caseID_indexName + " ON " + getTableName() + " (CaseID)");
    }
    String ref_indexName = (getTableName() + "_Ref").toLowerCase();
    if (!existingIndices.contains(ref_indexName)) {
      System.out.println("creating " + ref_indexName + " index on DWInfo");
      executeCommand("CREATE INDEX " + ref_indexName + " ON " + getTableName() + " (Ref)");
    }
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      String docID_indexName = (getTableName() + "_DocID").toLowerCase();
      if (!existingIndices.contains(docID_indexName)) {
        System.out.println("creating " + docID_indexName + " index on DWInfo");
        executeCommand("CREATE INDEX " + docID_indexName + " ON " + getTableName() + " (DocID)");
      }
      String groupID_indexName = (getTableName() + "_GroupID").toLowerCase();
      if (!existingIndices.contains(groupID_indexName)) {
        System.out.println("creating " + groupID_indexName + " index on DWInfo");
        executeCommand(
                "CREATE INDEX " + groupID_indexName + " ON " + getTableName() + " (GroupID)");
      }
      String updateTime_storno_indexName = (getTableName() + "_UpdateTime_Storno").toLowerCase();
      if (!existingIndices.contains(updateTime_storno_indexName)) {
        System.out.println("creating " + updateTime_storno_indexName + " index on DWInfo");
        executeCommand("CREATE INDEX " + updateTime_storno_indexName + " ON " + getTableName()
                + " (UpdateTime, Storno) INCLUDE (PID, CaseID, AttrID)");
      }
    }
    createFulltextIndex();
    commit();
  }

  private void setInsertParameters(PreparedStatement st, int attrID, long pid, String value,
          String valueShort, double valueDecimal, long ref, Timestamp measureTime, long caseID,
          long docID, long groupID) throws SQLException {
    int paramOffset = 1;
    Timestamp importDate = new Timestamp(System.currentTimeMillis());

    st.setInt(paramOffset++, attrID);
    st.setLong(paramOffset++, pid);
    st.setTimestamp(paramOffset++, measureTime);
    st.setLong(paramOffset++, caseID);
    st.setLong(paramOffset++, ref);
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      st.setLong(paramOffset++, docID);
      st.setLong(paramOffset++, groupID);
    }
    st.setInt(paramOffset++, attrID);
    st.setLong(paramOffset++, pid);
    st.setTimestamp(paramOffset++, measureTime);
    st.setString(paramOffset++, value);
    st.setString(paramOffset++, valueShort);
    if (Double.isNaN(valueDecimal)) {
      st.setNull(paramOffset++, Types.DECIMAL);
    } else {
      st.setDouble(paramOffset++, valueDecimal);
    }
    st.setLong(paramOffset++, ref);
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      st.setLong(paramOffset++, docID);
      st.setLong(paramOffset++, groupID);
      st.setTimestamp(paramOffset++, importDate); // updateTime
      st.setBoolean(paramOffset++, false); // storno
    }
    st.setTimestamp(paramOffset++, importDate);
    st.setLong(paramOffset++, caseID);
  }

  @Override
  protected long insertBySingleStatement(int attrID, long pid, String value, String valueShort,
          double valueDecimal, long ref, Timestamp measureTime, long caseID, long docID,
          long groupID) throws SQLException {
    String commandAppendix = "";
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      commandAppendix = " AND docID=? AND groupID=?";
    }
    String command = "IF NOT EXISTS (SELECT * FROM " + getTableName() + " WHERE "
            + "attrID=? AND pid=? AND measureTime=? AND caseID=? AND ref=?" + commandAppendix + ") INSERT \n" +
            "INTO " + getTableName() + "\n(attrID, pid, measureTime, value, valueShort, valueDec, ref, ";
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      command += "docID, groupID, updateTime, storno, ";
    }
    command += "importTime, caseID) \n" + "VALUES (?, ?, ?, ?, ?, ?, ?, ";
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      command += "?, ?, ?, ?,";
    }
    command += "?, ?)";
    PreparedStatement st = sqlManager.createPreparedStatementReturnGeneratedKey(command);
    setInsertParameters(st, attrID, pid, value, valueShort, valueDecimal, ref, measureTime, caseID,
            docID, groupID);
    try {
      st.execute();
    } catch (Exception e) {
      System.out.println("attrID: " + attrID + "; pid: " + pid + "; value:" + value
              + "; valueShort:" + valueShort + "; valueDecimal:" + valueDecimal + "; ref:" + ref
              + "; measureTime:" + measureTime + ";caseID: " + caseID);
      throw e;
    }
    long result = getGeneratedLongKey(st);
    st.close();
    return result;
  }

  @Override
  protected long insertOrUpdateBySingleStatement(int attrID, long pid, String value,
          String valueShort, double valueDecimal, long ref, Timestamp measureTime, long caseID,
          long docID, long groupID) throws SQLException {
    String commandAppendix = "";
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      commandAppendix = " AND docID=? AND groupID=?";
    }
    String command = "IF NOT EXISTS (SELECT * FROM " + getTableName() + " WHERE "
            + "attrID=? AND pid=? AND measureTime=? AND caseID=? AND ref=?" + commandAppendix + ") INSERT \n" +
            "INTO " + getTableName() + "\n(attrID, pid, measureTime, value, valueShort, valueDec, ref, ";
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      command += "docID, groupID,";
    }
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      command += " updateTime";
    } else {
      command += " importTime";
    }
    command += ", caseID) \n" + "VALUES (?, ?, ?, ?, ?, ?, ?, ";
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      command += "?, ?, ";
    }
    command += "?, ?)" + " ELSE UPDATE " + getTableName()
            + " SET value=?, valueShort=?, valueDec=?, ref=?, ";
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      command += "docID=?, groupID=?, ";
    }
    command +=
            "importTime=?, caseID=?, storno=? WHERE " + "attrID=? AND pid=? AND measureTime=?" + commandAppendix + "\n";
    PreparedStatement st = sqlManager.createPreparedStatementReturnGeneratedKey(command);
    setUpdateParametersForInsertOrUpdateStatement(st, attrID, pid, value, valueShort, valueDecimal,
            ref, measureTime, caseID, docID, groupID);
    st.execute();
    long result = getGeneratedLongKey(st);
    st.close();
    return result;
  }

  private void setUpdateParametersForInsertOrUpdateStatement(PreparedStatement st, int attrID,
          long pid, String value, String valueShort, double valueDecimal, long ref,
          Timestamp measureTime, long caseID, long docID, long groupID) throws SQLException {
    int paramOffset = 1;
    Timestamp importDate = new Timestamp(System.currentTimeMillis());
    st.setInt(paramOffset++, attrID);
    st.setLong(paramOffset++, pid);
    st.setTimestamp(paramOffset++, measureTime);
    st.setLong(paramOffset++, caseID);
    st.setLong(paramOffset++, ref);
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      st.setLong(paramOffset++, docID);
      st.setLong(paramOffset++, groupID);
    }

    st.setInt(paramOffset++, attrID);
    st.setLong(paramOffset++, pid);
    st.setTimestamp(paramOffset++, measureTime);
    st.setString(paramOffset++, value);
    st.setString(paramOffset++, valueShort);
    if (Double.isNaN(valueDecimal)) {
      st.setNull(paramOffset++, Types.DECIMAL);
    } else {
      st.setDouble(paramOffset++, valueDecimal);
    }
    st.setLong(paramOffset++, ref);
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      st.setLong(paramOffset++, docID);
      st.setLong(paramOffset++, groupID);
    }
    st.setTimestamp(paramOffset++, importDate);
    st.setLong(paramOffset++, caseID);

    st.setString(paramOffset++, value);
    st.setString(paramOffset++, valueShort);
    if (Double.isNaN(valueDecimal)) {
      st.setNull(paramOffset++, Types.DECIMAL);
    } else {
      st.setDouble(paramOffset++, valueDecimal);
    }
    st.setLong(paramOffset++, ref);
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      st.setLong(paramOffset++, docID);
      st.setLong(paramOffset++, groupID);
    }
    st.setTimestamp(paramOffset++, importDate);
    st.setLong(paramOffset++, caseID);
    st.setInt(paramOffset++, 0);
    st.setInt(paramOffset++, attrID);
    st.setLong(paramOffset++, pid);
    st.setTimestamp(paramOffset++, measureTime);
    st.setLong(paramOffset++, caseID);
    st.setLong(paramOffset++, ref);
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      st.setLong(paramOffset++, docID);
      st.setLong(paramOffset++, groupID);
    }
  }

  @Override
  public String getStornoNoSetQuery() {
    return "(storno IS NULL or storno = 0)";
  }

  @Override
  public double getStandardDeviationForAllValues(int attrID) throws SQLException {
    String command = "SELECT STDEV(valueDec) AS 'stdev' FROM " + getTableName() + " WHERE attrID=?";
    command += " AND " + getStornoNoSetQuery();
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    st.setLong(1, attrID);
    ResultSet resultSet = st.executeQuery();
    double result = 0;
    if (resultSet.next())
      result = resultSet.getDouble("stdev");
    resultSet.close();
    st.close();
    return result;
  }

}
