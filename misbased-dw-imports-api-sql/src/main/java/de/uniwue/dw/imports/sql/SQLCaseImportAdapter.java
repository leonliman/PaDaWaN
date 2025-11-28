package de.uniwue.dw.imports.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.sql.IDwSqlSchemaConstant;
import de.uniwue.dw.core.sql.SQLDeleteAdapter;
import de.uniwue.dw.core.sql.SQLInfoAdapter;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.api.DWImportsConfig;
import de.uniwue.dw.imports.data.CaseInfo;
import de.uniwue.dw.imports.manager.CaseManager;
import de.uniwue.dw.imports.manager.adapter.ICaseImportAdapter;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLInsertMode;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;

public abstract class SQLCaseImportAdapter extends DatabaseManager implements IDwSqlSchemaConstant, ICaseImportAdapter {

  private CaseManager caseManager;

  public SQLCaseImportAdapter(SQLManager aSqlManager, CaseManager aCaseManager) throws SQLException {
    super(aSqlManager);
    caseManager = aCaseManager;
    createSQLTables();
    setUseBulkInserts(SQLPropertiesConfiguration.getSQLBulkImportDir());
    if (!DWImportsConfig.getTreatAsInitialImport()) {
      setInsertMode(SQLInsertMode.bulkInsertTmpTableMerge);
    }
  }

  @Override
  public Set<String> getKeyColumnsInternal() {
    HashSet<String> keyColumns = new HashSet<String>(Arrays.asList(new String[] { "caseid" }));
    return keyColumns;
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.sql.ICaseImportAdapter#readCase(long)
   */
  @Override
  public CaseInfo readCase(long caseID) throws SQLException {
    CaseInfo result = null;

    Statement st = sqlManager.createStatement();
    String command = "SELECT * FROM " + getTableName() + " WHERE caseid = " + caseID;
    ResultSet resultSet = st.executeQuery(command);
    if (resultSet.next()) {
      result = readResultInternal(resultSet);
    }
    resultSet.close();
    st.close();
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.sql.ICaseImportAdapter#readCasesForPID(long)
   */
  @Override
  public HashSet<CaseInfo> readCasesForPID(long PID) throws SQLException {
    HashSet<CaseInfo> result = new HashSet<CaseInfo>();

    Statement st = sqlManager.createStatement();
    String command = "SELECT caseID, pid, storno, admission, discharge, firstMovement, lastMovement, measureTime, casetype FROM "
            + getTableName() + " WHERE PID = " + PID;
    ResultSet resultSet = st.executeQuery(command);
    while (resultSet.next()) {
      CaseInfo anInfo = readResultInternal(resultSet);
      result.add(anInfo);
    }
    resultSet.close();
    st.close();
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.sql.ICaseImportAdapter#read()
   */
  @Override
  public void read() throws SQLException {
    Logger logger = LogManager.getLogger("DW-Imperator#importer");
    logger.trace("Started reading Cases for Import. ");
    readTables();
    logger.trace("Finished reading Cases for Import");
  }

  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
    readResultInternal(resultSet);
  }

  protected CaseInfo readResultInternal(ResultSet resultSet) throws SQLException {
    Timestamp admission, discharge, firstMovement, lastMovement, measureTime;

    long pid = resultSet.getLong("pid");
    long fallid = resultSet.getLong("caseid");
    admission = resultSet.getTimestamp("admission");
    discharge = resultSet.getTimestamp("discharge");
    firstMovement = resultSet.getTimestamp("firstMovement");
    lastMovement = resultSet.getTimestamp("lastMovement");
    measureTime = resultSet.getTimestamp("measureTime");
    boolean storno = resultSet.getBoolean("storno");
    String casetype = resultSet.getString("casetype");
    // String importFile = resultSet.getString("importFile");
    try {
      CaseInfo info = caseManager.createCaseInfo(pid, fallid, admission, discharge, firstMovement, lastMovement,
              measureTime, storno, casetype, null, measureTime);
      caseManager.addCase(info);
      return info;
    } catch (ImportException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public String getTableName() {
    return T_IMPORT_CASES;
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.sql.ICaseImportAdapter#deleteInfosOfStornoCases()
   */
  @Override
  public void deleteInfosOfStornoCases() throws SQLException {
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() < 2) {
      markInfosWithStornoCasesAsDeleted();
    }
    String command = "";
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      command += "UPDATE " + T_INFO + " SET storno=1, updateTime=? WHERE " + "infoID IN (SELECT * FROM (SELECT "
              + "i.infoID FROM " + T_INFO + " i, " + getTableName()
              + " c WHERE c.storno = ? AND i.caseID = c.caseID) AS p)";
    } else {
      command += "DELETE FROM " + T_INFO + " WHERE " + "infoID IN (SELECT * FROM (SELECT " + "i.infoID FROM " + T_INFO
              + " i, " + getTableName() + " c WHERE c.storno = ? AND i.caseID = c.caseID) AS p)";
    }
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    int paramOffset = 1;
    paramOffset = SQLInfoAdapter.setDeleteUpdateTime(st, paramOffset);
    st.setBoolean(paramOffset, true);
    st.execute();
    st.close();
    commit();
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.sql.ICaseImportAdapter#markInfosWithStornoCasesAsDeleted()
   */
  private void markInfosWithStornoCasesAsDeleted() throws SQLException {
    SQLDeleteAdapter deleteAdapter = (SQLDeleteAdapter) DwClientConfiguration.getInstance()
            .getInfoManager().deleteAdapter;
    String command = "INSERT INTO " + T_IMPORT_DELETE + " (infoid, deletetime, caseid, pid";
    // the stuff with the attrID is because of downward compatibilities
    if (deleteAdapter.getColumnNames().contains("attrid".toLowerCase())) {
      command += ", AttrID";
    }
    if (deleteAdapter.getColumnNames().contains("ref".toLowerCase())) {
      command += ", Ref";
    }
    if (deleteAdapter.getColumnNames().contains("measuretime".toLowerCase())) {
      command += ", MeasureTime";
    }
    command += ") " + "SELECT " + T_INFO + ".infoID, ?, " + T_INFO + ".caseID, " + T_INFO + ".pid ";
    if (deleteAdapter.getColumnNames().contains("attrid".toLowerCase())) {
      command += ", " + T_INFO + ".AttrID ";
    }
    if (deleteAdapter.getColumnNames().contains("ref".toLowerCase())) {
      command += ", " + T_INFO + ".Ref ";
    }
    if (deleteAdapter.getColumnNames().contains("measuretime".toLowerCase())) {
      command += ", " + T_INFO + ".MeasureTime ";
    }
    command += "FROM " + getTableName() + ", " + T_INFO + " WHERE \n" + T_INFO + ".caseID = " + getTableName()
            + ".caseID AND \n" + "storno = ?";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    int paramOffset = 1;
    Timestamp importDate = new Timestamp(System.currentTimeMillis());
    st.setString(paramOffset++, importDate.toString());
    st.setBoolean(paramOffset++, true);
    st.execute();
    st.close();
    commit();
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.sql.ICaseImportAdapter#deleteCase(long)
   */
  @Override
  public void deleteCase(long caseID) throws SQLException {
    String command = "DELETE FROM " + getTableName() + " WHERE " + " caseID=?";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    st.setLong(1, caseID);
    st.execute();
    st.close();
  }

}
