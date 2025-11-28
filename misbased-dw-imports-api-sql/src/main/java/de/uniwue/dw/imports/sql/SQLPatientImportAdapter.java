package de.uniwue.dw.imports.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.sql.IDwSqlSchemaConstant;
import de.uniwue.dw.core.sql.SQLDeleteAdapter;
import de.uniwue.dw.core.sql.SQLInfoAdapter;
import de.uniwue.dw.imports.api.DWImportsConfig;
import de.uniwue.dw.imports.data.PatientInfo;
import de.uniwue.dw.imports.manager.PatientManager;
import de.uniwue.dw.imports.manager.adapter.IPatientImportAdapter;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLInsertMode;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;

public abstract class SQLPatientImportAdapter extends DatabaseManager
        implements IDwSqlSchemaConstant, IPatientImportAdapter {

  private PatientManager patientManager;

  public SQLPatientImportAdapter(SQLManager aSqlManager, PatientManager aPatientManager) throws SQLException {
    super(aSqlManager);
    createSQLTables();
    patientManager = aPatientManager;
    setUseBulkInserts(SQLPropertiesConfiguration.getSQLBulkImportDir());
    if (!DWImportsConfig.getTreatAsInitialImport()) {
      setInsertMode(SQLInsertMode.bulkInsertTmpTableMerge);
    }
  }

  @Override
  public Set<String> getKeyColumnsInternal() {
    HashSet<String> keyColumns = new HashSet<String>(Arrays.asList(new String[] { "pid" }));
    return keyColumns;
  }

  // @Override
  // public void readTables() throws SQLException {
  // if (DWImportsConfig.getLoadMetaDataLazy()) {
  // String command = "SELECT pid FROM " + getTableName();
  // Statement st = sqlManager.createStatement();
  // ResultSet resultSet = st.executeQuery(command);
  // while (resultSet.next()) {
  // long pid = resultSet.getLong("pid");
  // patientManager.addPatient(pid);
  // }
  // } else {
  // super.readTables();
  // }
  // }

  @Override
  public String getTableName() {
    return T_IMPORT_PIDS;
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.sql.IPatientImportAdapter#deleteInfosOfStornoPIDs()
   */
  @Override
  public void deleteInfosOfStornoPIDs() throws SQLException {
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() < 2) {
      markInfosWithStornoPIDsDeleted();
    }
    String command = "";
    if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
      command += "UPDATE " + T_INFO + " SET storno=1, updateTime=? WHERE "
              + "infoID IN (SELECT * FROM (SELECT infoID FROM " + T_INFO + " i, " + getTableName()
              + " p WHERE p.storno = ? AND i.PID = p.PID) AS p)";
    } else {
      command += "DELETE FROM " + T_INFO + " WHERE " + "infoID IN (SELECT * FROM (SELECT infoID FROM " + T_INFO + " i, "
              + getTableName() + " p WHERE p.storno = ? AND i.PID = p.PID) AS p)";
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
   * @see de.uniwue.dw.imports.sql.IPatientImportAdapter#markInfosWithStornoPIDsDeleted()
   */
  private void markInfosWithStornoPIDsDeleted() throws SQLException {
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
      command += "," + T_INFO + ".AttrID ";
    }
    if (deleteAdapter.getColumnNames().contains("ref".toLowerCase())) {
      command += "," + T_INFO + ".Ref ";
    }
    if (deleteAdapter.getColumnNames().contains("measuretime".toLowerCase())) {
      command += "," + T_INFO + ".MeasureTime ";
    }
    command += "FROM " + getTableName() + ", " + T_INFO + " WHERE \n" + T_INFO + ".pid = " + getTableName()
            + ".pid AND \n" + "storno = ?";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    int paramOffset = 1;
    Timestamp importDate = new Timestamp(System.currentTimeMillis());
    st.setString(paramOffset++, importDate.toString());
    st.setBoolean(paramOffset++, true);
    st.execute();
    st.close();
    commit();
  }

  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
    readResultInternal(resultSet);
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.sql.IPatientImportAdapter#getPatient(long)
   */
  @Override
  public PatientInfo getPatient(long pid) throws SQLException {
    PatientInfo anInfo = null;

    Statement st = sqlManager.createStatement();
    String command = "SELECT pid, storno, YOB, sex FROM " + getTableName() + " WHERE pid=" + pid;
    ResultSet resultSet = st.executeQuery(command);
    while (resultSet.next()) {
      anInfo = readResultInternal(resultSet);
    }
    st.close();
    return anInfo;
  }

  protected PatientInfo readResultInternal(ResultSet resultSet) throws SQLException {
    long PID = resultSet.getLong("pid");
    boolean storno = resultSet.getBoolean("storno");
    String sex = resultSet.getString("sex");
    Integer yob = resultSet.getInt("YOB");
    PatientInfo anInfo = patientManager.createPatient(PID, storno, sex, yob);
    patientManager.addPatient(anInfo);
    return anInfo;
  }

}
