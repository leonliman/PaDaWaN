package de.uniwue.dw.imports.sql.mssql;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import de.uniwue.dw.imports.manager.CaseManager;
import de.uniwue.dw.imports.sql.SQLCaseImportAdapter;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;

public class MSSQLCaseImportAdapter extends SQLCaseImportAdapter {

  public MSSQLCaseImportAdapter(SQLManager aSqlManager, CaseManager aCaseManager)
          throws SQLException {
    super(aSqlManager, aCaseManager);
  }

  @Override
  // @formatter:off
  protected String getCreateTableString() {
    String command = getCreateTableStub();
    command += "caseid BIGINT PRIMARY KEY, \n" 
            + "pid BIGINT, \n" + "storno TINYINT, \n        "
            + "importDate " + SQLTypes.timestampType(sqlManager.config) + ", \n"
            + "admission " + SQLTypes.timestampType(sqlManager.config) + ", \n"
            + "discharge " + SQLTypes.timestampType(sqlManager.config) + ", \n"
            + "importFile VARCHAR(500), \n"
            + "firstMovement " + SQLTypes.timestampType(sqlManager.config) + ", \n"
            + "lastMovement " + SQLTypes.timestampType(sqlManager.config) + ", \n"
            + "measureTime " + SQLTypes.timestampType(sqlManager.config) + ", \n"
            + "casetype VARCHAR(20)"
            + ") \n" 
            + "CREATE INDEX " + getTableName() + "_pid on " + getTableName() + " (pid); \n" 
            + "ALTER TABLE " + getTableName() + " REBUILD WITH (IGNORE_DUP_KEY = ON)";
    return command;
  } // @formatter:on

  public void insert(long pid, long caseID, boolean storno, Timestamp admissionDate,
          Timestamp dischargeDate, Timestamp firstMovement, Timestamp lastMovement,
          String importFile, Timestamp creationDate, String casetype) throws SQLException {
    Timestamp importDate = new Timestamp(System.currentTimeMillis());

    if (useBulkInserts()) {
      insertByBulk(Long.valueOf(caseID), Long.valueOf(pid), Boolean.valueOf(storno), importDate,
              admissionDate, dischargeDate, importFile, firstMovement, lastMovement, creationDate,
              casetype);
      return;
    }
    String command = "UPDATE " + getTableName() + " SET "
            + "pid=?, storno=?, importDate=?, importFile=?, admission=?, discharge=?, "
            + "firstMovement=?, lastMovement=?, measureTime=?, casetype=? WHERE caseID=?;\n"
            + "IF @@ROWCOUNT=0 \n " + "INSERT INTO " + getTableName()
            + " (pid, storno, importDate, importFile, admission, discharge, firstMovement, "
            + "lastMovement, measureTime, casetype, caseid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    int paramOffset = 1;
    st.setLong(paramOffset++, pid);
    st.setBoolean(paramOffset++, storno);
    st.setTimestamp(paramOffset++, importDate);
    st.setString(paramOffset++, importFile);
    st.setTimestamp(paramOffset++, admissionDate);
    st.setTimestamp(paramOffset++, dischargeDate);
    st.setTimestamp(paramOffset++, firstMovement);
    st.setTimestamp(paramOffset++, lastMovement);
    st.setTimestamp(paramOffset++, creationDate);
    st.setString(paramOffset++, casetype);
    st.setLong(paramOffset++, caseID);

    st.setLong(paramOffset++, pid);
    st.setBoolean(paramOffset++, storno);
    st.setTimestamp(paramOffset++, importDate);
    st.setString(paramOffset++, importFile);
    st.setTimestamp(paramOffset++, admissionDate);
    st.setTimestamp(paramOffset++, dischargeDate);
    st.setTimestamp(paramOffset++, firstMovement);
    st.setTimestamp(paramOffset++, lastMovement);
    st.setTimestamp(paramOffset++, creationDate);
    st.setString(paramOffset++, casetype);
    st.setLong(paramOffset++, caseID);
    st.execute();
    st.close();
  }

}
