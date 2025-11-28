package de.uniwue.dw.imports.sql.mssql;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

import de.uniwue.dw.imports.manager.PatientManager;
import de.uniwue.dw.imports.sql.SQLPatientImportAdapter;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;

public class MSSQLPatientImportAdapter extends SQLPatientImportAdapter {

  public MSSQLPatientImportAdapter(SQLManager aSqlManager, PatientManager aPatientManager)
          throws SQLException {
    super(aSqlManager, aPatientManager);
  }

  @Override
  // @formatter:off
  protected String getCreateTableString() {
    String command = getCreateTableStub();
    command += "pid BIGINT NOT NULL PRIMARY KEY, \n" 
            + "storno TINYINT, \n"
            + "YOB SMALLINT, \n" 
            + "sex varchar(1), \n" 
            + "importDate " + SQLTypes.timestampType(sqlManager.config) + ", \n" 
            + "importFile VARCHAR(500)); \n"
            + "ALTER TABLE " + getTableName() + " REBUILD WITH (IGNORE_DUP_KEY = ON)";
    return command;
  } // @formatter:on

  public void read() throws SQLException {
    readTables();
  }

  public void insert(long pid, boolean storno, String sex, int yob, String importFile) throws SQLException {
    Timestamp importDate = new Timestamp((new Date()).getTime());

    if (useBulkInserts()) {
      insertByBulk(Long.valueOf(pid), Boolean.valueOf(storno), yob, sex, importDate, importFile);
      return;
    }
    String command = "UPDATE " + getTableName() + " SET "
            + "storno=?, importDate=?, YOB=?, sex=?, importFile=? WHERE PID=?;\n"
            + "IF @@ROWCOUNT=0 \n " + "INSERT INTO " + getTableName() + " "
            + "(storno, importDate, YOB, sex, importFile, pid) VALUES (?, ?, ?, ?, ?, ?)";

    PreparedStatement st = sqlManager.createPreparedStatement(command);

    int paramOffset = 1;

    st.setBoolean(paramOffset++, storno);
    st.setTimestamp(paramOffset++, importDate);
    st.setInt(paramOffset++, yob);
    st.setString(paramOffset++, sex);
    st.setString(paramOffset++, importFile);
    st.setLong(paramOffset++, pid);

    st.setBoolean(paramOffset++, storno);
    st.setTimestamp(paramOffset++, importDate);
    st.setInt(paramOffset++, yob);
    st.setString(paramOffset++, sex);
    st.setString(paramOffset++, importFile);
    st.setLong(paramOffset++, pid);

    st.execute();
    st.close();
  }

}
