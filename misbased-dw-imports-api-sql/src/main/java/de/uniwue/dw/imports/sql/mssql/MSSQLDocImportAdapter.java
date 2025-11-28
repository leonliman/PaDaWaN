package de.uniwue.dw.imports.sql.mssql;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import de.uniwue.dw.imports.manager.DocManager;
import de.uniwue.dw.imports.sql.SQLDocImportAdapter;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;

public class MSSQLDocImportAdapter extends SQLDocImportAdapter {

  public MSSQLDocImportAdapter(SQLManager aSqlManager, DocManager aDocManager) throws SQLException {
    super(aSqlManager, aDocManager);
  }

  @Override
  // @formatter:off
  protected String getCreateTableString() {
    String command = getCreateTableStub();
    command += "docID BIGINT PRIMARY KEY, \n" 
            + "creationTime " + SQLTypes.timestampType(sqlManager.config) + ", \n" 
            + "importDate " + SQLTypes.timestampType(sqlManager.config) + ", \n" 
            + "Type VARCHAR(200), \n"
            + "caseID BIGINT, \n" 
            + "storno TINYINT, \n" 
            + "PID BIGINT, \n"
            + "importFile VARCHAR(500));\n " 
            + "ALTER TABLE " + getTableName() + " REBUILD WITH (IGNORE_DUP_KEY = ON)";
    return command;
  } // @formatter:on

  public void insert(long docid, long caseID, Timestamp creationTime, long pid, String type,
          boolean storno, String importFile) throws SQLException {
    Timestamp importDate = new Timestamp(System.currentTimeMillis());

    if (useBulkInserts()) {
      insertByBulk(Long.valueOf(docid), creationTime, importDate, type, Long.valueOf(caseID),
              Boolean.valueOf(storno), Long.valueOf(pid), importFile);
      return;
    }
    String command = "UPDATE " + getTableName() + " SET "
            + "caseID=?, creationTime=?, pid=?, type=?, storno=?, importDate=?, importFile=? WHERE docID=?;\n"
            + "IF @@ROWCOUNT=0 \n " + "INSERT INTO " + getTableName() + " "
            + "(caseid, creationTime, pid, type, storno, importDate, docID, importFile) VALUES (?, ?, ?, ?, ?, ?, ?, ?)\n";

    PreparedStatement st = sqlManager.createPreparedStatement(command);
    int paramOffset = 1;
    st.setLong(paramOffset++, caseID);
    st.setTimestamp(paramOffset++, creationTime);
    st.setLong(paramOffset++, pid);
    st.setString(paramOffset++, type);
    st.setBoolean(paramOffset++, storno);
    st.setTimestamp(paramOffset++, importDate);
    st.setString(paramOffset++, importFile);
    st.setLong(paramOffset++, docid);

    st.setLong(paramOffset++, caseID);
    st.setTimestamp(paramOffset++, creationTime);
    st.setLong(paramOffset++, pid);
    st.setString(paramOffset++, type);
    st.setBoolean(paramOffset++, storno);
    st.setTimestamp(paramOffset++, importDate);
    st.setLong(paramOffset++, docid);
    st.setString(paramOffset++, importFile);

    st.execute();
    st.close();
  }

}
