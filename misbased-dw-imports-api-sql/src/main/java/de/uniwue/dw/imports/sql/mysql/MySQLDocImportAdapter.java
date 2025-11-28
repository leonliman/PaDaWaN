package de.uniwue.dw.imports.sql.mysql;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import de.uniwue.dw.imports.manager.DocManager;
import de.uniwue.dw.imports.sql.SQLDocImportAdapter;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;
import de.uniwue.misc.sql.SQLTypes;

public class MySQLDocImportAdapter extends SQLDocImportAdapter {

  public MySQLDocImportAdapter(SQLManager aSqlManager, DocManager aDocManager) throws SQLException {
    super(aSqlManager, aDocManager);
  }

  @Override
  protected String getCreateTableString() {
    String command = getCreateTableStub();
    command += "docID BIGINT PRIMARY KEY, \n" + "creationTime "
            + SQLTypes.timestampType(sqlManager.config) + ", \n" + "importDate "
            + SQLTypes.timestampType(sqlManager.config) + ", \n" + "Type VARCHAR(200), \n"
            + "caseID BIGINT, \n" + "storno TINYINT, \n" + "PID BIGINT" + ", \n"
            + "importFile VARCHAR(500));\n ";
    return command;
  }

  public void insert(long docid, long caseID, Timestamp creationTime, long pid, String type,
          boolean storno, String importFile) throws SQLException {
    Timestamp importDate = new Timestamp(System.currentTimeMillis());

    if (useBulkInserts() && !SQLPropertiesConfiguration.getSQLBulkInsertMerge()) {
      insertByBulk(Long.valueOf(docid), creationTime, importDate, type, Long.valueOf(caseID),
              Boolean.valueOf(storno), Long.valueOf(pid), importFile);
      return;
    }
    String command = "INSERT INTO " + getTableName() + " "
            + "(caseid, creationTime, pid, type, storno, importDate, docID, importFile) VALUES (?, ?, ?, ?, ?, ?, ?, ?)\n"
            + " ON DUPLICATE KEY "
            + "UPDATE caseID=?, creationTime=?, pid=?, type=?, storno=?, importDate=?, importFile=?";

    PreparedStatement st = sqlManager.createPreparedStatement(command);
    int paramOffset = 1;
    st.setLong(paramOffset++, caseID);
    st.setTimestamp(paramOffset++, creationTime);
    st.setLong(paramOffset++, pid);
    st.setString(paramOffset++, type);
    st.setBoolean(paramOffset++, storno);
    st.setTimestamp(paramOffset++, importDate);
    st.setLong(paramOffset++, docid);
    st.setString(paramOffset++, importFile);

    st.setLong(paramOffset++, caseID);
    st.setTimestamp(paramOffset++, creationTime);
    st.setLong(paramOffset++, pid);
    st.setString(paramOffset++, type);
    st.setBoolean(paramOffset++, storno);
    st.setTimestamp(paramOffset++, importDate);
    st.setString(paramOffset++, importFile);

    st.execute();
    st.close();
  }

}
