package de.uniwue.dw.imports.sql.mssql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;

import de.uniwue.dw.core.model.manager.InfoManager;
import de.uniwue.dw.core.sql.SQLInfoAdapter;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.manager.adapter.IRefIDAdapter;
import de.uniwue.dw.imports.sql.SQLRefIDAdapter;
import de.uniwue.misc.sql.SQLManager;

public class MSSQLRefIDAdapter extends SQLRefIDAdapter implements IRefIDAdapter {

  public MSSQLRefIDAdapter(SQLManager aSqlManager, InfoManager ainfoManager) throws SQLException {
    super(aSqlManager, ainfoManager);
  }

  public void dispose() {
  }

  @Override
  protected String getCreateTableString() {
    String command = getCreateTableStub();
    command += "" + "Ref BIGINT PRIMARY KEY IDENTITY (1000000000000000000,1), addRefID TINYINT"
            + ") \n" + "CREATE INDEX " + getTableName() + "_Ref on " + getTableName() + " (Ref); \n"
            + "ALTER TABLE " + getTableName() + " REBUILD WITH (IGNORE_DUP_KEY = ON)";
    return command;
  }

  public Long getUsedRefID(int attrId, long pid, long caseid, Timestamp measureDate,
          boolean ignoreNanoseconds) throws SQLException {
    PreparedStatement st = null;
    ResultSet resultSet;
    Long returnRef = null;

    SQLInfoAdapter infoad = (SQLInfoAdapter) super.infoManager.infoAdapter;
    String command = "SELECT ref FROM " + infoad.getTableName()
            + " WHERE AttrID=? and pid=? and caseID = ?";
    if (!ignoreNanoseconds) {
      command += "and MeasureTime=CAST('" + measureDate + "' as datetime2)  ";

    } else {
      command += "and MeasureTime>CAST('" + measureDate
              + "' as datetime2) and MeasureTime < DATEADD(s,1,CAST('" + measureDate
              + "' as datetime2))  ";
    }
    try {

      // sqlManagerForRef.autoCommitEnable();
      st = sqlManagerForRef.createPreparedStatement(command);
      st.setLong(1, attrId);
      st.setLong(2, pid);
      st.setLong(3, caseid);
      // st.setTimestamp(4, measureDate);
      resultSet = st.executeQuery();
      while (resultSet.next()) {
        returnRef = resultSet.getLong("ref");
        break;
      }
      resultSet.close();
      st.close();
      // sqlManagerForRef.autoCommitDisable();
    } catch (SQLException e) {
      throw new SQLException(e.getMessage() + "; SQL: " + command, e);
    }
    return returnRef;
  }

  public long getNonUsedRefID() throws SQLException {
    long result = -1L;

    // getSQLManager().autoCommitEnable();

    Statement st;
    String command = "";
    command += "INSERT INTO " + getTableName() + " " + " (addRefID) " + " VALUES (1) ";
    st = sqlManagerForRef.createStatement();
    // st.setInt(1, 1);
    st.executeUpdate(command, Statement.RETURN_GENERATED_KEYS);
    ResultSet rs = st.getGeneratedKeys();
    if (rs != null && rs.next()) {
      result = rs.getLong(1);
    }
    rs.close();
    st.close();
    // getSQLManager().autoCommitDisable();
    return result;
  }

  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public Long getUsedOrNewRefID(int attrId, long pid, long caseid, Timestamp measureDate,
          boolean ignoreNanoseconds) throws ImportException {
    Long toRet = null;
    try {
      if (refsOfCurrentImport == null) {
        refsOfCurrentImport = new HashMap<String, Long>();
      }
      String key = Integer.toString(attrId) + Long.toString(pid) + Long.toString(caseid)
              + measureDate.toString();

      if (refsOfCurrentImport.containsKey(key)) {
        toRet = refsOfCurrentImport.get(key);

      } else {
        toRet = getUsedRefID(attrId, pid, caseid, measureDate, ignoreNanoseconds);

        if (toRet == null) {
          toRet = getNonUsedRefID();
        }

        refsOfCurrentImport.put(key, toRet);

      }
    } catch (SQLException e) {
      ImportException tmp = new ImportException(ImportExceptionType.SQL_ERROR,
              "error while getting refid", e);
      throw tmp;
    }
    return toRet;
  }

}
