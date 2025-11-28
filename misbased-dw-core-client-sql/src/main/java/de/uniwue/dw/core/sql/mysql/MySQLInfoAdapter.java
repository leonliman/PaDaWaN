package de.uniwue.dw.core.sql.mysql;

import de.uniwue.dw.core.sql.SQLInfoAdapter;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;

import java.io.File;
import java.sql.*;

public class MySQLInfoAdapter extends SQLInfoAdapter {

  public MySQLInfoAdapter(SQLManager aSqlManager, File bulkInsertFolderIfNeeded)
          throws SQLException {
    super(aSqlManager, bulkInsertFolderIfNeeded);
  }

  @Override
  // @formatter:off
  protected String getCreateTableString() {
    String command = getCreateTableStub();
    command += "InfoID BIGINT " + SQLTypes.incrementFlagStartingWith1(sqlManager.config) + " PRIMARY KEY NOT NULL, \n" 
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
            + "ValueDec " + SQLTypes.decimalType() + ",\n" 
            + "Storno TINYINT,\n" 
            + "UpdateTime " + SQLTypes.timestampType(sqlManager.config) + ", \n" 
            + "CONSTRAINT " + getTableName() + " UNIQUE (AttrID, PID, MeasureTime, CaseID, Ref, DocID, GroupID),"
            + "INDEX " + getTableName() + "_ValueShort (ValueShort), \n" 
            + "INDEX " + getTableName() + "_ValueDec (ValueDec), \n" 
            + "INDEX " + getTableName() + "_Ref (Ref), \n"
            + "INDEX " + getTableName() + "_Storno_UpdateTime (Storno, UpdateTime), \n"
            + "INDEX " + getTableName() + "_DocID (DocID), \n"
            + "INDEX " + getTableName() + "_GroupID (GroupID), \n"
            + "INDEX " + getTableName() + "_PID (PID), \n" 
            + "INDEX " + getTableName() + "_CaseID (CaseID) \n" + ")";
    return command;
  } // @formatter:on

  private void setInsertParameters(PreparedStatement st, int attrID, long pid, String value,
          String valueShort, double valueDecimal, long ref, Timestamp measureTime, long caseID,
          long docID, long groupID) throws SQLException {
    int paramOffset = 1;
    Timestamp importDate = new Timestamp(System.currentTimeMillis());
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
    st.setLong(paramOffset++, docID);
    st.setLong(paramOffset++, groupID);
    st.setTimestamp(paramOffset++, importDate);
    st.setTimestamp(paramOffset++, importDate);
    st.setLong(paramOffset++, caseID);
  }

  @Override
  protected long insertBySingleStatement(int attrID, long pid, String value, String valueShort,
          double valueDecimal, long ref, Timestamp measureTime, long caseID, long docID,
          long groupID) throws SQLException {
    String command = "INSERT IGNORE " + "INTO " + getTableName() + "\n"
            +
            "(attrID, pid, measureTime, value, valueShort, valueDec, ref, docID, groupID, updateTime, importTime, caseID) \n"
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    PreparedStatement st = sqlManager.createPreparedStatementReturnGeneratedKey(command);
    setInsertParameters(st, attrID, pid, value, valueShort, valueDecimal, ref, measureTime, caseID,
            docID, groupID);
    st.execute();
    long result = getGeneratedLongKey(st);
    st.close();
    return result;
  }

  @Override
  protected long insertOrUpdateBySingleStatement(int attrID, long pid, String value,
          String valueShort, double valueDecimal, long ref, Timestamp measureTime, long caseID,
          long docID, long groupID) throws SQLException {
    String command = "INSERT \n" + "INTO " + getTableName() + "\n"
            + "(attrID, pid, measureTime, value, valueShort, valueDec, ref, docID, groupID, updateTime, caseID) \n"
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" + " ON DUPLICATE KEY UPDATE "
            + "value=?, valueShort=?, valueDec=?, docID=?, groupID=?, ref=?, updateTime=?, caseID=?, storno=?";
    PreparedStatement st = sqlManager.createPreparedStatementReturnGeneratedKey(command);
    setInsertParameters(st, attrID, pid, value, valueShort, valueDecimal, ref, measureTime, caseID,
            docID, groupID);
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
    st.setString(paramOffset++, value);
    st.setString(paramOffset++, valueShort);
    if (Double.isNaN(valueDecimal)) {
      st.setNull(paramOffset++, Types.DECIMAL);
    } else {
      st.setDouble(paramOffset++, valueDecimal);
    }
    st.setLong(paramOffset++, ref);
    st.setLong(paramOffset++, docID);
    st.setLong(paramOffset++, groupID);
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
    st.setLong(paramOffset++, docID);
    st.setLong(paramOffset++, groupID);
    st.setTimestamp(paramOffset++, importDate);
    st.setLong(paramOffset++, caseID);
    st.setNull(paramOffset++, Types.TINYINT);
  }

  @Override
  public String getStornoNoSetQuery() {
    return "(storno IS NULL OR storno = 0)";
  }

  @Override
  public double getStandardDeviationForAllValues(int attrID) throws SQLException {
    String command = "SELECT STD(valueDec) AS 'stdev' FROM " + getTableName() + " WHERE attrID=?";
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
