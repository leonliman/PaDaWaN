package de.uniwue.dw.core.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.core.model.manager.InfoIterator;
import de.uniwue.dw.core.model.manager.adapter.IDeleteAdapter;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;

// When Infos from the DWInfo-Table are deleted the deleted InfoIDs and their PIDs and CaseIDs are stored 
// in the DWDeleteTable. From there they can be used by other services who like to know what has been deleted.
// The table for this adapter has to be manually deleted or by the other services if needed as this is not done here.
public abstract class SQLDeleteAdapter extends DatabaseManager
        implements IDwSqlSchemaConstant, IDeleteAdapter {

  public SQLDeleteAdapter(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
    createSQLTables();
  }

  @Override
  public String getTableName() {
    return T_IMPORT_DELETE;
  }

  @Override
  public List<Integer> getAttrIDsOfDeletedInfosAfterTime(Timestamp timestamp) throws SQLException {
    List<Integer> result = new ArrayList<Integer>();
    if (getColumnNames().contains("attrid".toLowerCase())) {
      String sql = "SELECT DISTINCT attrID FROM " + IDwSqlSchemaConstant.T_IMPORT_DELETE
              + " i WHERE i.deleteTime >= ?";
      PreparedStatement st = sqlManager.createPreparedStatement(sql);
      int paramOffset = 1;
      st.setTimestamp(paramOffset++, timestamp);
      ResultSet resultSet = st.executeQuery();
      while (resultSet.next()) {
        int id = resultSet.getInt("attrid");
        result.add(id);
      }
      resultSet.close();
      st.close();
    }
    return result;
  }

  public Information getInfo(ResultSet resultSet) throws SQLException {
    Information result = new Information();
    result.setImportTime(resultSet.getTimestamp("ImportTime"));
    result.setPid(resultSet.getLong("PID"));
    result.setCaseID(resultSet.getLong("CaseID"));
    result.setInfoID(resultSet.getLong("InfoID"));
    if (getColumnNames().contains("attrid".toLowerCase())) {
      result.setAttrID(resultSet.getInt("AttrID"));
    }
    if (getColumnNames().contains("ref".toLowerCase())) {
      result.setRef(resultSet.getLong("Ref"));
    }
    if (getColumnNames().contains("measuretime".toLowerCase())) {
      result.setMeasureTime(resultSet.getTimestamp("MeasureTime"));
    }
    return result;
  }

  @Override
  public List<Information> getIDsAfterTimeForDeletedInfos(Timestamp timestamp) throws SQLException {
    String sql = "SELECT pid, caseid, DeleteTime AS ImportTime, InfoID";
    // the stuff with the attrID is because of downward compatibilities
    if (getColumnNames().contains("attrid".toLowerCase())) {
      sql += ", AttrID";
    }
    if (getColumnNames().contains("ref".toLowerCase())) {
      sql += ", Ref";
    }
    if (getColumnNames().contains("measuretime".toLowerCase())) {
      sql += ", MeasureTime";
    }
    sql += " FROM " + IDwSqlSchemaConstant.T_IMPORT_DELETE + " i WHERE i.deleteTime >= ?";
    PreparedStatement st = sqlManager.createPreparedStatement(sql);
    int paramOffset = 1;
    if (timestamp != null) {
      st.setTimestamp(paramOffset++, timestamp);
    }
    ResultSet resultSet = st.executeQuery();
    List<Information> result = new ArrayList<Information>();
    while (resultSet.next()) {
      Information anInfo = getInfo(resultSet);
      result.add(anInfo);
    }
    resultSet.close();
    st.close();
    return result;
  }

  @Override
  public void markInfosDeleted(InfoIterator anIter) throws SQLException {
    for (Information anInfo : anIter) {
      markInfoDeleted(anInfo.getAttrID(), anInfo.getPid(), anInfo.getCaseID(), anInfo.getMeasureTime());
    }
  }
  
  public void markInfoDeleted(int attrID, long pid, long caseID, Timestamp measureTime)
          throws SQLException {
    String command = "INSERT INTO " + getTableName() + " (infoID, deletetime, caseid, pid";
    // the stuff with the attrID is because of downward compatibilities
    if (getColumnNames().contains("attrid".toLowerCase())) {
      command += ", AttrID";
    }
    if (getColumnNames().contains("ref".toLowerCase())) {
      command += ", Ref";
    }
    if (getColumnNames().contains("measuretime".toLowerCase())) {
      command += ", MeasureTime";
    }
    command += ") " + "SELECT infoID, ?, caseID, pid";
    if (getColumnNames().contains("attrid".toLowerCase())) {
      command += ", AttrID";
    }
    if (getColumnNames().contains("ref".toLowerCase())) {
      command += ", Ref";
    }
    if (getColumnNames().contains("measuretime".toLowerCase())) {
      command += ", MeasureTime";
    }
    command += " FROM " + IDwSqlSchemaConstant.T_INFO
            + " WHERE attrID=? AND pid=? AND measureTime=?";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    Timestamp deleteTimestamp = new Timestamp(System.currentTimeMillis());
    measureTime = SQLTypes.getMinTimestamp(measureTime);
    int paramOffset = 1;
    st.setTimestamp(paramOffset++, deleteTimestamp);
    st.setInt(paramOffset++, attrID);
    st.setLong(paramOffset++, pid);
    st.setTimestamp(paramOffset++, measureTime);
    st.execute();
    st.close();
    commit();
  }

  public void markInfosForProjectDeleted(String project) throws SQLException {
    String command = "INSERT INTO " + getTableName() + " (infoID, deletetime, caseid, pid";
    // the stuff with the attrID is because of downward compatibilities
    if (getColumnNames().contains("attrid".toLowerCase())) {
      command += ", AttrID";
    }
    if (getColumnNames().contains("ref".toLowerCase())) {
      command += ", Ref";
    }
    if (getColumnNames().contains("measuretime".toLowerCase())) {
      command += ", MeasureTime";
    }
    command += ") " + "SELECT infoID, ?, caseID, pid";
    if (getColumnNames().contains("attrid".toLowerCase())) {
      command += ", AttrID";
    }
    if (getColumnNames().contains("ref".toLowerCase())) {
      command += ", Ref";
    }
    if (getColumnNames().contains("measuretime".toLowerCase())) {
      command += ", MeasureTime";
    }
    command += " FROM " + IDwSqlSchemaConstant.T_INFO + " WHERE attrID IN (SELECT attrID FROM "
            + IDwSqlSchemaConstant.T_CATALOG + " WHERE project=?)";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    Timestamp importDate = new Timestamp(System.currentTimeMillis());
    int paramOffset = 1;
    st.setTimestamp(paramOffset++, importDate);
    st.setString(paramOffset++, project);
    st.execute();
    st.close();
  }

  public void markInfosForAttrIDDeleted(int attrID) throws SQLException {
    String command = "INSERT INTO " + getTableName() + " (infoID, deletetime, caseid, pid";
    // the stuff with the attrID is because of downward compatibilities
    if (getColumnNames().contains("attrid".toLowerCase())) {
      command += ", AttrID";
    }
    if (getColumnNames().contains("ref".toLowerCase())) {
      command += ", Ref";
    }
    if (getColumnNames().contains("measuretime".toLowerCase())) {
      command += ", MeasureTime";
    }
    command += ") " + "SELECT infoID, ?, caseID, pid";
    if (getColumnNames().contains("attrid".toLowerCase())) {
      command += ", AttrID";
    }
    if (getColumnNames().contains("ref".toLowerCase())) {
      command += ", Ref";
    }
    if (getColumnNames().contains("measuretime".toLowerCase())) {
      command += ", MeasureTime";
    }
    command += " FROM " + IDwSqlSchemaConstant.T_INFO + " WHERE attrID=?";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    Timestamp importDate = new Timestamp(System.currentTimeMillis());
    int paramOffset = 1;
    st.setTimestamp(paramOffset++, importDate);
    st.setInt(paramOffset++, attrID);
    st.execute();
    st.close();
  }

  public void markInfoDeleted(int attrID, long pid, long caseID, Timestamp measureTime,
          String infoTableName) throws SQLException {
    String command = "INSERT INTO " + getTableName() + " (infoID, deletetime, caseid, pid";
    // the stuff with the attrID is because of downward compatibilities
    if (getColumnNames().contains("attrid".toLowerCase())) {
      command += ", AttrID";
    }
    if (getColumnNames().contains("ref".toLowerCase())) {
      command += ", Ref";
    }
    if (getColumnNames().contains("measuretime".toLowerCase())) {
      command += ", MeasureTime";
    }
    command += ") " + "SELECT infoID, ?, caseID, pid";
    if (getColumnNames().contains("attrid".toLowerCase())) {
      command += ", AttrID";
    }
    if (getColumnNames().contains("ref".toLowerCase())) {
      command += ", Ref";
    }
    if (getColumnNames().contains("measuretime".toLowerCase())) {
      command += ", MeasureTime";
    }
    command += " FROM " + infoTableName + " WHERE attrID=? AND pid=? AND measureTime=?";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    Timestamp deleteTimestamp = new Timestamp(System.currentTimeMillis());
    measureTime = SQLTypes.getMinTimestamp(measureTime);
    int paramOffset = 1;
    st.setTimestamp(paramOffset++, deleteTimestamp);
    st.setInt(paramOffset++, attrID);
    st.setLong(paramOffset++, pid);
    st.setTimestamp(paramOffset++, measureTime);
    st.execute();
    st.close();
    commit();
  }

  public void markInfosForProjectDeleted(String project, String catalogTableName,
          String infoTableName) throws SQLException {
    String command = "INSERT INTO " + getTableName() + " (infoID, deletetime, caseid, pid";
    // the stuff with the attrID is because of downward compatibilities
    if (getColumnNames().contains("attrid".toLowerCase())) {
      command += ", AttrID";
    }
    if (getColumnNames().contains("ref".toLowerCase())) {
      command += ", Ref";
    }
    if (getColumnNames().contains("measuretime".toLowerCase())) {
      command += ", MeasureTime";
    }
    command += ") " + "SELECT infoID, ?, caseID, pid";
    if (getColumnNames().contains("attrid".toLowerCase())) {
      command += ", AttrID";
    }
    if (getColumnNames().contains("ref".toLowerCase())) {
      command += ", Ref";
    }
    if (getColumnNames().contains("measuretime".toLowerCase())) {
      command += ", MeasureTime";
    }
    command += " FROM " + infoTableName + " WHERE attrID IN (SELECT attrID FROM " + catalogTableName
            + " WHERE project=?)";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    Timestamp importDate = new Timestamp(System.currentTimeMillis());
    int paramOffset = 1;
    st.setTimestamp(paramOffset++, importDate);
    st.setString(paramOffset++, project);
    st.execute();
    st.close();
  }

  public void insertInfoIDsWithoutCatalogEntry(String infoTableName, String catalogTableName)
          throws SQLException {
    String command = "INSERT INTO " + getTableName() + " (infoID, deletetime, caseid, pid";
    // the stuff with the attrID is because of downward compatibilities
    if (getColumnNames().contains("attrid".toLowerCase())) {
      command += ", AttrID";
    }
    if (getColumnNames().contains("ref".toLowerCase())) {
      command += ", Ref";
    }
    if (getColumnNames().contains("measuretime".toLowerCase())) {
      command += ", MeasureTime";
    }
    command += ") " + "SELECT infoID, ?, caseID, pid";
    if (getColumnNames().contains("attrid".toLowerCase())) {
      command += ", AttrID";
    }
    if (getColumnNames().contains("ref".toLowerCase())) {
      command += ", Ref";
    }
    if (getColumnNames().contains("measuretime".toLowerCase())) {
      command += ", MeasureTime";
    }
    command += " FROM " + infoTableName + " WHERE attrID NOT IN (SELECT attrID FROM "
            + catalogTableName + ")";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    int paramOffset = 1;
    Timestamp importDate = new Timestamp(System.currentTimeMillis());
    st.setTimestamp(paramOffset++, importDate);
    st.execute();
    st.close();
  }

  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
  }

}
