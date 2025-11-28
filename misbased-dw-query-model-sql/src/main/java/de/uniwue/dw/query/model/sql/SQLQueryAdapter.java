package de.uniwue.dw.query.model.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import de.uniwue.dw.core.sql.IDwSqlSchemaConstant;
import de.uniwue.dw.query.model.data.RawQuery;
import de.uniwue.dw.query.model.manager.IQueryIOManager;
import de.uniwue.dw.query.model.manager.adapter.IQueryAdapter;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;
 
public class SQLQueryAdapter extends DatabaseManager implements IDwSqlSchemaConstant, IQueryAdapter {

  private IQueryIOManager manager;

  public SQLQueryAdapter(IQueryIOManager aManager, SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
    manager = aManager;
    createSQLTables();
    readTables();
  }

  public String getTableName() {
    return T_QUERY;
  }

  @Override
  // @formatter:off
  protected String getCreateTableString() {
    String command = getCreateTableStub();
    command += "QueryID INT " + SQLTypes.incrementFlagStartingWith1(sqlManager.config) + " NOT NULL PRIMARY KEY, \n"
            + "XML " + SQLTypes.bigTextType(sqlManager.config) + ", \n"
            + "QueryName VARCHAR(500), \n" 
            + "CreationTime DATETIME, \n" 
            + "ModifyTime DATETIME \n" 
            + ")";
    return command;
  } // @formatter:on

  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
    readResultInternal(resultSet);
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.query.model.sql.IQueryAdapter#insert(java.lang.String, java.lang.String)
   */
  public RawQuery insert(String name, String xml) throws SQLException {
    String command = "INSERT INTO " + getTableName() + "\n"
            + "(queryName, xml, creationTime, ModifyTime) \n" + "VALUES (?, ?, ?, ?)";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    int paramOffset = 1;
    Timestamp currentTime = new Timestamp(System.currentTimeMillis());
    st.setString(paramOffset++, name);
    st.setString(paramOffset++, xml);
    st.setTimestamp(paramOffset++, currentTime);
    st.setTimestamp(paramOffset++, currentTime);
    st.execute();
    st.close();
    Integer generatedIntKey = getGeneratedIntKey(st);
    st.close();
    commit();
    RawQuery result = new RawQuery(generatedIntKey, name, xml, currentTime, currentTime);
    return result;
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.query.model.sql.IQueryAdapter#updateEntry(de.uniwue.dw.query.model.data.RawQuery)
   */
  public void updateEntry(RawQuery aQuery) throws SQLException {
    String command = "UPDATE " + getTableName() + " SET "
            + "xml=?, queryName=?, ModifyTime=? WHERE queryID=?";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    int paramOffset = 1;
    Timestamp currentTime = new Timestamp(System.currentTimeMillis());
    st.setString(paramOffset++, aQuery.getXml());
    st.setString(paramOffset++, aQuery.getName());
    st.setTimestamp(paramOffset++, currentTime);
    st.setInt(paramOffset++, aQuery.getId());
    st.execute();
    st.close();
    commit();
    aQuery.setModifyTime(currentTime);
  }

  /* (non-Javadoc)
   * @see de.uniwue.dw.query.model.sql.IQueryAdapter#deleteEntry(int)
   */
  public void deleteEntry(int id) throws SQLException {
    String command = "DELETE FROM " + getTableName() + " WHERE queryID=?";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    int paramOffset = 1;
    st.setInt(paramOffset++, id);
    st.execute();
    st.close();
    commit();
  }

  protected RawQuery readResultInternal(ResultSet resultSet) throws SQLException {
    int queryID = resultSet.getInt("QueryID");
    String xml = resultSet.getString("xml");
    String name = resultSet.getString("QueryName");
    Timestamp creationTime = resultSet.getTimestamp("creationTime");
    Timestamp modifyTime = resultSet.getTimestamp("ModifyTime");
    if (name == null) {
      name = Integer.toString(queryID);
    }
    RawQuery result = new RawQuery(queryID, name, xml, creationTime, modifyTime);
    manager.add(result);
    return result;
  }

}
