package de.uniwue.dw.core.sql.mssql;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.sql.SQLCatalogAdapter;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class MSSQLCatalogAdapter extends SQLCatalogAdapter {

  public MSSQLCatalogAdapter(SQLManager aSqlManager, CatalogManager aCatalogManager)
          throws SQLException {
    super(aSqlManager, aCatalogManager);
  }

  @Override
  // @formatter:off
  protected String getCreateTableString() {
    String command = getCreateTableStub();
    command += "AttrID INT IDENTITY(1, 1) NOT NULL PRIMARY KEY, \n"
            + "Name VARCHAR(" + SQLCatalogAdapter.nameColumnSize + "), \n" 
            + "ExtID VARCHAR(" + SQLCatalogAdapter.extIDColumnSize + ") NOT NULL, \n" 
            + "ParentID INT, \n"
            + "OrderValue DECIMAL(12,4), \n" 
            + "DataType VARCHAR(50), \n"
            + "Project VARCHAR(" + SQLCatalogAdapter.projectColumnSize + ") NOT NULL, \n" 
            + "CreationTime " + SQLTypes.timestampType(sqlManager.config) + ", \n"
            + "UniqueName VARCHAR(" + SQLCatalogAdapter.uniqueNameColumnSize + "), \n" 
            + "Description TEXT \n"
            + ") \n" 
            + "CREATE UNIQUE INDEX " + getTableName() + "_ExtID_Project on " 
            + getTableName() + " (ExtID, Project); \n";
    return command;
  } // @formatter:on

  @Override
  protected CatalogEntry insertEntry(int attrID, String name, CatalogEntryType dataType,
          String extID, int parentID, double orderValue, String aProject, Timestamp creationTime,
          String uniqueName, String description) throws SQLException {
    PreparedStatement st;
    String command = "";
    CatalogEntry result = null;

    if (attrID != -1) {
      sqlManager.executeSQL("SET identity_insert " + getTableName() + " on");
    }
    command += "IF NOT EXISTS (SELECT * FROM " + getTableName()
            + " WHERE extID=? AND project=?) INSERT INTO " + getTableName() + "\n"
            + "(name, dataType, extId, parentID, orderValue, project, creationTime, uniqueName, description";
    if (attrID != -1) {
      command += ", attrID";
    }
    command += ") \n" + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?";
    if (attrID != -1) {
      command += ", ?";
    }
    command += ")";
    st = sqlManager.createPreparedStatementReturnGeneratedKey(command);
    int paramOffset = 1;
    st.setString(paramOffset++, extID);
    st.setString(paramOffset++, aProject);
    st.setString(paramOffset++, name);
    st.setString(paramOffset++, dataType.toString());
    st.setString(paramOffset++, extID);
    st.setInt(paramOffset++, parentID);
    st.setDouble(paramOffset++, orderValue);
    st.setString(paramOffset++, aProject);
    st.setTimestamp(paramOffset++, creationTime);
    st.setString(paramOffset++, uniqueName);
    st.setString(paramOffset++, description);
    if (attrID != -1) {
      st.setInt(paramOffset++, attrID);
    }
    st.execute();
    int genID;
    if (attrID == -1) {
      genID = getGeneratedIntKey(st);
    } else {
      genID = attrID;
    }
    st.close();
    result = new CatalogEntry(genID, name, dataType, extID, parentID, orderValue, aProject,
            uniqueName, description, creationTime);
    if (attrID != -1) {
      sqlManager.executeSQL("SET identity_insert " + getTableName() + " OFF");
    }
    return result;
  }

}
