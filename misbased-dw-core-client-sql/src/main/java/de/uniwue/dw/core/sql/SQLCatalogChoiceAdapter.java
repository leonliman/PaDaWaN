package de.uniwue.dw.core.sql;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.model.manager.adapter.ICatalogChoiceAdapter;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SQLCatalogChoiceAdapter extends DatabaseManager
        implements IDwSqlSchemaConstant, ICatalogChoiceAdapter {

  private CatalogManager catalogManager;

  public SQLCatalogChoiceAdapter(SQLManager aSqlManager, CatalogManager aCatalogManager)
          throws SQLException {
    super(aSqlManager);
    catalogManager = aCatalogManager;
    createSQLTables();
  }

  @Override
  public String getTableName() {
    return T_CATALOG_CHOICES;
  }

  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
    int attrID = resultSet.getInt("attrid");
    String choice = resultSet.getString("choice");
    CatalogEntry entry = catalogManager.getEntryByID(attrID);
    if (entry != null) {
      entry.addSingleChoiceChoice(choice);
    }
  }

  @Override
  // @formatter:off
  protected String getCreateTableString() {
    String command = getCreateTableStub();
    command += 
            "ChoiceID INT NOT NULL " + SQLTypes.incrementFlagStartingWith1(sqlManager.config)+ " PRIMARY KEY, \n" + 
            "AttrID INT NOT NULL, \n" + 
            "Choice VARCHAR(200) NOT NULL)";
    return command;
  } // @formatter:on

  @Override
  public void delete(Integer attrID) throws SQLException {
    String command = "DELETE FROM " + getTableName() + " " + " where AttrID=?";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    int paramOffset = 1;
    st.setInt(paramOffset++, attrID);
    st.execute();
    st.close();
    commit();
  }

  @Override
  public void insert(Integer attrID, String choice) throws SQLException {
    if (choice == null) {
      System.err.println("Tried to insert null value as choice for AttrID " + attrID);
      return;
    } else if (choice.length() >= 200) {
      System.err.println("Tried to insert a choice value with more than 200 characters for AttrID " + attrID
              + " (value: '" + choice + "')");
      return;
    }
    String command = "INSERT INTO " + getTableName() + " " + "(AttrID, Choice) values (?, ?)";
    PreparedStatement st = sqlManager.createPreparedStatementReturnGeneratedKey(command);
    int paramOffset = 1;
    st.setInt(paramOffset++, attrID);
    st.setString(paramOffset++, choice);
    st.execute();
    st.close();
    commit();
  }

}
