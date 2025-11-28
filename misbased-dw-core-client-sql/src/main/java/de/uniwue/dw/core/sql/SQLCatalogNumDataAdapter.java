package de.uniwue.dw.core.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.model.manager.adapter.ICatalogNumDataAdapter;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;

public abstract class SQLCatalogNumDataAdapter extends DatabaseManager
        implements IDwSqlSchemaConstant, ICatalogNumDataAdapter {

  private CatalogManager catalogManager;

  public SQLCatalogNumDataAdapter(SQLManager aSqlManager, CatalogManager aCatalogManager)
          throws SQLException {
    super(aSqlManager);
    catalogManager = aCatalogManager;
    createSQLTables();
    readTables();
  }

  @Override
  public String getTableName() {
    return T_CATALOG_NUMDATA;
  }

  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
    int attrID = resultSet.getInt("attrid");
    Double lowerBound = resultSet.getDouble("LowBound");
    Double upperBound = resultSet.getDouble("HighBound");
    String unit = resultSet.getString("Unit");
    CatalogEntry entry;
    entry = catalogManager.getEntryByID(attrID);
    if (entry != null) {
      entry.setHighBound(upperBound);
      entry.setLowBound(lowerBound);
      entry.setUnit(unit);
    }
  }

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
  // @formatter:off
  protected String getCreateTableString() {
    String command = getCreateTableStub();
    command += "AttrID INT NOT NULL PRIMARY KEY, \n" + "LowBound DECIMAL(18,4), \n"
            + "HighBound DECIMAL(18,4), \n" + "Unit varchar(100) \n)";
    return command;
  } // @formatter:on

  public abstract void insert(Integer attrID, String unit, double lowBound, double highBound)
          throws SQLException;

  /**
   * Baut Insert Statement für die Tabelle DWCatalogNumData zusammen und gibt sie als StringBuffer
   * zurück
   * 
   * @return
   * @throws SQLException
   */
  public StringBuffer dumpDWCatalogNumData() throws SQLException {
    StringBuffer result = new StringBuffer();

    // Daten laden
    PreparedStatement stmt = sqlManager.createPreparedStatement("SELECT * FROM DWCatalogNumData");
    ResultSet rs = stmt.executeQuery();

    // Metadaten der Tabelle betrachten
    ResultSetMetaData metaData = rs.getMetaData();
    int columnCount = metaData.getColumnCount();

    Object value;
    int countInserts = 0;
    String outputValue;

    // Jeden Datensatz durchlaufen
    while (rs.next()) {
      // Allgemeiner Insert Teil als String hinzufügen
      result.append("INSERT DWCatalogNumData ([AttrID], [LowBound], [HighBound], [unit]) VALUES (");
      for (int i = 0; i < columnCount; i++) {
        value = rs.getObject(i + 1);
        // Einzelne Werte hinzufügen, abhängig vom Datenfeld
        if (value == null) {
          result.append("NULL");
        } else {
          if (i > 0) {
            result.append(", ");
          }
          switch (i) {
            case 0:
              // AttrID
              result.append(value.toString());
              break;
            case 1:
              // LowBound
              result.append("CAST(" + value.toString() + " AS Decimal(18, 4))");
              break;
            case 2:
              // High Bound
              result.append("CAST(" + value.toString() + " AS Decimal(18, 4))");
              break;
            case 3:
              // unit
              outputValue = value.toString();
              outputValue = outputValue.replaceAll("'", " ");
              result.append("'" + outputValue + "'");
              break;
          }
        }
      }
      result.append(")\n");
      countInserts++;
      // Nach 50 Insert Statements ein GO Befehl Einfügen, damit nicht alle Statements auf einmal
      // ausgeführt werden
      if (countInserts >= 50) {
        result.append("GO\n");
        countInserts = 0;
      }
    }
    rs.close();
    stmt.close();
    return result;
  }

}
