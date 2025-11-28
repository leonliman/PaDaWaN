package de.uniwue.dw.core.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.model.manager.adapter.ICatalogCountAdapter;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;

/**
 * This adapter manages the table in which the counts for all catalog entries are managed. Each
 * entry has a count for its absolute quantity (quantitityAbsolute), its quantity of patients having
 * at least one fact for this catalog entry (quantityPID) and a quantity of cases having at least
 * one fact for this entry (quantity). The adapter has methods doing the count calculations himself
 * but the table can as well be filled by other modules using insertOrUpdateQuantity
 */
public abstract class SQLCatalogCountAdapter extends DatabaseManager
        implements IDwSqlSchemaConstant, ICatalogCountAdapter {

  private CatalogManager catalogManager;

  public SQLCatalogCountAdapter(SQLManager aSqlManager, CatalogManager aCatalogManager)
          throws SQLException {
    super(aSqlManager);
    catalogManager = aCatalogManager;
    createSQLTables();
  }

  @Override
  public String getTableName() {
    return T_CATALOG_COUNT;
  }

  public abstract void insertOrUpdateCounts(int attrid, long pidCount, long caseIDCount,
          long absoluteCount) throws SQLException;

  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
    int attrID = resultSet.getInt("attrid");
    long quantityCases = resultSet.getLong("quantity");
    long quantityAbsolute = resultSet.getLong("quantityAbsolute");
    long quantityPID = resultSet.getLong("quantityPID");
    CatalogEntry entryByID;
    entryByID = catalogManager.getEntryByID(attrID);
    if (entryByID != null) {
      entryByID.setCountAbsolute(quantityAbsolute);
      entryByID.setCountDistinctCaseID(quantityCases);
      entryByID.setCountDistinctPID(quantityPID);
    }
  }

  // @formatter:off
  @Override
  protected String getCreateTableString() {
    String command = getCreateTableStub();
    command += "AttrID INT NOT NULL PRIMARY KEY, \n" 
            + "quantity BIGINT, \n"
            + "quantityAbsolute BIGINT, \n" 
            + "quantityPID BIGINT \n" 
            + ")";
    return command;
  } // @formatter:on

  /**
   * This method creates a SQL-statement that calculates all attridID from the catalog with all
   * sibling attrids they have
   */
  // @formatter:off
  private String getCollectedSiblingAttrIDs() {
    String result = "select attrid, attrid as childID from "
            + T_CATALOG + "\n"
            + "union\n"
            + "select ParentID, attrid from "
            + T_CATALOG + "\n"
            + "union\n"
            + "select c1.ParentID, c2.AttrID from "
            + T_CATALOG + " c1, " + T_CATALOG + " c2 "
            + "where c1.AttrID = c2.ParentID\n"
            + "union\n"
            + "select c1.ParentID, c3.AttrID from "
            + T_CATALOG + " c1, " + T_CATALOG + " c2, " + T_CATALOG + " c3 "
            + "where c1.AttrID = c2.ParentID and c2.AttrID = c3.ParentID\n"
            + "union\n"
            + "select c1.ParentID, c4.AttrID from "
            + T_CATALOG + " c1, " + T_CATALOG + " c2, " + T_CATALOG + " c3, " + T_CATALOG + " c4 "
            + "where c1.AttrID = c2.ParentID and c2.AttrID = c3.ParentID and c3.AttrID = c4.ParentID\n"
            + "union\n"
            + "select c1.ParentID, c5.AttrID from "
            + T_CATALOG + " c1, " + T_CATALOG + " c2, " + T_CATALOG + " c3, " + T_CATALOG + " c4, " + T_CATALOG + " c5 "
            + "where c1.AttrID = c2.ParentID and c2.AttrID = c3.ParentID and c3.AttrID = c4.ParentID and c4.AttrID = c5.ParentID \n"
            + "union\n"
            + "select c1.ParentID, c6.AttrID from "
            + T_CATALOG + " c1, " + T_CATALOG + " c2, " + T_CATALOG + " c3, " + T_CATALOG + " c4, "
            + T_CATALOG + " c5, " + T_CATALOG + " c6 "
            + "where c1.AttrID = c2.ParentID and c2.AttrID = c3.ParentID and c3.AttrID = c4.ParentID and c4.AttrID = c5.ParentID and "
            + "c5.AttrID = c6.ParentID \n"
            + "union\n"
            + "select c1.ParentID, c7.AttrID from "
            + T_CATALOG + " c1, " + T_CATALOG + " c2, " + T_CATALOG + " c3, " + T_CATALOG + " c4, "
            + T_CATALOG + " c5, " + T_CATALOG + " c6, " + T_CATALOG + " c7 "
            + "where c1.AttrID = c2.ParentID and c2.AttrID = c3.ParentID and c3.AttrID = c4.ParentID and c4.AttrID = c5.ParentID and "
            + "c5.AttrID = c6.ParentID and c6.AttrID = c7.ParentID \n"
            + "union\n"
            + "select c1.ParentID, c8.AttrID from "
            + T_CATALOG + " c1, " + T_CATALOG + " c2, " + T_CATALOG + " c3, " + T_CATALOG + " c4, "
            + T_CATALOG + " c5, " + T_CATALOG + " c6, " + T_CATALOG + " c7, " + T_CATALOG + " c8 "
            + "where c1.AttrID = c2.ParentID and c2.AttrID = c3.ParentID and c3.AttrID = c4.ParentID and c4.AttrID = c5.ParentID and "
            + "c5.AttrID = c6.ParentID and c6.AttrID = c7.ParentID and c7.AttrID = c8.ParentID \n"
            + "union\n"
            + "select c1.ParentID, c9.AttrID from "
            + T_CATALOG + " c1, " + T_CATALOG + " c2, " + T_CATALOG + " c3, " + T_CATALOG + " c4, "
            + T_CATALOG + " c5, " + T_CATALOG + " c6, " + T_CATALOG + " c7, " + T_CATALOG + " c8, " + T_CATALOG + " c9 "
            + "where c1.AttrID = c2.ParentID and c2.AttrID = c3.ParentID and c3.AttrID = c4.ParentID and c4.AttrID = c5.ParentID and "
            + "c5.AttrID = c6.ParentID and c6.AttrID = c7.ParentID and c7.AttrID = c8.ParentID and c8.AttrID = c9.ParentID \n"
            + "union\n"
            + "select c1.ParentID, c10.AttrID from "
            + T_CATALOG + " c1, " + T_CATALOG + " c2, " + T_CATALOG + " c3, " + T_CATALOG + " c4, "
            + T_CATALOG + " c5, " + T_CATALOG + " c6, " + T_CATALOG + " c7, " + T_CATALOG + " c8, "
            + T_CATALOG + " c9, " + T_CATALOG + " c10 "
            + "where c1.AttrID = c2.ParentID and c2.AttrID = c3.ParentID and c3.AttrID = c4.ParentID and c4.AttrID = c5.ParentID and "
            + "c5.AttrID = c6.ParentID and c6.AttrID = c7.ParentID and c7.AttrID = c8.ParentID and c8.AttrID = c9.ParentID and "
            + "c9.AttrID = c10.ParentID \n"
            + "union\n"
            + "select c1.ParentID, c11.AttrID from "
            + T_CATALOG + " c1, " + T_CATALOG + " c2, " + T_CATALOG + " c3, " + T_CATALOG + " c4, "
            + T_CATALOG + " c5, " + T_CATALOG + " c6, " + T_CATALOG + " c7, " + T_CATALOG + " c8, "
            + T_CATALOG + " c9, " + T_CATALOG + " c10, " + T_CATALOG + " c11 "
            + "where c1.AttrID = c2.ParentID and c2.AttrID = c3.ParentID and c3.AttrID = c4.ParentID and c4.AttrID = c5.ParentID and "
            + "c5.AttrID = c6.ParentID and c6.AttrID = c7.ParentID and c7.AttrID = c8.ParentID and c8.AttrID = c9.ParentID and "
            + "c9.AttrID = c10.ParentID and c10.AttrID = c11.ParentID \n";
    return result;
  } // @formatter:on

  // @formatter:off
  public void calculateCountsBasedOnCaseIDs()
          throws SQLException {
    emptyTable();
    String command = "INSERT INTO " + getTableName() + " (attrid, quantity) "
            + "SELECT attrid, count(*) FROM (\n" + "SELECT children.attrid, caseid FROM "
            + T_INFO + ", \n" + "(" + getCollectedSiblingAttrIDs()
            + ") children\n" + "WHERE children.childID = " + T_INFO + ".AttrID\n"
            + "GROUP BY children.attrid, caseid\n" + ") s GROUP BY attrid\n" + "";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    st.execute();
    st.close();
    commit();
  } // @formatter:on

  // @formatter:off
  public void calculateAbsoluteCounts()
          throws SQLException {
    String command = "UPDATE " + getTableName()
            + " SET quantityAbsolute = countAbsolute FROM " + getTableName() + " "
            + "INNER JOIN (" + "SELECT attrid, count(*) AS countAbsolute FROM (\n"
            + "SELECT children.attrid from " + T_INFO + ", \n" + "("
            + getCollectedSiblingAttrIDs() + ") children\n"
            + "WHERE children.childID = " + T_INFO + ".AttrID\n"
            + ") s GROUP BY attrid) t ON t.attrid = " + getTableName() + ".attrid\n"
            + "";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    st.execute();
    st.close();
    commit();
  } // @formatter:on

  // @formatter:off
  public void calculateCountsBasedOnPIDs()
          throws SQLException {
    String command = "UPDATE " + getTableName() + " SET quantityPID = countPID FROM "
            + getTableName() + " " + "INNER JOIN ("
            + "SELECT attrid, count(*) AS countPID FROM (\n" + "SELECT children.attrid, pid from "
            + T_INFO + ", \n" + "(" + getCollectedSiblingAttrIDs()
            + ") children\n" + "WHERE children.childID = " + T_INFO + ".AttrID\n"
            + "GROUP BY children.attrid, pid\n" + ") s GROUP BY attrid) t ON t.attrid = "
            + getTableName() + ".attrid\n" + "";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    st.execute();
    st.close();
    commit();
  } // @formatter:on

  public void delete(Integer attrID) throws SQLException {
    String command = "DELETE FROM " + getTableName() + " " + " where AttrID=?";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    int paramOffset = 1;
    st.setInt(paramOffset++, attrID);
    st.execute();
    st.close();
    commit();
  }

}
