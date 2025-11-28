package de.uniwue.dw.core.sql;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.model.manager.adapter.ICatalogAdapter;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;

public abstract class SQLCatalogAdapter extends DatabaseManager
        implements IDwSqlSchemaConstant, ICatalogAdapter {

  protected static int nameColumnSize = 500;

  protected static int projectColumnSize = 200;

  protected static int extIDColumnSize = 200;

  protected static int uniqueNameColumnSize = 1000;

  protected CatalogManager catalogManager;

  public SQLCatalogAdapter(SQLManager aSqlManager, CatalogManager aCatalogManager)
          throws SQLException {
    super(aSqlManager);
    createSQLTables();
    catalogManager = aCatalogManager;
  }

  public Set<String> getKeyColumnsInternal() {
    HashSet<String> keyColumns = new HashSet<String>(
            Arrays.asList(new String[] { "ExtID", "Project" }));
    return keyColumns;
  }

  public void readTables(String project) throws SQLException {
    PreparedStatement st;
    ResultSet resultSet;

    String command = "SELECT * FROM " + getTableName() + " WHERE project=?";
    st = sqlManager.createPreparedStatement(command);
    st.setString(1, project);
    resultSet = st.executeQuery();
    while (resultSet.next()) {
      readResultInternal(resultSet);
    }
    resultSet.close();
    st.close();
  }

  @Override
  public String getTableName() {
    return T_CATALOG;
  }

  /*
   * This method tries to fix external IDs in the database if anyhow these have been written in a
   * wrong fashion (e.g. evil characters)
   */
  public void fixExternalIDs() throws SQLException {
    String command = "";
    PreparedStatement st;
    HashMap<String, HashSet<String>> existingExtID = new HashMap<String, HashSet<String>>();
    HashSet<String> existingProjects = new HashSet<String>();
    Collection<CatalogEntry> entries = catalogManager.getEntries();

    int size = entries.size();
    int count = 0;
    for (CatalogEntry anEntry : entries) {
      count++;
      if (count % 100 == 0) {
        System.out.println(count + " / " + size);
      }
      String cleanProject = catalogManager.cleanExtID(anEntry.getProject());
      String cleanExtID = catalogManager.cleanExtID(anEntry.getExtID());
      boolean projectExists = true;
      int j = 1;
      while (projectExists) {
        if (!anEntry.getProject().equals(cleanProject) && existingProjects.contains(cleanExtID)) {
          j++;
          cleanProject = cleanProject + "_" + j;
        } else {
          projectExists = false;
          existingProjects.add(cleanProject);
        }
      }

      boolean idExists = true;
      int i = 1;
      while (idExists) {
        HashSet<String> idsForProject = existingExtID.get(cleanProject);
        if (idsForProject == null) {
          idsForProject = new HashSet<String>();
          existingExtID.put(cleanProject, idsForProject);
        }
        if (idsForProject.contains(cleanExtID)) {
          i++;
          cleanExtID = cleanExtID + "_" + i;
        } else {
          idExists = false;
          idsForProject.add(cleanExtID);
        }
      }

      if (!cleanExtID.equals(anEntry.getExtID()) || !cleanProject.equals(anEntry.getProject())) {
        command = "UPDATE " + getTableName()
                + " SET extId=?, project=? WHERE extId=? AND project=?";
        st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setString(paramOffset++, cleanExtID);
        st.setString(paramOffset++, cleanProject);
        st.setString(paramOffset++, anEntry.getExtID());
        st.setString(paramOffset++, anEntry.getProject());
        st.execute();
        st.close();
      }
    }
    commit();
  }

  public CatalogEntry getEntryByRefID(String refID, String aProject,
          boolean throwExceptionWhenNonExists) throws SQLException {
    PreparedStatement st;
    ResultSet resultSet;
    CatalogEntry result = null;

    String command = "SELECT * FROM " + getTableName() + " WHERE extID=? AND project=?";
    st = sqlManager.createPreparedStatement(command);
    int paramOffset = 1;
    st.setString(paramOffset++, refID);
    st.setString(paramOffset++, aProject);
    resultSet = st.executeQuery();
    if (resultSet.next()) {
      result = readResultInternal(resultSet);
    } else {
      if (throwExceptionWhenNonExists) {
        st.close();
        throw new SQLException(
                "Entry with refID '" + refID + "' and project '" + aProject + "' does not exist.");
      }
    }
    resultSet.close();
    st.close();
    return result;
  }

  public CatalogEntry getEntryByID(int attrID) throws SQLException {
    PreparedStatement st;
    ResultSet resultSet;
    CatalogEntry result = null;

    String command = "SELECT * FROM " + getTableName() + " WHERE attrID=?";
    st = sqlManager.createPreparedStatement(command);
    st.setInt(1, attrID);
    resultSet = st.executeQuery();
    if (resultSet.next()) {
      result = readResultInternal(resultSet);
    }
    resultSet.close();
    st.close();
    return result;
  }

  public void deleteEntry(int attrID) throws SQLException {
    String command = "DELETE FROM " + getTableName() + " WHERE attrID=?";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    st.setInt(1, attrID);
    st.execute();
    st.close();
    commit();
  }

  public void deleteEntryByAttrID(int attrID) throws SQLException {
    String command = "DELETE FROM " + getTableName() + " WHERE AttrID=?";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    st.setInt(1, attrID);
    st.execute();
    st.close();
    commit();
  }

  // Returns the node with the given project which does not have
  // a parent from the same project. This node is the root
  public int getProjectRootCatalogAttrID(String project) throws SQLException {
    PreparedStatement st;
    ResultSet resultSet;
    int result = 0;

    String command = "SELECT attrID FROM " + getTableName() + " a WHERE a.project=? AND "
            + "(a.parentID=0 OR " + "EXISTS (SELECT b.project FROM " + getTableName() + " b WHERE "
            + "b.attrID=a.parentID AND b.project != a.project))";
    st = sqlManager.createPreparedStatement(command);
    st.setString(1, project);
    resultSet = st.executeQuery();
    while (resultSet.next()) {
      result = resultSet.getInt("attrid");
    }
    resultSet.close();
    st.close();
    return result;
  }

  public CatalogEntry getEntryByPath(List<String> path) throws SQLException {
    PreparedStatement st;
    ResultSet resultSet;
    CatalogEntry currentResult = null;
    int parentID = 0;

    String command = "SELECT * FROM " + getTableName() + " WHERE name=? and parentID=?";
    st = sqlManager.createPreparedStatement(command);
    for (String aName : path) {
      st.setString(1, aName);
      st.setInt(2, parentID);
      resultSet = st.executeQuery();
      while (resultSet.next()) {
        currentResult = readResultInternal(resultSet);
      }
      resultSet.close();
      parentID = currentResult.getAttrId();
    }
    st.close();
    return currentResult;
  }

  public List<CatalogEntry> getTextEntries() throws SQLException {
    PreparedStatement st;
    ResultSet resultSet;
    List<CatalogEntry> result = new ArrayList<CatalogEntry>();

    String command = "SELECT attrID FROM " + getTableName() + " WHERE dataType='Text'";
    st = sqlManager.createPreparedStatement(command);
    resultSet = st.executeQuery();
    while (resultSet.next()) {
      int attrID = resultSet.getInt("attrID");
      CatalogEntry anEntry = getEntryByID(attrID);
      result.add(anEntry);
    }
    resultSet.close();
    st.close();
    return result;
  }

  public List<Integer> getAttrIDsForProject(String project) throws SQLException {
    PreparedStatement st;
    ResultSet resultSet;
    List<Integer> result = new ArrayList<Integer>();

    String command = "SELECT attrID FROM " + getTableName() + " WHERE project=?";
    st = sqlManager.createPreparedStatement(command);
    st.setString(1, project);
    resultSet = st.executeQuery();
    while (resultSet.next()) {
      int attrID = resultSet.getInt("attrID");
      result.add(attrID);
    }
    resultSet.close();
    st.close();
    return result;
  }

  public Integer getAttrIDForIEProject(String project) throws SQLException {
    PreparedStatement st;
    ResultSet resultSet;
    List<Integer> result = new ArrayList<Integer>();

    String command = "SELECT attrID FROM " + getTableName()
            + " WHERE project='ArztbriefSegmentierung' AND name=?";
    st = sqlManager.createPreparedStatement(command);
    st.setString(1, project);
    resultSet = st.executeQuery();
    while (resultSet.next()) {
      int attrID = resultSet.getInt("attrID");
      result.add(attrID);
    }
    resultSet.close();
    st.close();
    return result.get(0);
  }

  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
    readResultInternal(resultSet);
  }

  protected CatalogEntry readResultInternal(ResultSet resultSet) throws SQLException {
    int attrID = resultSet.getInt("attrid");
    String dataTypeString = resultSet.getString("dataType");
    CatalogEntryType dataType = CatalogEntryType.valueOf(dataTypeString);
    String name = resultSet.getString("name");
    if (name == null) {
      name = "";
    }
    String refID = resultSet.getString("extID");
    int parentID = resultSet.getInt("parentID");
    double orderValue = resultSet.getInt("orderValue");
    String project = resultSet.getString("project");
    String uniqueName = resultSet.getString("uniqueName");
    String description = resultSet.getString("description");
    Timestamp creationTime = resultSet.getTimestamp("creationTime");
    description = description == null ? "" : description;
    CatalogEntry result = new CatalogEntry(attrID, name, dataType, refID, parentID, orderValue,
            project, uniqueName, description, creationTime);
    catalogManager.addEntry(result);
    return result;
  }

  private int counter = 0;

  public void insertByBulk(int attrID, String name, CatalogEntryType dataType, String extID,
          int parentID, double orderValue, String aProject, String uniqueName, String description)
          throws IOException, SQLException {
    getBulkInserter().addRow(attrID, name, dataType, extID, parentID, orderValue, aProject,
            uniqueName, description);
  }

  protected abstract CatalogEntry insertEntry(int attrID, String name, CatalogEntryType dataType,
          String extID, int parentID, double orderValue, String aProject, Timestamp creationTime,
          String uniqueName, String description) throws SQLException;

  public CatalogEntry insertEntry(String name, CatalogEntryType dataType, String extID,
          int parentID, double orderValue, String aProject, String uniqueName, String descripton)
          throws SQLException {
    return insertEntry(-1, name, dataType, extID, parentID, orderValue, aProject, uniqueName,
            descripton);
  }

  public CatalogEntry insertEntry(int attrID, String name, CatalogEntryType dataType, String extID,
          int parentID, double orderValue, String aProject, String uniqueName, String descripton)
          throws SQLException {
    if ((name == null) || name.isEmpty()) {
      throw new SQLException("'name' has to be provided");
    }
    Timestamp creationTime = new Timestamp(new Date().getTime());
    if (name.length() >= nameColumnSize) {
      name = name.substring(0, nameColumnSize - 1);
    }
    if (aProject.length() >= projectColumnSize) {
      aProject = aProject.substring(0, projectColumnSize - 1);
    }
    if (uniqueName.length() >= uniqueNameColumnSize) {
      uniqueName = uniqueName.substring(0, uniqueNameColumnSize - 1);
    }
    if (extID.length() >= extIDColumnSize) {
      extID = extID.substring(0, extIDColumnSize - 1);
    }
    CatalogEntry result = insertEntry(attrID, name, dataType, extID, parentID, orderValue, aProject,
            creationTime, uniqueName, descripton);
    catalogManager.addEntry(result);
    counter++;
    if (counter % 5000 == 0) {
      System.out.print("Created " + counter + " catalog entries.");
    }
    return result;
  }

  public void updateEntry(CatalogEntry anEntry) throws SQLException {
    String command = "";
    PreparedStatement st;

    command += "UPDATE " + getTableName() + " SET " + "name=?, extId=?, project=?, dataType=?, "
            + "orderValue=?, parentID=?, uniqueName=?, description=? WHERE attrID=?";
    st = sqlManager.createPreparedStatement(command);
    int paramOffset = 1;
    st.setString(paramOffset++, anEntry.getName());
    st.setString(paramOffset++, anEntry.getExtID());
    st.setString(paramOffset++, anEntry.getProject());
    st.setString(paramOffset++, anEntry.getDataType().toString());
    st.setDouble(paramOffset++, anEntry.getOrderValue());
    st.setInt(paramOffset++, anEntry.getParentID());
    st.setString(paramOffset++, anEntry.getUniqueName());
    st.setString(paramOffset++, anEntry.getDescription());
    st.setInt(paramOffset++, anEntry.getAttrId());
    st.execute();
    st.close();
  }

  public void importEntries(File f) throws IOException, SQLException {
    Scanner sc = new Scanner(f);
    getBulkInserter().createWriter();
    while (sc.hasNext()) {
      String line = sc.nextLine();
      String[] cellValues = line.split("\t");
      getBulkInserter().addRow((Object[]) cellValues);
    }
    sc.close();
    commit();

  }

  /**
   * Baut Insert Statement für die Tabelle DWCatalog zusammen und gibt sie als StringBuffer zurück
   * 
   * @return
   * @throws SQLException
   */
  public StringBuffer dumpDWCatalog() throws SQLException {
    StringBuffer result = new StringBuffer();
    // Daten laden
    PreparedStatement stmt = sqlManager.createPreparedStatement("SELECT * FROM DWCatalog");
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
      result.append(
              "INSERT DWCatalog ([AttrID], [Name], [ExtID], [ParentID], [orderValue], [DataType], "
                      + "[Project], [CreationTime], [UniqueName], [Description]) VALUES (");
      for (int i = 0; i < columnCount; i++) {
        value = rs.getObject(i + 1);
        // Einzelne Werte hinzufügen, abhängig vom Datenfeld
        if (value == null) {
          result.append("NULL");
        } else {
          if (i > 0) {
            result.append(", ");
          }
          String s = value.toString();
          outputValue = s.replace(System.getProperty("line.separator"), " ");
          switch (i) {
            case 0:
              // AttrID
              result.append(outputValue);
              break;
            case 1:
              // Name
              outputValue = outputValue.replaceAll("'", " ");
              result.append("'" + outputValue + "'");
              break;
            case 2:
              // ExtID
              outputValue = outputValue.replaceAll("'", " ");
              result.append("'" + outputValue + "'");
              break;
            case 3:
              // ParentID
              result.append(outputValue);
              break;
            case 4:
              // OrderValue
              result.append("CAST(" + outputValue + " AS Decimal(12, 4))");
              break;
            case 5:
              // DataType
              outputValue = outputValue.replaceAll("'", " ");
              result.append("'" + outputValue + "'");
              break;
            case 6:
              // Project
              outputValue = outputValue.replaceAll("'", " ");
              result.append("'" + outputValue + "'");
              break;
            case 7:
              // CreationTime
              result.append("CAST('" + outputValue + "' AS DateTime2)");
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
