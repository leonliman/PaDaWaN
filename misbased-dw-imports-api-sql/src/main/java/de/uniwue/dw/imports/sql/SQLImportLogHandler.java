package de.uniwue.dw.imports.sql;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;

import de.uniwue.dw.imports.IDataElem;
import de.uniwue.dw.imports.data.ImportedFileData;
import de.uniwue.dw.imports.manager.adapter.IImportLogHandlerAdapter;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;

/**
 * A handler which stores logged data in a sql database table.
 * 
 * @author Georg Fette
 */
public abstract class SQLImportLogHandler extends DatabaseManager
        implements IImportLogHandlerAdapter {

  public class ImportedFile {
    public ImportedFile() {
    }

    public ImportedFileData data = new ImportedFileData();
  }

  private String tableName;

  // the purpose is a constant String which is filled into the
  // purpose column of the table to be logged into. By defining a purpose
  // different program runs can be stored in the same table and can be
  // distinguished afterwards
  private String purpose;

  public SQLImportLogHandler(SQLManager aSqlManager, String aTableName) throws SQLException {
    this(aSqlManager, aTableName, false);
  }

  public SQLImportLogHandler(SQLManager aSqlManager, String aTableName, boolean dropLogTable)
          throws SQLException {
    this(aSqlManager, aTableName, "", false);
  }

  protected boolean useBulkInsertMerge() {
    return false;
  }
    
  public SQLImportLogHandler(SQLManager aSqlManager, String aTableName, String aPurpose,
          boolean dropLogTable) throws SQLException {
    super(aSqlManager);
    tableName = aTableName;
    purpose = aPurpose;
    if (dropLogTable) {
      dropTable();
    }
    createSQLTables();
    setUseBulkInserts(SQLPropertiesConfiguration.getSQLBulkImportDir());
  }

  @Override
  public void dispose() {
    try {
      commit();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void saveEntryByBulk(String message, String level, String errorType, String project,
          IDataElem file, long line) {
    Timestamp time = new Timestamp(new Date().getTime());
    try {
      getBulkInserter().addRow(0, time, message, purpose, errorType, project, level, file.getName(),
              new Timestamp(file.getTimestamp()), line);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.sql.ILogHandlerAdapter#saveEntry(java.lang.String, java.lang.String,
   * java.lang.String, java.lang.String, java.io.File, long)
   */
  public void saveEntry(String message, String level, String errorType, String project,
          IDataElem file, long line) {
    PreparedStatement st;
    String command = "";
    String filename = "";
    Timestamp filetime = null;
    if (file != null) {
      filename = file.getName();
      filetime = new Timestamp(file.getTimestamp());
    }
    Date today = new Date();
    Timestamp time = new Timestamp(today.getTime());
    System.out.println(errorType + "\t" + message + " " + time);
    try {
      command += "INSERT INTO " + getTableName() + " "
              + "(message, time, level, purpose, errorType, project, filename, filetime, fileline) "
              + "VALUES (?, ?, ?, ?, ?,?,?, ?, ?)";
      st = sqlManager.createPreparedStatement(command);
      int paramOffset = 1;
      st.setString(paramOffset++, message);
      st.setTimestamp(paramOffset++, time);
      st.setString(paramOffset++, level);
      st.setString(paramOffset++, purpose);
      st.setString(paramOffset++, errorType);
      st.setString(paramOffset++, project);
      st.setString(paramOffset++, filename);
      st.setTimestamp(paramOffset++, filetime);
      st.setLong(paramOffset++, line);
      st.execute();
      st.close();
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  protected void readResult(ResultSet resultSet) {
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.sql.ILogHandlerAdapter#getImportedFiles(java.lang.String)
   */
  public ArrayList<ImportedFileData> getImportedFiles(String aProject) throws SQLException {
    ArrayList<ImportedFileData> result = new ArrayList<ImportedFileData>();
    String command = "SELECT filetime, filename FROM " + getTableName()
            + " WHERE errorType = 'FILE_IMPORT_SUCCESS' AND project=?";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    int paramOffset = 1;
    st.setString(paramOffset++, aProject);
    ResultSet resultSet = st.executeQuery();
    String filename, project;
    Timestamp filetime;
    while (resultSet.next()) {
      filename = resultSet.getString("filename");
      filetime = resultSet.getTimestamp("filetime");
      if (filename != null && filetime != null) {
        File file = new File(filename);
        long lastModifyTime = filetime.getTime();
        ImportedFileData rif = new ImportedFileData(file, lastModifyTime, aProject);
        result.add(rif);
      }
    }
    resultSet.close();
    st.close();

    return result;
  }

}
