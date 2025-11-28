package de.uniwue.misc.sql;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.misc.util.StringUtilsUniWue;

/**
 * A DatabaseManager is a helper class which stores and loads task specific data. The manager uses
 * only one table whose name has to be returned by getTableName(). The user has to implement the
 * methods - getTableName: to determine the name of the table the manager is working on -
 * readResult(ResultSet resultSet): this method is called when readTables is called on the manager.
 * the read command reads all columns (*) from the table if not given otherwise by overriding
 * getReadTablesCommand() - getCreatTableString(): returns the SQLCode to create the table the
 * manager administrates
 * <p>
 * The manager's database connection works via a SQLManager object which has to be given in the
 * constructor.
 *
 * @author Georg Fette
 */
public abstract class DatabaseManager {

  public static final String LOG_NAME = "DW-DatabaseManager";

  protected static final Logger log = LogManager.getLogger(DatabaseManager.class);

  protected SQLManager sqlManager;

  // To ensure downwards compatibility for some tables the columnNames can be checked to see if the
  // desired columns do really exist
  private LinkedHashSet<String> columnNames;

  private BulkInserter bulkInserter;

  private SQLInsertMode insertMode = SQLInsertMode.insertSingleStatement;

  private boolean isIdentityInsert = false;

  public abstract String getTableName();

  protected abstract void readResult(ResultSet resultSet) throws SQLException;

  protected abstract String getCreateTableString();

  public DatabaseManager(SQLManager aSqlManager) throws SQLException {
    if (aSqlManager == null) {
      throw new SQLException("sqlManager is null");
    }
    sqlManager = aSqlManager;
  }

  // The set of columns that determine the unique key of the table. When inserting data and the
  // insertMode is set to insertOrUpdateXXX those columns are used as a check if the row already
  // exists in the table. This method can be overriden in subclasses
  public Set<String> getKeyColumnsInternal() {
    return null;
  }

  public Set<String> getKeyColumns() {
    Set<String> keyColumns = getKeyColumnsInternal();
    if (keyColumns == null) {
    } else {
      StringUtilsUniWue.lowerCaseCollection(keyColumns);
    }
    return keyColumns;
  }

  /**
   * These columns are not updates by the bulk inserter if the import mode is set to
   * insertOrUpdateXXX
   */
  public Set<String> getNoUpdateColumnsInternal() {
    return new HashSet<String>();
  }

  public Set<String> getNoUpdateColumns() {
    Set<String> noUpdateColumns = getNoUpdateColumnsInternal();
    if (noUpdateColumns == null) {
    } else {
      StringUtilsUniWue.lowerCaseCollection(noUpdateColumns);
    }
    return noUpdateColumns;
  }

  public boolean isAutoincrementColumn(String aColumn) throws SQLException {
    boolean isAuto = false;
    if (getSQLManager().getDBType() == DBType.MSSQL) {
      String command = "SELECT is_identity FROM sys.columns WHERE object_id = object_id('"
              + getTableName() + "') AND name = '" + aColumn + "'";
      isAuto = getSQLManager().executeBoolResultQuery(command);
    } else if (getSQLManager().getDBType() == DBType.MySQL) {
      String command = "SHOW COLUMNS FROM dwinfo where field = '" + aColumn
              + "' and extra like '%auto_increment%'";
      Statement st = getSQLManager().createStatement();
      ResultSet result = st.executeQuery(command);
      if (result.next()) {
        isAuto = true;
      }
      result.close();
      st.close();
    }
    return isAuto;
  }

  public String getColumnType(String aColumn) throws SQLException {
    String command = "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"
            + getTableName() + "' AND COLUMN_NAME = '" + aColumn + "'";
    String type = getSQLManager().executeStringResultQuery(command);
    if (type.toLowerCase().equals("varchar")) {
      command = "SELECT CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"
              + getTableName() + "' AND COLUMN_NAME = '" + aColumn + "'";
      int textLength = getSQLManager().executeIntResultQuery(command);
      if (textLength == -1) {
        type = type + "(MAX)";
      } else {
        type = type + "(" + textLength + ")";
      }
    } else if (type.toLowerCase().equals("datetime2")) {
      command = "SELECT DATETIME_PRECISION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"
              + getTableName() + "' AND COLUMN_NAME = '" + aColumn + "'";
      int precision = getSQLManager().executeIntResultQuery(command);
      type = type + "(" + precision + ")";
    } else if (type.toLowerCase().equals("decimal")) {
      command = "SELECT NUMERIC_PRECISION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"
              + getTableName() + "' AND COLUMN_NAME = '" + aColumn + "'";
      int precision = getSQLManager().executeIntResultQuery(command);
      command = "SELECT NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"
              + getTableName() + "' AND COLUMN_NAME = '" + aColumn + "'";
      int scale = getSQLManager().executeIntResultQuery(command);
      type = type + "(" + precision + ", " + scale + ")";
    }
    return type;
  }

  public Set<String> getColumnNames() throws SQLException {
    if (columnNames == null) {
      columnNames = new LinkedHashSet<String>();
      String command = "SELECT column_name " + "FROM INFORMATION_SCHEMA.COLUMNS "
              + "WHERE TABLE_NAME = '" + getTableName() + "'";
      if (getSQLManager().getDBType() == DBType.MySQL) {
        // MSSQL already only returns the columns of the correct DB, MySQL also returns the columns
        // of all other databases and has to be restricted to the used one
        command += " AND TABLE_SCHEMA = '" + SQLPropertiesConfiguration.getSQLDBName() + "'";
        // Starting with MySQL 8 the order of the columns does not always match the order in the
        // database by default;
        // this order is enforced by the following command
        command += "ORDER BY ORDINAL_POSITION";
      }
      Statement st = sqlManager.createStatement();
      ResultSet resultSet = st.executeQuery(command);
      while (resultSet.next()) {
        String name = resultSet.getString("column_name").toLowerCase();
        columnNames.add(name);
      }
      resultSet.close();
      st.close();
    }
    return new LinkedHashSet<String>(columnNames);
  }

  public void setUseBulkInserts(File aBulkFolder) throws SQLException {
    setUseBulkInserts(aBulkFolder, false);
  }

  protected boolean useBulkInsertMerge() {
    return SQLPropertiesConfiguration.getSQLBulkInsertMerge();
  }

  // (de-)activates the bulk insert feature
  public void setUseBulkInserts(File aBulkFolder, boolean keepIdentity) throws SQLException {
    isIdentityInsert = keepIdentity;
    if (aBulkFolder != null) {
      try {
        bulkInserter = new BulkInserter(this, aBulkFolder);
        bulkInserter.setKeepIdentity(keepIdentity);
        if (useBulkInsertMerge()) {
          if (getKeyColumns() != null) {
            insertMode = SQLInsertMode.bulkInsertTmpTableMerge;
          } else {
            insertMode = SQLInsertMode.bulkInsert;
            System.out.println("no key columns given for bulkimport with tmpTable merge for table '"
                    + getTableName() + "'");
          }
        } else {
          insertMode = SQLInsertMode.bulkInsert;
        }
      } catch (IOException e) {
        throw new SQLException(e);
      }
    } else {
      bulkInserter = null;
      insertMode = SQLInsertMode.insertSingleStatement;
    }
  }

  // indicates if the manager uses bulk inserts
  public boolean useBulkInserts() {
    return getBulkInserter() != null;
  }

  public void lockTable() throws SQLException {
    String lockStmt = "SELECT * from " + getTableName() + " WITH (TABLOCKX)";
    Statement st = sqlManager.createStatement();
    st.execute(lockStmt);
    st.close();
  }

  protected boolean executeCommand(String command) throws SQLException {
    Statement st = sqlManager.createStatement();
    boolean result = st.execute(command);
    st.close();
    return result;
  }

  public void dispose() {
    if (useBulkInserts()) {
      getBulkInserter().dispose();
    }
  }

  protected String getCreateTableAdditionalColumns() {
    return "";
  }

  protected String getCreateTableStub() {
    String command;
    if (sqlManager.config.dbType == DBType.MSSQL) {
      command = "CREATE TABLE [" + getSQLManager().config.database + "].[dbo].[" + getTableName()
              + "] (\n";
    } else {
      command = "CREATE TABLE " + getTableName() + " (\n";
    }
    return command;
  }

  public SQLManager getSQLManager() {
    return sqlManager;
  }

  protected boolean tableExists() throws SQLException {
    return sqlManager.tableExists(getTableName());
  }

  protected static int getBooleanInt(boolean aValue) {
    if (aValue) {
      return 1;
    } else {
      return 0;
    }
  }

  public void insertByBulk(Object... objects) throws SQLException {
    try {
      getBulkInserter().addRow(objects);
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }

  protected void createSQLTables() throws SQLException {
    if ((getTableName() != null) && !tableExists()) {
      String command = getCreateTableString();
      executeCommand(command);
    }
  }

  protected String getRestrictionString() {
    return "";
  }

  protected Long getGeneratedLongKey(PreparedStatement st) {
    ResultSet result;
    long genID = 0;
    try {
      result = st.getGeneratedKeys();
      if (result.next()) {
        genID = result.getLong(1);
      }
      result.close();
    } catch (SQLException e) {
    }
    return genID;
  }

  protected Integer getGeneratedIntKey(PreparedStatement st) {
    ResultSet result;
    Integer genID = 0;
    try {
      result = st.getGeneratedKeys();
      if (result.next()) {
        genID = result.getInt(1);
      }
      result.close();
    } catch (SQLException e) {
    }
    return genID;
  }

  protected String getReadTablesCommand() {
    String result = "SELECT * FROM " + getTableName();
    String restriction = getRestrictionString();
    if (!restriction.isEmpty()) {
      result += " WHERE " + restriction;
    }
    return result;
  }

  protected void preProcessResultSet(ResultSet resultSet) throws SQLException {
  }

  public void readTables() throws SQLException {
    Logger logger = LogManager.getLogger(log.getName() + ".readTables");
    int counter = 0;
    if (getTableName() == null) {
      throw new SQLException("Table has to have a name");
    }
    Statement st = sqlManager.createStatement();
    logger.debug("Started reading table '" + getTableName() + "'");
    String command = getReadTablesCommand();
    ResultSet resultSet = st.executeQuery(command);
    logger.debug("Finished command for reading table '" + getTableName() + "'");
    preProcessResultSet(resultSet);
    while (resultSet.next()) {
      readResult(resultSet);
      counter++;
      if (counter % 500000 == 0) {
        logger.debug("Loaded " + counter + " entries");
      }
      if (resultSet.isClosed()) {
        break;
      }
    }
    resultSet.close();
    logger.debug("Finished reading table '" + getTableName() + "'");
    st.close();
  }

  public void commit() throws SQLException {
    if (useBulkInserts()) {
      try {
        getBulkInserter().executeInsert();
      } catch (IOException e) {
        throw new SQLException(e);
      }
    }
  }

  public boolean dropTable() throws SQLException {
    if ((getTableName() != null) && tableExists()) {
      return sqlManager.dropTable(getTableName());
    }
    return true;
  }

  public boolean emptyTable() throws SQLException {
    if ((getTableName() != null) && tableExists()) {
      String command = "DELETE FROM " + getTableName();
      boolean result = executeCommand(command);
      return result;
    }
    return true;
  }

  public boolean truncateTable() throws SQLException {
    if ((getTableName() != null) && tableExists()) {
      String command = "TRUNCATE TABLE " + getTableName();
      boolean result = executeCommand(command);
      return result;
    }
    return true;
  }

  public BulkInserter getBulkInserter() {
    return bulkInserter;
  }

  public SQLInsertMode getInsertMode() {
    return insertMode;
  }

  public void setInsertMode(SQLInsertMode insertMode) {
    this.insertMode = insertMode;
  }

  public boolean isIdentityInsert() {
    return isIdentityInsert;
  }

  public boolean isParallelizeInsert() {
    return getBulkInserter().isParallelizeInsert();
  }

  public void setParallelizeInsert(boolean parallelizeInsert) {
    if (getBulkInserter() != null) {
      getBulkInserter().setParallelizeInsert(parallelizeInsert);
    }
  }
}
