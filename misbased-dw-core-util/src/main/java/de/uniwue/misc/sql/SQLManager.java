package de.uniwue.misc.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The SQLManager manages the connection to a SQL database. The coordinates, which database to
 * access with which kind of configuration is determined by the SQLConfig object, which has to be
 * used to create a SQLManager object. The actual creation of instances have to be carried out by
 * concrete factories (e.g. MSSQLFactory or MySQLFactory)
 * <p>
 * A manager instance uses a single connection which is used during all database operations. Every
 * time a manager is requested with a certain config the same manager instance is used. For
 * connections with another config a new manager instance is created. The manager currently supports
 * MSSQL and MySQL
 *
 * @author Georg Fette
 */
public abstract class SQLManager {

  protected static Logger logger = LogManager.getLogger(SQLManager.class);

  protected PoolingDataSource<PoolableConnection> conPool;

  public SQLConfig config;

  public SQLManager(SQLConfig aConfig) throws SQLException {
    config = aConfig;
    createConnection();
  }

  public boolean executeBoolResultQuery(String command) throws SQLException {
    Statement st = createStatement();
    ResultSet set = st.executeQuery(command);
    set.next();
    boolean result = set.getBoolean(1);
    set.close();
    st.close();
    return result;
  }

  public String executeStringResultQuery(String command) throws SQLException {
    Statement st = createStatement();
    ResultSet set = st.executeQuery(command);
    set.next();
    String result = set.getString(1);
    set.close();
    st.close();
    return result;
  }

  public int executeIntResultQuery(String command) throws SQLException {
    Statement st = createStatement();
    ResultSet set = st.executeQuery(command);
    set.next();
    int result = set.getInt(1);
    set.close();
    st.close();
    return result;
  }

  public long executeLongResultQuery(String command) throws SQLException {
    Statement st = createStatement();
    ResultSet set = st.executeQuery(command);
    set.next();
    long result = set.getLong(1);
    set.close();
    st.close();
    return result;
  }

  public boolean hasResults(String command) throws SQLException {
    boolean result = false;
    Statement st = createStatement();
    ResultSet set = st.executeQuery(command);
    result = set.next();
    set.close();
    st.close();
    return result;
  }

  public abstract boolean fulltextIndexEnabled() throws SQLException;

  public abstract boolean dropTable(String tableName) throws SQLException;

  protected abstract Properties getPropertiesForConnection() throws SQLException;

  protected abstract String getHostURI() throws SQLException;

  protected void createConnection() throws SQLException {
    ConnectionFactory connectionFactory;
    if (config.useJDBUrl) {
      connectionFactory = new DriverManagerConnectionFactory(config.jdbcURL);
    } else {
      Properties props = getPropertiesForConnection();
      String hostUri = getHostURI();
      connectionFactory = new DriverManagerConnectionFactory(hostUri, props);
    }
    PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(
            connectionFactory, null);
    ObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(
            poolableConnectionFactory);
    poolableConnectionFactory.setPool(connectionPool);
    conPool = new PoolingDataSource<>(connectionPool);
  }

  public DBType getDBType() {
    return config.dbType;
  }

  public void clearCaches() throws SQLException {
    String command = "CHECKPOINT -- force dirty pages in the buffer to be written to disk\n"
            + "DBCC DROPCLEANBUFFERS -- clear the data cache\n"
            + "DBCC FREEPROCCACHE -- clear the execution plan cache";
    executeSQL(command);
  }

  public PreparedStatement createPreparedStatement(String sql) throws SQLException {
    return createPreparedStatement(sql, false);
  }

  public PreparedStatement createPreparedStatementReturnGeneratedKey(String sql)
          throws SQLException {
    return createPreparedStatement(sql, true);
  }

  public PreparedStatement createPreparedStatement(String sql, boolean returnGeneratedKeys)
          throws SQLException {
    Connection con = getConnection();
    PreparedStatement st;
    if (returnGeneratedKeys) {
      st = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
    } else {
      st = con.prepareStatement(sql);
    }
    PooledPreparedStatement pst = new PooledPreparedStatement(st, con);
    return pst;
  }

  public Statement createStatement() throws SQLException {
    Connection con = getConnection();
    Statement st = con.createStatement();
    PooledStatement pst = new PooledStatement(st, con);
    return pst;
  }

  // public Statement createStatementWithScrollableCursor() throws SQLException {
  // return getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
  // ResultSet.CONCUR_READ_ONLY);
  // }
  //
  // public Statement createStatementWithCursor() throws SQLException {
  // return getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY,
  // ResultSet.CONCUR_READ_ONLY);
  // }

  public Connection getConnection() throws SQLException {
    return conPool.getConnection();
  }

  public void dispose() {
    try {
      conPool.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public abstract boolean tableExists(String tableName) throws SQLException;

  public void setLogFileGrowthToUnlimited() throws SQLException {
    String dbName = config.database;
    String sql = "ALTER DATABASE " + dbName + " MODIFY FILE (NAME = N'" + dbName
            + "_log', MAXSIZE=UNLIMITED)";
    Statement stmt = createStatement();
    stmt.execute(sql);
    stmt.close();
    System.out.println("Log file size of db " + dbName + " is set to unlimited at " + new Date());
  }

  public void setLogFileGrowthLimit(int gb) throws SQLException {
    String dbName = config.database;
    String sql = "ALTER DATABASE " + dbName + " MODIFY FILE (NAME = N'" + dbName + "_log', MAXSIZE="
            + (gb * 1000) + ")";
    Statement stmt = createStatement();
    stmt.execute(sql);
    stmt.close();
    System.out
            .println("Log file size of db " + dbName + " is set to " + gb + " gb at " + new Date());
  }

  public void shrinkLogFile() throws SQLException {
    shrinkLogFile(config.database);
  }

  public void shrinkLogFile(String database) throws SQLException {
    String[] queries = { "DBCC SHRINKDATABASE ( " + database + ", 1)",
            "DBCC SHRINKFILE(" + database + "_Log, 1)" };
    Statement stmt = createStatement();
    for (String query : queries) {
      System.out.println(query);
      stmt.execute(query);
    }
    stmt.close();
    System.out.println("Database " + database + " shrinked at " + new Date());
  }

  public boolean indexExists(String indexname) throws SQLException {
    String sql = "select * from sys.indexes where name = ?";
    PreparedStatement stmt = createPreparedStatement(sql);
    stmt.setString(1, indexname);
    ResultSet rs = stmt.executeQuery();
    boolean tableExits = false;
    if (rs.next())
      tableExits = true;
    rs.close();
    stmt.close();
    return tableExits;
  }

  public void shrinkAllDBsOnTheServer() throws SQLException {
    String sql = "SELECT name FROM master..sysdatabases";
    Statement stmt = createStatement();
    ResultSet rs = stmt.executeQuery(sql);
    while (rs.next()) {
      String dbName = rs.getString("name");
      shrinkLogFile(dbName);
    }
    rs.close();
    stmt.close();
  }

  public void shrinkAllFilesOnServer() throws SQLException {
    String sql = "SELECT 'USE [' + d.name + N']' + CHAR(13) + CHAR(10) + "
            + "'DBCC SHRINKFILE (N''' + mf.name + N''' , 1)' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) "
            +
            "FROM sys.master_files mf JOIN sys.databases d ON mf.database_id = d.database_id WHERE d.database_id > 4;";
    Statement stmt = createStatement();
    ResultSet rs = stmt.executeQuery(sql);
    Statement executeStmt = createStatement();
    List<String> sqls = new ArrayList<>();
    while (rs.next()) {
      String shrinkSQL = rs.getString(1);
      sqls.add(shrinkSQL);
      // executeStmt.execute(shrinkSQL);
    }
    rs.close();
    stmt.close();
    for (String shrinkSQL : sqls) {
      System.out.println(shrinkSQL);
      executeStmt.execute(shrinkSQL);
    }
    executeStmt.close();
  }

  /**
   * @param sql
   * @return
   * @throws SQLException
   * @see Statement#execute(String)
   */
  public boolean executeSQL(String sql) throws SQLException {
    Statement stmt = createStatement();
    boolean result = stmt.execute(sql);
    stmt.close();
    return result;
  }

}
