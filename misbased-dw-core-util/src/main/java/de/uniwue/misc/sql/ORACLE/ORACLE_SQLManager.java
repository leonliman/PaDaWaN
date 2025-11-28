package de.uniwue.misc.sql.ORACLE;

import de.uniwue.misc.sql.SQLConfig;
import de.uniwue.misc.sql.SQLManager;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

// this class does not work yet. It is for the most part a copy of the MySQLManager.
// As we do not have an ORACLE-Installation this cannot be ajusted more at the moment...
public class ORACLE_SQLManager extends SQLManager {

  public ORACLE_SQLManager(SQLConfig aConfig) throws SQLException {
    super(aConfig);
  }

  @Override
  public boolean fulltextIndexEnabled() throws SQLException {
    boolean result = false;
    String command = "SELECT FULLTEXTSERVICEPROPERTY('IsFullTextInstalled') as installed";
    result = executeBoolResultQuery(command);
    return result;
  }

  @Override
  public boolean dropTable(String tableName) throws SQLException {
    String command = "IF EXISTS (SELECT * FROM information_schema.tables WHERE table_name='"
            + tableName + "') DROP TABLE " + tableName;
    return executeSQL(command);
  }

  @Override
  protected Properties getPropertiesForConnection() {
    Properties props = new Properties();
    props = new Properties();
    if (config.useTrustedConnection) {
      props.put("integratedSecurity", "true");
    }
    if (config.user != null) {
      props.put("user", config.user);
    }
    if (config.password != null) {
      props.put("password", config.password);
    }
    // props.put("allowMultiQueries", "true");
    props.put("MultipleActiveResultSets", "true");
    return props;
  }

  @Override
  public boolean tableExists(String tableName) throws SQLException {
    String command;
    boolean result = false;
    if (tableName != null) {
      command = "SELECT * FROM USER_TABLES WHERE TABLE_NAME = ?";
      PreparedStatement st = createPreparedStatement(command);
      st.setString(1, tableName);
      ResultSet set = st.executeQuery(command);
      result = set.next();
      set.close();
      st.close();
    }
    return result;
  }

  @Override
  protected String getHostURI() throws SQLException {
    String host = "jdbc:oracle:thin:@" + config.sqlServer + ":" + config.database;
    return host;
  }

}
