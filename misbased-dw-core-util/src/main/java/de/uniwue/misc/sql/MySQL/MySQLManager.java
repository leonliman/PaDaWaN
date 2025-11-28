package de.uniwue.misc.sql.MySQL;

import de.uniwue.misc.sql.SQLConfig;
import de.uniwue.misc.sql.SQLManager;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class MySQLManager extends SQLManager {

  public MySQLManager(SQLConfig aConfig) throws SQLException {
    super(aConfig);
  }

  @Override
  public boolean fulltextIndexEnabled() throws SQLException {
    return false;
  }

  @Override
  public boolean dropTable(String tableName) throws SQLException {
    String command = "DROP TABLE IF EXISTS " + tableName;
    return executeSQL(command);
  }

  @Override
  protected Properties getPropertiesForConnection() {
    Properties props = new Properties();
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
    props.put("allowPublicKeyRetrieval", "true");
    props.put("serverTimezone", "Europe/Berlin");
    props.put("allowLoadLocalInfile", "true");
    return props;
  }

  @Override
  public boolean tableExists(String tableName) throws SQLException {
    String command;
    boolean result = false;
    if (tableName != null) {
      Statement st = createStatement();
      command = "SELECT table_name from INFORMATION_SCHEMA.TABLES " + "WHERE TABLE_SCHEMA='"
              + config.database.replaceAll("'", "''") + "' AND table_name='"
              + tableName.replaceAll("'", "''") + "'";
      ResultSet set = st.executeQuery(command);
      result = set.next();
      set.close();
      st.close();
    }
    return result;
  }

  @Override
  protected String getHostURI() throws SQLException {
    String host = "jdbc:mysql://" + config.sqlServer + "/" + config.database
            + "?useSSL=false&allowMultiQueries=true";
    return host;
  }

}
