package de.uniwue.misc.sql.MAXDB;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import de.uniwue.misc.sql.SQLConfig;
import de.uniwue.misc.sql.SQLManager;

public class MAXDBManager extends SQLManager {

  public MAXDBManager(SQLConfig aConfig) throws SQLException {
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
    String command = "IF EXISTS (SELECT * FROM information_schema.tables WHERE table_name='" + tableName
            + "') DROP TABLE " + tableName;
    return executeSQL(command);
  }

  @Override
  protected String getHostURI() throws SQLException {
    String host = "jdbc:sapdb://" + config.sqlServer;
    return host;
  }

  @Override
  protected Properties getPropertiesForConnection() {
    Properties props = new Properties();
    if (config.user != null) {
      props.put("user", config.user);
    }
    if (config.useTrustedConnection) {
      props.put("integratedSecurity", "true");
    } else {
      // for truted connection, just omit the use and passwort credentials
      if (config.password != null) {
        props.put("password", config.password);
      }
    }
    props.put("databaseName", config.database);
    props.put("MultipleActiveResultSets", "true");
    return props;
  }

  @Override
  public boolean tableExists(String tableName) throws SQLException {
    String command;
    boolean result = false;
    if (tableName != null) {
      Statement st = createStatement();
      command = "SELECT * FROM sysobjects WHERE name='" + tableName.replaceAll("'", "''") + "'";
      ResultSet set = st.executeQuery(command);
      result = set.next();
      set.close();
      st.close();
    }
    return result;
  }

}
