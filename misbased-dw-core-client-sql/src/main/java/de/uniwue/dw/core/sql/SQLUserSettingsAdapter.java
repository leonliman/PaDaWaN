package de.uniwue.dw.core.sql;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.client.authentication.UserSettings;
import de.uniwue.dw.core.model.manager.adapter.IUserSettingsAdapter;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class SQLUserSettingsAdapter extends DatabaseManager implements IUserSettingsAdapter {

  public static final String TABLE_NAME = "DWUserSettings";

  private static final Logger logger = LogManager.getLogger(SQLUserSettingsAdapter.class);

  public SQLUserSettingsAdapter(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
    createSQLTables();
  }

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  protected String getCreateTableString() {
    // @formatter:off
    return "CREATE TABLE " + getTableName() + " ( " + "id INT "
            + SQLTypes.incrementFlagStartingWith1(getSQLManager().config) + ", \n"
            + "username VARCHAR(100) NOT NULL, \n"
            + "param VARCHAR(255) NOT NULL, \n"
            + "value VARCHAR(255) NOT NULL, \n"
            + "CONSTRAINT PK_id PRIMARY KEY CLUSTERED (id), \n"
            + "CONSTRAINT AK_username UNIQUE(username, param) \n" + ")";
    // @formatter:on
  }

  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * de.uniwue.dw.core.client.authentication.IUserSettingsAdapter#getUserSettings(de.uniwue.dw.core.
   * client.authentication.User)
   */
  public UserSettings getUserSettings(User user) throws SQLException {
    return getUserSettings(user.getUsername());
  }

  private UserSettings getUserSettings(String username) throws SQLException {
    logger.debug("loading settings for user " + username);
    Properties props = new Properties();
    String sql = "Select * from " + getTableName() + " where username = ?";
    PreparedStatement stmt = this.sqlManager.createPreparedStatement(sql);
    stmt.setString(1, username);
    ResultSet rs = stmt.executeQuery();
    while (rs.next()) {
      String param = rs.getString("param");
      String value = rs.getString("value");
      props.setProperty(param, value);
    }
    rs.close();
    stmt.close();
    if (props.isEmpty())
      return null;
    else
      return new UserSettings(props);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * de.uniwue.dw.core.client.authentication.IUserSettingsAdapter#saveUserSettings(de.uniwue.dw.core
   * .client.authentication.User, de.uniwue.dw.core.client.authentication.UserSettings)
   */
  public void saveUserSettings(User user, UserSettings settings) throws SQLException {
    logger.debug("saving settings for user " + user.getUsername());
    String username = user.getUsername();

    for (Map.Entry<Object, Object> entry : settings.getInternalProperties().entrySet()) {
      String param = entry.getKey().toString();
      String value = entry.getValue().toString();

      if (sqlManager.getDBType() == DBType.MSSQL) {
        String sql = "MERGE INTO " + getTableName() + " WITH (HOLDLOCK) AS target " + "USING "
                + "(SELECT ? AS username, ? AS param) AS source (username, param) "
                + "ON (target.username = source.username AND target.param = source.param) "
                + "WHEN MATCHED THEN UPDATE SET value = ? "
                + "WHEN NOT MATCHED THEN INSERT (username, param, value) "
                + "VALUES (?, ?, ?);";
        // @formatter:on
        PreparedStatement stmt = this.sqlManager.createPreparedStatement(sql);
        stmt.setString(1, username);
        stmt.setString(2, param);
        stmt.setString(3, value);
        stmt.setString(4, username);
        stmt.setString(5, param);
        stmt.setString(6, value);
        stmt.execute();
        stmt.close();
      } else {
        // @formatter:off
        String sql = "INSERT INTO " + getTableName()
                + " (username, param, value) VALUES "
                + "(?, ?, ?) ON DUPLICATE KEY UPDATE value=?";
        // @formatter:on
        PreparedStatement stmt = this.sqlManager.createPreparedStatement(sql);
        stmt.setString(1, username);
        stmt.setString(2, param);
        stmt.setString(3, value);
        stmt.setString(4, value);
        stmt.execute();
        stmt.close();
      }
    }
  }

}
