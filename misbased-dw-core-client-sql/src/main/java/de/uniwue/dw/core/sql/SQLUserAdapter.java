package de.uniwue.dw.core.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.client.authentication.group.IUserAdapter;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;

public class SQLUserAdapter extends DatabaseManager implements IUserAdapter {

  private static final Logger logger = LogManager.getLogger(SQLUserAdapter.class);

  public SQLUserAdapter(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
    createSQLTables();
  }

  @Override
  public String getTableName() {
    return IDwSqlSchemaConstant.T_USERS;
  }

  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
  }

  @Override
  protected String getCreateTableString() {
    //@formatter:off
    String sql = "CREATE TABLE " + getTableName() + " ( " 
            + "id int not null " + SQLTypes.incrementFlagStartingWith1(sqlManager.config) + " PRIMARY KEY, \n"
            + "username varchar(100) not null, \n" 
            + "password varchar(200) not null, \n";
    //@formatter:on
    if (DwClientConfiguration.getInstance().useProketPasswords()) {
      sql += "salt varchar(200) not null, \n";
    }
    //@formatter:off
    sql += "first varchar(100) not null, \n" 
            + "last varchar(100) not null, \n"
            + "email varchar(100) not null, \n" 
            + "superuser bit not null, \n" 
            + "admin bit not null,  \n"
            + SQLTypes.createUniqueConstraint(sqlManager.config, "username", "username")
            + ")";
    //@formatter:on
    return sql;
  }

  public boolean isOnline() {
    try {
      readTables();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public Optional<User> authenticate(String username, String password) throws SQLException {
    return selectUserByUsername(username, password);
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.core.client.authentication.group.IUserAdapter#selectUserByUsername(java.lang.
   * String, java.lang.String)
   */
  public Optional<User> selectUserByUsername(String username, String password) throws SQLException {
    String sql = "select * from " + getTableName() + " where username=? ";
    if (password != null)
      sql += " AND password=?";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setString(1, username);
    if (password != null)
      stmt.setString(2, password);
    ResultSet rs = stmt.executeQuery();
    User user = null;
    if (rs.next()) {
      String firstname = rs.getString("first");
      String lastname = rs.getString("last");
      String email = rs.getString("email");
      user = new User(username, firstname, lastname, email);
    }
    rs.close();
    stmt.close();
    return Optional.ofNullable(user);
  }

  public String getSaltForUsername(String username) throws SQLException {
    if (DwClientConfiguration.getInstance().useProketPasswords()) {
      String sql = "select salt from " + getTableName() + " where username=? ";
      PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
      stmt.setString(1, username);
      ResultSet rs = stmt.executeQuery();
      String salt = null;
      if (rs.next()) {
        salt = rs.getString("salt");
      }
      rs.close();
      stmt.close();
      return salt;
    } else {
      return null;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.core.client.authentication.group.IUserAdapter#getUsernames()
   */
  public List<String> getUsernames() throws SQLException {
    String sql = "select username from " + getTableName();
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    ResultSet rs = stmt.executeQuery();
    List<String> result = new ArrayList<String>();
    while (rs.next()) {
      String username = rs.getString("username");
      result.add(username);
    }
    rs.close();
    stmt.close();
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.core.client.authentication.group.IUserAdapter#selectAllUser()
   */
  public List<User> selectAllUser() throws SQLException {
    String sql = "select * from " + getTableName();
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    ResultSet rs = stmt.executeQuery();
    List<User> users = new LinkedList<User>();
    while (rs.next()) {
      String username = rs.getString("username");
      String firstname = rs.getString("first");
      String lastname = rs.getString("last");
      String email = rs.getString("email");
      users.add(new User(username, firstname, lastname, email));
    }
    rs.close();
    stmt.close();
    return users;
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.core.client.authentication.group.IUserAdapter#selectUserByUsername(java.lang.
   * String)
   */
  public Optional<User> selectUserByUsername(String username) throws SQLException {
    return selectUserByUsername(username, null);
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.core.client.authentication.group.IUserAdapter#addUser(java.lang.String,
   * java.lang.String, java.lang.String, java.lang.String, java.lang.String)
   */
  public void addUser(String username, String password, String first, String last, String email) throws SQLException {
    PreparedStatement stmt = sqlManager.createPreparedStatement("INSERT INTO " + getTableName()
            + " (userName, password, first, last, email, superuser, admin) " + "VALUES (?,?,?,?,?,?,?)");
    int paramOffset = 1;
    stmt.setString(paramOffset++, username);
    stmt.setString(paramOffset++, password);
    stmt.setString(paramOffset++, first);
    stmt.setString(paramOffset++, last);
    stmt.setString(paramOffset++, email);
    stmt.setBoolean(paramOffset++, false);
    stmt.setBoolean(paramOffset++, false);
    stmt.execute();
    stmt.close();
    commit();
    logger.info("Inserted user " + username);
  }

  public void addUserWithSalt(String username, String password, String salt, String first, String last, String email)
          throws SQLException {
    if (DwClientConfiguration.getInstance().useProketPasswords()) {
      PreparedStatement stmt = sqlManager.createPreparedStatement("INSERT INTO " + getTableName()
              + " (userName, password, salt, first, last, email, superuser, admin) " + "VALUES (?,?,?,?,?,?,?,?)");
      int paramOffset = 1;
      stmt.setString(paramOffset++, username);
      stmt.setString(paramOffset++, password);
      stmt.setString(paramOffset++, salt);
      stmt.setString(paramOffset++, first);
      stmt.setString(paramOffset++, last);
      stmt.setString(paramOffset++, email);
      stmt.setBoolean(paramOffset++, false);
      stmt.setBoolean(paramOffset++, false);
      stmt.execute();
      stmt.close();
      commit();
      logger.info("Inserted user " + username);
    } else {
      addUser(username, password, first, last, email);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.core.client.authentication.group.IUserAdapter#deleteUser(java.lang.String)
   */
  public void deleteUser(String username) throws SQLException {
    String sql = "delete from " + getTableName() + " where username=?";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setString(1, username);
    stmt.executeUpdate();
    stmt.close();
    logger.info("Deleted user " + username);
  }

}
