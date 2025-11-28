package de.uniwue.dw.core.client.authentication.group;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import de.uniwue.dw.core.client.authentication.User;

public interface IUserAdapter {

  Optional<User> selectUserByUsername(String username, String password) throws SQLException;

  String getSaltForUsername(String username) throws SQLException;

  List<String> getUsernames() throws SQLException;

  List<User> selectAllUser() throws SQLException;

  Optional<User> selectUserByUsername(String username) throws SQLException;

  void addUser(String username, String password, String first, String last, String email)
          throws SQLException;

  void addUserWithSalt(String username, String password, String salt, String first, String last,
          String email) throws SQLException;

  void deleteUser(String username) throws SQLException;

  void commit() throws SQLException;

  boolean dropTable() throws SQLException;

  boolean truncateTable() throws SQLException;

}