package de.uniwue.dw.core.client.authentication.group;

import java.sql.SQLException;
import java.util.List;

public interface IUserInGroupAdapter {

  List<Integer> selectGroupsByUser(String username) throws SQLException;

  List<String> selectUsersByGroupId(int groupId) throws SQLException;

  boolean selectUserIsMemberInGroup(int groupId, String username) throws SQLException;

  void updateUsersInGroup(int groupId, List<String> userList) throws SQLException;

  void removeRowByGroup(int groupId) throws SQLException;

  void deleteUser(String username) throws SQLException;

  void deleteUserFromSingleGroup(String username, int groupId) throws SQLException;

  void commit() throws SQLException;

  void insertUser2Group(int groupID, String userName) throws SQLException;

  boolean truncateTable() throws SQLException;

}