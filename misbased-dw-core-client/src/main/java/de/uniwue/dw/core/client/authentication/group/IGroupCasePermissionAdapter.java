package de.uniwue.dw.core.client.authentication.group;

import java.sql.SQLException;
import java.util.Set;

public interface IGroupCasePermissionAdapter {

  Set<String> selectPermission(int groupId, String listType) throws SQLException;

  boolean insert(long groupID, long caseID, String listType) throws SQLException;

  void removeGroupCasePermission(int id, long caseID, String listType) throws SQLException;

  void removeAllGroupCasePermissions(int id) throws SQLException;

  void removeGroupCasePermission(int id, long caseID) throws SQLException;

  void commit() throws SQLException;

  boolean dropTable() throws SQLException;

  boolean truncateTable() throws SQLException;

}