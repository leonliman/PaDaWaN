package de.uniwue.dw.core.client.authentication.group;

import java.sql.SQLException;
import java.util.List;

public interface IGroupAdapter {

  List<Group> selectAllGroups() throws SQLException;

  void deleteGroupById(int groupId) throws SQLException;

  void updateGroupById(int groupId, String name, int kAnonymity, Boolean case_query, Boolean admin)
          throws SQLException;

  List<Group> selectGroupsByID(List<Integer> groupIDs) throws SQLException;

  Group selectGroupsByName(String groupName) throws SQLException;

  void integrateAllGroupsToCatalog() throws SQLException;

  Group insertGroup(String name, int kAnonymity, boolean caseQuery, boolean admin)
          throws SQLException;

  void commit() throws SQLException;
  
  boolean dropTable() throws SQLException;

	boolean truncateTable() throws SQLException;
  
}