package de.uniwue.dw.core.client.authentication.group;

import java.sql.SQLException;
import java.util.Set;

import de.uniwue.dw.core.model.data.CatalogEntry;

public interface IGroupCatalogPermissionAdapter {

  Set<CatalogEntry> selectPermission(int groupId, String listType) throws SQLException;

  String selectCatalogPermission(int groupId, String extid, String project) throws SQLException;

  void commit() throws SQLException;
  
  boolean dropTable() throws SQLException;

  void insertGroupCatalogPermission(int groupId, String extID, String project, String listType)  throws SQLException;

	void removeGroupCatalogPermission(int id, String extID, String project, String listType) throws SQLException;

	void removeAllGroupCatalogPermissions(int id) throws SQLException;

	void removeGroupCatalogPermission(int id, String extID, String project) throws SQLException;

	boolean truncateTable() throws SQLException;
  
}