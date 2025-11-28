package de.uniwue.dw.core.model.manager.adapter;

import java.io.File;
import java.sql.SQLException;

import de.uniwue.dw.core.client.authentication.group.AuthManager;
import de.uniwue.dw.core.client.authentication.group.IGroupAdapter;
import de.uniwue.dw.core.client.authentication.group.IGroupCasePermissionAdapter;
import de.uniwue.dw.core.client.authentication.group.IGroupCatalogPermissionAdapter;
import de.uniwue.dw.core.client.authentication.group.IUserAdapter;
import de.uniwue.dw.core.client.authentication.group.IUserInGroupAdapter;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.model.manager.InfoManager;
import de.uniwue.misc.sql.IParamsAdapter;

public interface IDWClientAdapterFactory {

  CatalogManager createCatalogManager() throws SQLException;

  ICatalogAdapter createCatalogAdapter(CatalogManager catalogManager) throws SQLException;

  ICatalogChoiceAdapter createCatalogChoiceAdapter(CatalogManager catalogManager)
          throws SQLException;

  ICatalogCountAdapter createCatalogCountAdapter(CatalogManager catalogManager) throws SQLException;

  ICatalogNumDataAdapter createCatalogNumDataAdapter(CatalogManager catalogManager)
          throws SQLException;

  InfoManager createInfoManager() throws SQLException;

  IDeleteAdapter createDeleteAdapter(InfoManager infoManager) throws SQLException;

  IInfoAdapter createInfoAdapter(InfoManager anInfoManager, File bulkInsertFolder) throws SQLException;

  AuthManager createAuthManager() throws SQLException;

  IGroupAdapter createGroupAdapter(AuthManager authManager) throws SQLException;

  IGroupCasePermissionAdapter createGroupCasePermissionAdapter(AuthManager authManager)
          throws SQLException;

  IGroupCatalogPermissionAdapter createGroupCatalogPermissionAdapter(AuthManager authManager)
          throws SQLException;

  IUserInGroupAdapter createUserInGroupAdapter(AuthManager authManager) throws SQLException;

  IUserAdapter createUserAdapter(AuthManager authManager) throws SQLException;

  IParamsAdapter createParamsAdapter() throws SQLException;

  IUserSettingsAdapter createUserSettingsAdapter() throws SQLException;

  void dropDataTables() throws SQLException;

}
