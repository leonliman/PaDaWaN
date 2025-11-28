package de.uniwue.dw.imports.impl.base;

import java.io.IOException;
import java.sql.SQLException;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.group.AuthManager;
import de.uniwue.dw.core.model.hooks.IDwCatalogHooks;
import de.uniwue.dw.imports.CatalogImporter;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.ImporterTable;
import de.uniwue.dw.imports.configured.data.ConfigMetaGroupCase;
import de.uniwue.dw.imports.manager.ImporterManager;

public class GroupCaseImporter extends ImporterTable {

  public static final String PARAM_IMPORT_DIR = "import.dir.groups";

  public static String PROJECT_NAME = IDwCatalogHooks.PROJECT_HOOK_GROUP_ID;

  private AuthManager groupManager;

  private ConfigMetaGroupCase config;

  public GroupCaseImporter(ImporterManager mgr, ConfigMetaGroupCase config) throws ImportException {
    super(mgr, null, PROJECT_NAME, config.getDataSource());
    try {
      this.groupManager = DwClientConfiguration.getInstance().getAuthManager();
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, e);
    }
  }

  @Override
  protected CatalogImporter createCatalogImporter() throws ImportException {
    return null; // no catalog entries for pids
  }

  private long getCaseID() throws ImportException {
    return getCaseID(config.caseIDColumn);
  }

  private String getListType() throws ImportException {
    String listType = "B";
    listType = getItem(config.listTypeColumn);
    if (listType.equalsIgnoreCase("black")) {
      listType = "B";
    } else if (listType.equalsIgnoreCase("white")) {
      listType = "W";
    }
    return listType;
  }

  private long getGroupID() throws ImportException {
    String groupIdString = getItem(config.groupIDColumn);
    long groupId = Long.valueOf(groupIdString);
    return groupId;
  }

  @Override
  protected void processImportInfoFileLine() throws ImportException, SQLException, IOException {
    long caseID = getCaseID();
    long groupID = getGroupID();
    String listType = getListType();
    getGroupManager().insertGroupCasePermission(groupID, caseID, listType);
  }

  @Override
  protected void commit() throws ImportException {
    super.commit();
    try {
      getPatientManager().commit();
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, "", getProject(), e);
    }
  }

  public AuthManager getGroupManager() {
    return groupManager;
  }

  public void setGroupManager(AuthManager groupManager) {
    this.groupManager = groupManager;
  }

}
