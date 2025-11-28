package de.uniwue.dw.imports;

import java.sql.SQLException;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.hooks.IDwCatalogHooks;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.configured.data.ConfigCatalogEntry;

/*
 * CatalogImporters are members of the importer root class and manage the import of the catalog of a single domain.
 */
public class CatalogImporter implements IDwCatalogHooks {

  protected Importer importer;

  private CatalogEntry domainRoot;

  private String noCodeName;

  private String unknownCodeName;

  public String encoding = "Windows-1252";

  // bei true wird der Eintrag aus ROOT_PROJECT_NAME bei false der Projektname genommen
  public boolean useAbstractNameForRootProject = true;

  public CatalogImporter(Importer anImporter, CatalogEntry root) {
    importer = anImporter;
    domainRoot = root;
  }

  public CatalogImporter(Importer anImporter) throws ImportException {
    importer = anImporter;
  }

  public void setEmptyCodeName(String CatalogName) {
    noCodeName = CatalogName;
  }

  public String getEmptyCodeName() {
    return noCodeName;
  }

  protected void setUnknowCodeName(String string) {
    unknownCodeName = string;
  }

  public String getUnknownCodeName() {
    return unknownCodeName;
  }

  public void commit() throws ImportException {
    importer.commit();
  }

  protected CatalogManager getCatalogManager() {
    return importer.getCatalogManager();
  }

  protected String getProject() {
    return importer.getProject();
  }

  public Importer getImporter() {
    return importer;
  }

  private void createRoot() throws ImportException {
    // the project of the roots has to be the "rootProjectName" because it has to be possible to
    // reference them from different projects by their extID/projectName (name/rootProjectName).
    try {
      ConfigCatalogEntry parentEntry = importer.getParentEntry();
      if (useAbstractNameForRootProject) {
        importer.getParentEntry().projectName = ROOT_PROJECT_NAME;
      }
      domainRoot = getCatalogManager().getOrCreateEntry(parentEntry.name, parentEntry.dataType,
              parentEntry.extID, getCatalogManager().getRoot(), parentEntry.projectName);
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR,
              "while getting or creating root '" + importer.parentEntry.name + "'");
    }
  }

  public void doImport() throws ImportException {
    createRoot();
    if (getEmptyCodeName() != null) {
      getOrCreateEntry(getEmptyCodeName(), CatalogEntryType.Bool, getEmptyCodeName());
    }
    if (getUnknownCodeName() != null) {
      getOrCreateEntry(getUnknownCodeName(), CatalogEntryType.Bool, getUnknownCodeName());
    }
  }

  public CatalogEntry insertEntry(String name, CatalogEntryType catalogEntryType, String aProject)
          throws ImportException {
    try {
      return getCatalogManager().getOrCreateEntry(name, catalogEntryType, aProject);
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR,
              "while inserting entry '" + name + "'");
    }
  }

  protected CatalogEntry getOrCreateEntry(String name, CatalogEntryType type, String extID)
          throws ImportException {
    return importer.getOrCreateEntry(name, type, extID);
  }

  protected CatalogEntry getOrCreateEntry(String name, CatalogEntryType type, String extID,
          double orderValue) throws ImportException {
    return importer.getOrCreateEntry(name, type, extID, orderValue);
  }

  protected CatalogEntry getOrCreateEntry(String name, CatalogEntryType type, String extID,
          CatalogEntry parentEntry) throws ImportException {
    return importer.getOrCreateEntry(name, type, extID, parentEntry);
  }

  protected CatalogEntry getOrCreateEntry(String name, CatalogEntryType type, String extID,
          CatalogEntry parentEntry, double orderValue) throws ImportException {
    return importer.getOrCreateEntry(name, type, extID, parentEntry, orderValue);
  }

  public CatalogEntry getDomainRoot() throws ImportException {
    if (domainRoot == null) {
      createRoot();
    }
    return domainRoot;
  }

}
