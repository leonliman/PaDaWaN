package de.uniwue.dw.imports;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.api.DWImportsConfig;
import de.uniwue.dw.imports.configured.ImporterConfigured;
import de.uniwue.dw.imports.manager.ImportLogManager;
import de.uniwue.dw.imports.manager.ImporterManager;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;

/**
 * Validate a dw-imports configuration and report errors if necessary.
 */
public class DwImportsConfigValidator {

  private ImporterManager importerManager;

  public DwImportsConfigValidator(ImporterManager anImporterManager) {
    importerManager = anImporterManager;
  }

  /*
   * Check if the bulk insert directory exists and is accessible. Try a dummy bulk insert
   */
  private void doBulkInserterChecks() throws ImportException {
    File bulkInsertDir = SQLPropertiesConfiguration.getSQLBulkImportDir();
    if (bulkInsertDir != null) {
      // do we have access to the bulk import file ?
      if (!bulkInsertDir.exists()) {
        bulkInsertDir.mkdir();
      }
      File testFile = new File(bulkInsertDir, "test.csv");
      try {
        testFile.createNewFile();
      } catch (IOException e) {
        throw new ImportException(ImportExceptionType.FILE_NOT_FOUND,
                "can't access bulk insert folder '" + bulkInsertDir.getAbsolutePath()
                        + "' or create file therein.",
                e);
      }
      testFile.delete();
      try {
        importerManager.infoManager.infoAdapter.doBulkImportTest();
      } catch (SQLException e) {
        throw new ImportException(ImportExceptionType.SQL_ERROR, "bulk insert failed.", e);
      } catch (IOException e) {
        throw new ImportException(ImportExceptionType.SQL_ERROR, "bulk insert failed.", e);
      }
    }
  }

  private void doImporterChecks(List<Importer> allImporters) {
    for (Importer anImporter : allImporters) {
      try {
        // when an importer has problems, do not compromise the whole import. Just note it already
        // at the beginning of the import.
        anImporter.checkForImport();
      } catch (ImportException e) {
        ImportLogManager.error(e);
      }
    }
  }

  private void doDirectoryChecks() throws ImportException {
    if (DWImportsConfig.getImportInfos()) {
      File importSAPExportDir = DWImportsConfig.getSAPImportDir();
      if ((importSAPExportDir != null) && !importSAPExportDir.exists()) {
        throw new ImportException(ImportExceptionType.IMPORT_DIR_NONE_EXISTANT,
                "Import directory '" + importSAPExportDir.toString() + "' does not exist");
      }
      if ((DWImportsConfig.getImportConfigsDir() != null)
              && !DWImportsConfig.getImportConfigsDir().exists()) {
        throw new ImportException(ImportExceptionType.FILE_NOT_FOUND,
                "ConfiguredImporter root directory '" + DWImportsConfig.getImportConfigsDir()
                        + "' does not exist");
      }
    }
  }

  private void doConfiguredImporterCheck() {
    ImporterConfigured configuredImporter = new ImporterConfigured(importerManager);
    try {
      // when the configured importer has problems, do not compromise the whole import. Just note it
      // already at the beginning of the import.
      configuredImporter.doChecks();
    } catch (ImportException e) {
      ImportLogManager.error(e);
    }
  }

  public void doCheck(List<Importer> allImporters) throws ImportException {
    ImportLogManager.info("started do checks before import");
    doBulkInserterChecks();
    doDirectoryChecks();
    doImporterChecks(allImporters);
    doConfiguredImporterCheck();
    ImportLogManager.info("finished do checks before import");
  }
}
