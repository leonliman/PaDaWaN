package de.uniwue.dw.core.model.util;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.model.manager.InfoManager;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;
import de.uniwue.misc.util.ConfigException;

/*
 * Util class that can export and import dumps of the catalogue and the fact table
 */
public class ImportExportDump {

  public static void main(String args[]) throws SQLException, IOException, ConfigException {
    DwClientConfiguration.loadProperties(new File(
            "d:/Code/DW/testconfigs/mergedConfigs/MySQL_Test_ImportDump.properties"));
    ImportExportDump ieDump = new ImportExportDump();
    String catalogDumpFileString = "C:/tmp/catalogDump.csv";
    String infoDumpFileString = "C:/tmp/infoDump.csv";
    ieDump.exportDump(catalogDumpFileString, infoDumpFileString);
    // ieDump.importDump(catalogDumpFileString, infoDumpFileString);
    // ieDump.importSQLDump();
  }

  public void importDump(String catalogDumpFileString, String infoDumpFileString)
          throws IOException, SQLException {
    CatalogManager catalogManager = DwClientConfiguration.getInstance().getCatalogManager();
    if (catalogManager.getEntries().size() <= 1) { // only root
      catalogManager.importBranch(new File(catalogDumpFileString));
    }
    InfoManager infoManager = new InfoManager(
            DwClientConfiguration.getInstance().getClientAdapterFactory(),
            SQLPropertiesConfiguration.getSQLBulkImportDir());
    if (infoManager.getCounts().size() == 0) {
      infoManager.importBulk(new File(infoDumpFileString));
    }
  }

  public void exportDump(String catalogDumpFileString, String infoDumpFileString)
          throws SQLException, IOException {
    CatalogManager catalogManager = DwClientConfiguration.getInstance().getCatalogManager();
    catalogManager.exportEntriesAsBulkExport(new File(catalogDumpFileString));
    InfoManager infoManager = new InfoManager(
            DwClientConfiguration.getInstance().getClientAdapterFactory(),
            SQLPropertiesConfiguration.getSQLBulkImportDir());
    infoManager.exportBulk(new File(infoDumpFileString));
  }

}
