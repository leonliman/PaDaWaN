package de.uniwue.dw.exchangeFormat;

import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.model.enums.CompressionLevel;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

public class EFExport {

  public static String catalogFileName = "exportedCatalog.csv";

  public static String factFileName = "exportedFacts.csv";

  public static String factMetaDataFileName = "exportedFactsMetaData.csv";

  private EFExport() {
  }

  public static void doExport(String exportCatalogFilePath, String exportFactsFilePath)
          throws SQLException, IOException {
    doExport(exportCatalogFilePath, exportFactsFilePath, false);
  }

  public static void doExport(String exportCatalogFilePath, String exportFactsFilePath,
          boolean withResultPrintedToConsole) throws SQLException, IOException {
    doExport(new File(exportCatalogFilePath), new File(exportFactsFilePath),
            withResultPrintedToConsole);
  }

  public static void doExport(File exportCatalogFile, File exportFactsFile,
          boolean withResultPrintedToConsole) throws SQLException, IOException {
    if (exportCatalogFile != null) {
      new EFCatalogCSVProcessor(exportCatalogFile).exportPaDaWaNCatalog(withResultPrintedToConsole);
    }
    if (exportFactsFile != null) {
      new EFFactCSVProcessor(exportFactsFile).exportPaDaWaNFacts(withResultPrintedToConsole);
    }
  }

  public static void zipExport(ZipFile zipFile, File catalogExportFile, File factsExportFile) throws ZipException {
    ZipParameters zipParameters = new ZipParameters();
    zipParameters.setCompressionLevel(CompressionLevel.ULTRA);
    zipFile.addFile(catalogExportFile, zipParameters);
    zipFile.addFile(factsExportFile, zipParameters);
  }

}
