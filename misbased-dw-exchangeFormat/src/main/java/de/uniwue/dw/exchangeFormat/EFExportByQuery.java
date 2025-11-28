package de.uniwue.dw.exchangeFormat;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.query.model.QueryReader;
import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.client.IGUIClient;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.result.Result;
import de.uniwue.dw.query.model.result.Row;
import de.uniwue.dw.query.solr.SolrGUIClient;
import de.uniwue.misc.util.ConfigException;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class EFExportByQuery {

  public static void main(String[] args) {
    if (args.length < 3) {
      System.err.println("You need to provide a path to a configuration file, " +
              "a path to a file containing a MXQL-Query " +
              "and a path at which the resulting zip-File should be stored");
      return;
    }
    try {
      DwClientConfiguration.loadProperties(new File(args[0]));
      System.out.println("Configuration loaded successfully");
      doExport(loadQuery(new File(args[1])), new File(args[2]));
    } catch (IOException | SQLException | GUIClientException | ConfigException e) {
      e.printStackTrace();
    }
  }

  public static QueryRoot loadQuery(String xmlQuery) throws QueryException, SQLException {
    return QueryReader.read(xmlQuery);
  }

  public static QueryRoot loadQuery(File xmlQueryFile) throws IOException, QueryException, SQLException {
    List<String> queryLines = FileUtils.readLines(xmlQueryFile, Charset.forName("UTF-8"));
    return loadQuery(String.join("\n", queryLines));
  }

  public static void doExport(QueryRoot query, File outputFile)
          throws GUIClientException, SQLException, IOException, ZipException {
    query.setOnlyCount(false);
    query.setLimitResult(0);
    query.setDisplayPID(true);

    for (QueryAttribute anAttribute : query.getAttributesRecursive()) {
      anAttribute.setDisplayValue(false);
    }
    System.out.println("Finished loading the query");

    System.out.println("\nStarting to query for matching patients");
    IGUIClient guiClient = new SolrGUIClient();
    User exportUser = new User("exportUser", "Export", "User", "export.user@mail.com");
    int queryID = guiClient.getQueryRunner().createQuery(query, exportUser);
    Result queryResult = guiClient.getQueryRunner().runQueryBlocking(queryID);
    Set<Long> foundPIDs = new HashSet<>();
    for (Row queryResultRow : queryResult.getRows()) {
      foundPIDs.add(queryResultRow.getPid());
    }
    guiClient.dispose();
    System.out.println(foundPIDs.size() + " matching patients have been found");

    Set<Integer> attrIDsToExport = DwClientConfiguration.getInstance().getInfoManager().getAttrIDsForPIDs(foundPIDs);
    System.out.println("\nThese patients have values for " + attrIDsToExport.size() + " catalog entries");

    System.out.println("\nStarting to export these catalog entries");
    String catalogExportFileName = EFExport.catalogFileName;
    String outputFilePath = outputFile.getParent();
    File catalogExportFile;
    if (outputFilePath == null) {
      catalogExportFile = new File(catalogExportFileName);
    } else {
      catalogExportFile = new File(outputFilePath, catalogExportFileName);
    }
    new EFCatalogCSVProcessor(catalogExportFile).exportPaDaWaNCatalog(false, attrIDsToExport);
    System.out.println("All " + attrIDsToExport.size() + " catalog entries have been exported successfully");

    System.out.println("\nStarting to export the patient data");
    String factsExportFileName = EFExport.factFileName;
    File factsExportFile;
    if (outputFilePath == null) {
      factsExportFile = new File(factsExportFileName);
    } else {
      factsExportFile = new File(outputFilePath, factsExportFileName);
    }
    new EFFactCSVProcessor(factsExportFile).exportPaDaWaNFacts(false, foundPIDs, null, attrIDsToExport);
    System.out.println("The data of all " + foundPIDs.size() + " patients has been exported successfully");

    System.out.println("\nStarting to zip the exported files");
    ZipFile zipFile = new ZipFile(outputFile);
    EFExport.zipExport(zipFile, catalogExportFile, factsExportFile);
    if (!catalogExportFile.delete()) {
      System.err.println(catalogExportFile.getAbsolutePath() + " could not be deleted automatically");
    }
    if (!factsExportFile.delete()) {
      System.err.println(factsExportFile.getAbsolutePath() + " could not be deleted automatically");
    }
    System.out.println("The export to " + outputFile.getAbsolutePath() + " has been successfully completed");
  }
}
