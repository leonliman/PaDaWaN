package de.uniwue.dw.query.solr.preprocess;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Scanner;

import org.apache.solr.client.solrj.SolrServerException;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.group.AuthManager;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.query.model.IQueryKeys;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.dw.query.solr.CatalogUtilFunctions;
import de.uniwue.dw.query.solr.client.SolrManager;
import de.uniwue.dw.query.solr.suggest.CatalogIndexer;
import de.uniwue.dw.solr.api.DWSolrConfig;
import de.uniwue.misc.util.ConfigException;

public class InteractiveIndexer {

  public static void main(String[] args)
          throws IOException, SQLException, IndexException, ConfigException, SolrServerException {
    String settingsPath = "config.prop";
    if (args.length == 1)
      settingsPath = args[0];
    for (int i = 0; i <= args.length - 1; i++) {
      if ("-s".equals(args[i])) {
        if (i + 1 <= args.length - 1) {
          settingsPath = args[i + 1];
        }
      }
    }
    createAndStartIndexerWithUserInteraction(settingsPath);
  }

  public static void createAndStartIndexerWithUserInteraction(String settingsPath)
          throws IOException, SQLException, IndexException, ConfigException, SolrServerException {
    loadDBConfig(settingsPath);
    Scanner in = new Scanner(System.in);
    boolean validConfig = false;
    do {
      String serverURL = getServerURL(in);
      String user = getUser(in);
      String password = getPassword(in);
      SolrManager solrManager = new SolrManager(user, password, serverURL);
      // SolrManager solrManager = DWSolrConfig.getSolrManager();
      DWSolrConfig.getInstance().setSolrManager(solrManager);
      validConfig = true;
    } while (!validConfig);

    boolean deleteIndex = getDeleteIndex(in);
    DwClientConfiguration.getInstance().setProperty(IQueryKeys.PARAM_INDEXER_DELETE_INDEX,
            Boolean.toString(deleteIndex));
    boolean indexAllCases = getIndexAllCases(in);
    DwClientConfiguration.getInstance().setProperty(IQueryKeys.PARAM_INDEXER_INDEX_ALL_DATA,
            Boolean.toString(indexAllCases));
    boolean indexCatalog = getIndexCatalog(in);
    DwClientConfiguration.getInstance().setProperty(IQueryKeys.PARAM_INDEXER_INDEX_CATALOG,
            Boolean.toString(indexCatalog));
    PropagateIndexer2 indexer = new PropagateIndexer2();
    indexer.update();
  }

  private static boolean getUniqueNames(Scanner in) {
    System.out.println("Create unique-names? (y/n)?");
    return getYesNoAnswer(in);
  }

  private static void createAndSaveUniqueNames() throws SQLException {
    CatalogUtilFunctions.createAndSaveUniqueCatalogEntryNamesInDB();
  }

  private static void indexCatalog() throws SQLException, SolrServerException, IOException {
    System.out.println("Indexing Catalog");
    CatalogManager catalogManager = DwClientConfiguration.getInstance().getCatalogManager();
    AuthManager groupManager = DwClientConfiguration.getInstance().getAuthManager();
    CatalogIndexer catalogIndexer = new CatalogIndexer(catalogManager, groupManager);
    catalogIndexer.indexAllCatalogEntries();
    System.out.println("Catalog Indexed.");
  }

  private static boolean getIndexCatalog(Scanner in) {
    System.out.println("Update Catalog? (y/n)?");
    return getYesNoAnswer(in);
  }

  private static boolean getCalcNewCatalogNumbers(Scanner in) {
    System.out.println("Calculate new catalog-case-numbers and store them in the db (y/n)?");
    return getYesNoAnswer(in);
  }

  private static void loadDBConfig(String settingsPath) throws IOException, ConfigException {
    File f = new File(settingsPath);
    System.out.println("Loading config from " + f.getAbsolutePath());
    DwClientConfiguration.getInstance().loadProperties(f);
  }

  private static boolean getIndexAllCases(Scanner in) {
    System.out.println("Index all cases (y/n)?");
    return getYesNoAnswer(in);
  }

  private static boolean getYesNoAnswer(Scanner in) {
    while (true) {
      String input = in.nextLine().trim().toLowerCase();
      if (input.equals("y"))
        return true;
      else if (input.equals("n"))
        return false;
      else
        System.out.println("Invalid input. Please enter 'y' or 'n'");
    }
  }

  private static boolean getDeleteIndex(Scanner in) {
    System.out.println("Delete index (y/n)?");
    return getYesNoAnswer(in);
  }

  private static String getPassword(Scanner in) {
    String solrPassword = DWSolrConfig.getSolrPassword();
    if (solrPassword != null && !solrPassword.isEmpty()) {
      System.out.println("Solr user password is: " + solrPassword);
      System.out.println("Do you want to change it (y/n)?");
      if (!getYesNoAnswer(in))
        return solrPassword;
    }
    System.out.println("Please enter the the password:");
    String input = in.nextLine().trim();
    return input;
  }

  private static String getUser(Scanner in) {
    String solrUsername = DWSolrConfig.getSolrUser();
    if (solrUsername != null && !solrUsername.isEmpty()) {
      System.out.println("Solr user name is: " + solrUsername);
      System.out.println("Do you want to change it (y/n)?");
      if (!getYesNoAnswer(in))
        return solrUsername;
    }
    System.out.println("Please enter the the user:");
    String input = in.nextLine().trim();
    return input;
  }

  private static String getServerURL(Scanner in) {
    String solrServerUrl = DWSolrConfig.getSolrServerUrl();
    if (solrServerUrl != null && !solrServerUrl.isEmpty()) {
      System.out.println("Solr-Server-URL is: " + solrServerUrl);
      System.out.println("Do you want to change it (y/n)?");
      if (!getYesNoAnswer(in))
        return solrServerUrl;
    }

    while (true) {
      System.out.println("Solr-Server-URL-selection:");
      String wflw234Port18983 = "http://wflw234:18983/solr";
      String wflw234Port28983 = "http://wflw234:28983/solr";
      String localPort18983 = "localhost:18983/solr";
      String localPort28983 = "localhost:28983/solr";
      System.out.println("(1) " + wflw234Port18983);
      System.out.println("(2) " + wflw234Port28983);
      System.out.println("(3) " + localPort18983);
      System.out.println("(4) " + localPort28983);
      System.out.println("(5) Enter URL");
      String input = in.nextLine().trim();
      if (input.equals("1"))
        return wflw234Port18983;
      else if (input.equals("2"))
        return wflw234Port28983;
      if (input.equals("3"))
        return localPort18983;
      else if (input.equals("4"))
        return localPort28983;
      else if (input.equals("5")) {
        System.out.println("Please enter the server URL");
        input = in.nextLine();
        return input;
      } else {
        System.out.println("Invalid input");
      }
    }
  }
}
