package de.uniwue.dw.query.model;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.client.authentication.group.AuthManager;
import de.uniwue.dw.core.client.authentication.group.Group;
import de.uniwue.dw.core.model.manager.ICatalogClientManager;
import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.client.IGUIClient;
import de.uniwue.dw.query.model.client.QueryRunnerTest;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.misc.util.ConfigException;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import javax.security.auth.login.AccountException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Properties;

public abstract class TestEnvironmentDataLoader {

  private boolean dataAlreadyLoaded = false;

  public void loadDataIfNecessary() throws IOException, SQLException, NoSuchAlgorithmException,
          URISyntaxException, IndexException, ConfigException, GUIClientException {
    if (!dataAlreadyLoaded) {
      loadConfig();
      clearDB();
      loadConfig();
      configHasBeenChanged();
      importData();
      addDemoUser();
      startStorageEngine();
      storeDataInStorageEngine();
      dataAlreadyLoaded = true;
    }
  }

  protected abstract void configHasBeenChanged() throws GUIClientException, SQLException;

  protected abstract void importData() throws IOException, URISyntaxException;

  protected abstract void storeDataInStorageEngine() throws IndexException;

  protected abstract void startStorageEngine();

  public abstract IGUIClient getGuiClient() throws GUIClientException;

  public abstract ICatalogClientManager getCompleteCatalogClientManager() throws SQLException;

  protected static Resource[] getResources(String path) throws IOException {
    final PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver(
            QueryRunnerTest.class.getClassLoader());
    final Resource[] resources = resolver.getResources(path);
    return resources;
  }

  protected static Resource getResource(String path) throws IOException {
    Resource[] resources = getResources(path);
    if (resources.length == 1)
      return resources[0];
    return null;
  }

  public static User getTestUser1() throws AccountException, SQLException {
    AuthManager authService = DwClientConfiguration.getInstance().getAuthManager();
    return authService.authenticate("demo", "demo", null);
  }

  public static User getTestUser2() throws AccountException, SQLException {
    AuthManager authService = DwClientConfiguration.getInstance().getAuthManager();
    return authService.authenticate("demo1", "demo", null);
  }

  private void addDemoUser() throws SQLException, NoSuchAlgorithmException {
    AuthManager authenticationService = DwClientConfiguration.getInstance().getAuthManager();
    checkUserAndGroupDeletion(authenticationService, "demo");
    authenticationService.addUser("demo", "demo", "demo", "demo", "demo");
    authenticationService.addGroup("demo", 0, true, false);
    authenticationService.addUser2Group("demo", "demo");

    checkUserAndGroupDeletion(authenticationService, "demo1");
    authenticationService.deleteUser("demo1");
    authenticationService.addUser("demo1", "demo", "demo", "demo", "demo");
    authenticationService.addGroup("demo1", 0, true, false);
    authenticationService.addUser2Group("demo1", "demo1");

    authenticationService.insertGroupCasePermission("demo1", 1000, "w");
    authenticationService.addGroupCatalogPermission("demo1", "diagnose", "untersuchung", "b");
  }

  private void checkUserAndGroupDeletion(AuthManager authenticationService, String name)
          throws SQLException {
    if (authenticationService.getUsernames().contains(name)) {
      if (authenticationService.userIsMemberInGroup(name, name)) {
        authenticationService.deleteUserInGroupByUser(name);
      }
      authenticationService.deleteUser(name);
    }
    Group demoGroup = authenticationService.getGroup(name);
    if (demoGroup != null) {
      authenticationService.deleteGroup(demoGroup.getId());
    }
  }

  private void clearDB() throws SQLException, ConfigException {
    try {
      DwClientConfiguration.getInstance().getAuthManager().truncateTables();
      DwClientConfiguration.getInstance().getCatalogManager().truncateCatalogTables();
      DwClientConfiguration.getInstance().getInfoManager().truncateInfoTables();
      DwClientConfiguration.getInstance().getClientAdapterFactory().dropDataTables();
      DWQueryConfig.getInstance().getQueryAdapterFactory().dropDataTables();
      DwClientConfiguration.clearAllConfigurations();
    } catch (SQLException e1) {
      System.err.println("For testing is a special db requiered: "
              + "db-name: padawan_test, user: padawan_test, password: padawan_test");
      System.out.println("You can reate this db and user by running the following MySQL-commands:");
      System.out.println("CREATE DATABASE `padawan_test` /*!40100 COLLATE 'utf8mb4_unicode_ci' */;");
      System.out.println("CREATE USER 'padawan_test'@'%' IDENTIFIED BY 'padawan_test';");
      System.out.println("GRANT USAGE ON *.* TO 'padawan_test'@'%';");
      System.out.println(
              "GRANT SELECT, EXECUTE, SHOW VIEW, ALTER, ALTER ROUTINE, CREATE, CREATE ROUTINE, "
                      + "CREATE TEMPORARY TABLES, CREATE VIEW, DELETE, DROP, EVENT, INDEX, INSERT, "
                      + "REFERENCES, TRIGGER, UPDATE, "
                      + "LOCK TABLES  ON `padawan_test`.* TO 'padawan_test'@'%' "
                      + "WITH GRANT OPTION;");
      System.out.println("FLUSH PRIVILEGES;");
      throw e1;
    }
  }

  protected void loadConfig() throws IOException, URISyntaxException {
    File file = getConfigFile();
    Properties props = new Properties();
    props.load(new FileInputStream(file));
    DwClientConfiguration.mergeProperties(props);
  }

  private File getDefaultConfigFile() throws URISyntaxException {
    URL resource = getClass().getClassLoader().getResource("config/SolrTest.properties");
    return Paths.get(resource.toURI()).toFile();
  }

  protected File getConfigFile() throws URISyntaxException, FileNotFoundException, IOException {
    File result = null;
    String testConfigsPath = System.getenv("PADAWAN_TEST_CONFIG");
    if (testConfigsPath != null) {
      result = new File(testConfigsPath, "SolrTest.properties");
      if (!result.exists()) {
        System.out.println("'SolrTest.properties' does not exist in folder '" + testConfigsPath
                + "'. Using default SolrTest.properties from resources folder.");
        result = getDefaultConfigFile();
      }
    } else {
      System.out.println(
              "PADAWAN_TEST_CONFIG environment variable not set. Using default SolrTest.properties from resources folder.");
      result = getDefaultConfigFile();
    }
    return result;
  }

}
