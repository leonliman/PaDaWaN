package de.uniwue.dw.query.model.client;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.model.manager.ICatalogClientManager;
import de.uniwue.dw.query.model.DWQueryConfig;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.manager.QueryManipulationManager;
import de.uniwue.dw.query.model.tests.QueryShrinkTestLoader;

public class QueryShrinkTest {

  @Before
  public void perpare() throws IOException, IndexException, SQLException, URISyntaxException {
    loadConfig();
  }

  @Test
  public void simpleTest() throws QueryException, IOException, URISyntaxException, SQLException {
    // testFolder("statistic/");
  }

  private void testFolder(String path)
          throws QueryException, URISyntaxException, IOException, SQLException {
    URL resource = getClass().getClassLoader().getResource(QueryShrinkTestLoader.QUERY_FOLDER + path);
    File file = Paths.get(resource.toURI()).toFile();
    String[] testFiles = file.list();
    if (testFiles != null) {
      for (String string : testFiles) {
        test(path + string);
      }
    }
  }

  private void test(String string) throws QueryException, IOException, SQLException {
    System.out.println("testing " + string);
    QueryShrinkTestLoader queryLoader = new QueryShrinkTestLoader(QueryShrinkTestLoader.QUERY_FOLDER + string,
            getCompleteCatalogClientManager());
    QueryRoot input = queryLoader.getInput();
    QueryRoot expected = queryLoader.getExpected();
    QueryManipulationManager.shrinkQuery(input);
    testEquals(expected, input);

  }

  private void testEquals(QueryRoot expected, QueryRoot input) throws QueryException {
    String expectedXML = expected.generateXML();
    String actualXML = input.generateXML();
    assertEquals(expectedXML, actualXML);
  }

  public ICatalogClientManager getCompleteCatalogClientManager() throws SQLException {
    return DWQueryConfig.getInstance().getCatalogClientManager();
  }

  private void loadConfig() throws IOException, URISyntaxException {
    File file = getConfigFile();
    InputStream inputStream = Files.newInputStream(file.toPath());
    Properties props = new Properties();
    props.load(inputStream);
    inputStream.close();
    DwClientConfiguration.mergeProperties(props);

  }

  public File getConfigFile() throws URISyntaxException {
    URL resource = getClass().getClassLoader().getResource("config/test.properties");
    File file = Paths.get(resource.toURI()).toFile();
    return file;
  }

}
