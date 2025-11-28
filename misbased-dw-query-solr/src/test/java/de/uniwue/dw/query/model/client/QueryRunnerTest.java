package de.uniwue.dw.query.model.client;

import de.uniwue.dw.core.model.manager.DataSourceException;
import de.uniwue.dw.core.model.manager.ICatalogClientManager;
import de.uniwue.dw.query.model.TestEnvironmentDataLoader;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.exception.QueryStructureException;
import de.uniwue.dw.query.model.exception.QueryStructureException.QueryStructureExceptionType;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.result.Cell;
import de.uniwue.dw.query.model.result.Result;
import de.uniwue.dw.query.model.result.export.ExportConfiguration;
import de.uniwue.dw.query.model.result.export.ExportType;
import de.uniwue.dw.query.model.result.export.MemoryOutputHandler;
import de.uniwue.dw.query.model.tests.QueryTest;
import de.uniwue.dw.query.model.tests.QueryTestLoader;
import de.uniwue.misc.util.ConfigException;
import org.junit.Before;
import org.junit.Test;

import javax.security.auth.login.AccountException;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class QueryRunnerTest {

  private ICatalogClientManager catalocClientManager;

  private IGUIClient guiClient;

  public abstract TestEnvironmentDataLoader getTestEnvironmentDataLoader() throws DataSourceException;

  @Before
  public void prepare() throws IOException, IndexException, SQLException, URISyntaxException, NoSuchAlgorithmException,
          GUIClientException, DataSourceException, ConfigException {
    TestEnvironmentDataLoader dataLoader = getTestEnvironmentDataLoader();
    dataLoader.loadDataIfNecessary();
    guiClient = dataLoader.getGuiClient();
    catalocClientManager = dataLoader.getCompleteCatalogClientManager();
  }

  @Test
  public void simpleTest() throws QueryException, IOException, URISyntaxException, AccountException, SQLException {
    testFolder("attributeTests/");
    testFolder("dateTimeTests/");
    testFolder("logicTests/");
    testFolder("queryTests/");
    testFolder("textQueryTests/");
    testFolder("complexTests/");
  }

  private void testFolder(String path) throws QueryException, URISyntaxException, AccountException, SQLException {
    String testPath = QueryTestLoader.QUERY_FOLDER + path;
    URL resource = getClass().getClassLoader().getResource(testPath);
    if (resource == null) {
      return;
    }
    File folder = Paths.get(resource.toURI()).toFile();
    File[] testFiles = folder.listFiles();
    if (testFiles != null) {
      for (File filename : testFiles) {
        if (filename.isFile()) {
//          if (!filename.getName().equals("betweenIntervalTest.txt"))
//            continue;
          test(testPath + filename.getName());
        }
      }
    }
  }

  private void test(String filename) throws QueryException, AccountException, SQLException {
    try {
      System.out.println("testing " + filename);
      QueryTestLoader queryLoader = new QueryTestLoader(this.catalocClientManager);
      QueryTest test = queryLoader.readTest(filename);
      Result actualResult = query(test.query);
      testEquals(test.desiredResult, actualResult, filename);
    } catch (GUIClientException | IOException e) {
      throw new QueryException(e);
    }
  }

  private Result query(QueryRoot root) throws QueryException, AccountException, SQLException {
    try {
      QueryCache.getInstance().clear();
      Set<QueryStructureException> structureErrors = guiClient.getQueryRunner().getStructureErrors(root);
      if (!structureErrors.isEmpty()) {
        for (QueryStructureException e : structureErrors) {
          if (e.structureType != QueryStructureExceptionType.ENGINE_CANNOT_PERFORM_OPERATION) {
            fail("Errors in query structure:\n" + structureErrors);
          }
        }
      }
      ExportConfiguration eConf = new ExportConfiguration(ExportType.RESULT_TABLE);
      int runningQueryID = guiClient.getQueryRunner().createQuery(root, TestEnvironmentDataLoader.getTestUser1(),
              eConf);
      Result result = guiClient.getQueryRunner().runQueryBlocking(runningQueryID);
      return result;
    } catch (GUIClientException e) {
      throw new QueryException(e);
    }
  }

  protected void testEquals(Result expected, Result actual, String filename) {
    String sortedActual = serializeAndSort2DArray(to2DStringArray(actual));
    String sortedExpected = serializeAndSort2DArray(to2DStringArray(expected));
    // String sDesiredResults = serializeAndSort2DArray(desiredResults);
    // check number of rows
    assertEquals(filename + " number of rows:", expected.getRows().size(), actual.getRows().size());

    for (int i = 0; i < expected.getRows().size(); i++) {
      // check number of columns in row i
      assertEquals("columns in row " + i + ":", expected.getRows().get(i).getCells().size(),
              actual.getRows().get(i).getCells().size());
    }
    // check results
    assertEquals(sortedExpected, sortedActual);
  }

  protected String[][] to2DStringArray(Result list) {
    if (list.getRows().size() < 1) {
      return new String[0][0];
    }
    String[][] res = new String[list.getRows().size()][list.getRows().get(0).getCells().size()];
    for (int i = 0; i < res.length; i++) {
      for (int j = 0; j < res[i].length; j++) {
        Cell cell = list.getCell(i, j);
        if (cell.getValueAsString() == null) {
          res[i][j] = "";
        } else {
          res[i][j] = MemoryOutputHandler.defaultFormatter.getFormattedValue(cell);
        }
      }
    }
    return res;
  }

  protected String serializeAndSort2DArray(String[][] array) {
    String res = "";
    for (int i = 0; i < array.length; i++) {
      Arrays.sort(array[i]);
      for (int j = 0; j < array[i].length - 1; j++) {
        res = res + array[i][j] + "|";
      }
      if (array[i].length == 0) {
        continue;
      }
      res += array[i][array[i].length - 1];
      res += "===";
    }
    String[] temp = res.split("===");
    Arrays.sort(temp);
    res = "";
    for (int i = 0; i < temp.length; i++) {
      res += temp[i] + "===";
    }
    return res;
  }

}
