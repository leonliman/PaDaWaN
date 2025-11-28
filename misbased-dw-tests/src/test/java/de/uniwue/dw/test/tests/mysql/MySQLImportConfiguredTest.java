package de.uniwue.dw.test.tests.mysql;

import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.dw.test.tests.ImportConfiguredTest;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.util.ConfigException;
import org.junit.BeforeClass;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

public class MySQLImportConfiguredTest extends ImportConfiguredTest {

  private static DBType dbType = DBType.MySQL;

  private static QueryEngineType engineType = QueryEngineType.SQL;

  public MySQLImportConfiguredTest() throws Exception {
    super(dbType, engineType);
  }

  @BeforeClass
  public static void initialize() throws Exception {
    initialize(dbType, engineType, testPropertiesFile, ImportDumpMode.None, MySQLImportConfiguredTest.class);
  }

  public static void main(String[] args) throws Exception {
    MySQLImportConfiguredTest.initialize();
    MySQLImportConfiguredTest test = new MySQLImportConfiguredTest();
    doTests(test);
  }

}
