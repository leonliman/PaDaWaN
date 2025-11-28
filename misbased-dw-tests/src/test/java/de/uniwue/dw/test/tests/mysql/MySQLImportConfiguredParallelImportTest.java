package de.uniwue.dw.test.tests.mysql;

import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.dw.test.tests.ImportConfiguredTest;
import de.uniwue.dw.tests.SystemTestParameters;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.util.ConfigException;
import org.junit.BeforeClass;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

public class MySQLImportConfiguredParallelImportTest extends ImportConfiguredTest {

  private static DBType dbType = DBType.MySQL;

  private static QueryEngineType engineType = QueryEngineType.SQL;

  public MySQLImportConfiguredParallelImportTest() throws Exception {
    super(dbType, engineType);
  }

  @BeforeClass
  public static void initialize() throws Exception {
    initialize(dbType, engineType,
            SystemTestParameters.getInstance().getCONFIGURED_IMPORT_PARALLEL_TEST_CONFIG_FILE(),
            ImportDumpMode.None, MySQLImportConfiguredParallelImportTest.class);
  }

  public static void main(String[] args) throws Exception {
    MySQLImportConfiguredParallelImportTest.initialize();
    MySQLImportConfiguredParallelImportTest test = new MySQLImportConfiguredParallelImportTest();
    doTests(test);
  }

}
