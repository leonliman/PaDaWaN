package de.uniwue.dw.test.tests.mysql;

import de.uniwue.dw.core.model.manager.DataSourceException;
import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.dw.test.tests.ImportSQLDumpTest;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.util.ConfigException;
import org.junit.BeforeClass;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

/*
 * This test imports a big SQL statement (instead of bulk insert dumps)
 */
public class MySQLImportSQLDumpTest extends ImportSQLDumpTest {

  private static DBType dbType = DBType.MySQL;

  private static QueryEngineType engineType = QueryEngineType.SQL;

  public MySQLImportSQLDumpTest() throws Exception {
    super(dbType, engineType);
  }

  public static void main(String[] args) throws Exception {
    MySQLImportSQLDumpTest.initialize();
    MySQLImportSQLDumpTest test = new MySQLImportSQLDumpTest();
    doTests(test);
  }

  @BeforeClass
  public static void initialize() throws Exception {
    initialize(dbType, engineType, MySQLImportSQLDumpTest.class);
  }

}
