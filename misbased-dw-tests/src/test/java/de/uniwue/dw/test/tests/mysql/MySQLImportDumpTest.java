package de.uniwue.dw.test.tests.mysql;

import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.dw.test.tests.ImportDumpTest;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.util.ConfigException;
import org.junit.BeforeClass;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

/*
 * This test imports bulk insert dumps that look like the bulk insert files that are regularily imported
 */
public class MySQLImportDumpTest extends ImportDumpTest {

  private static DBType dbType = DBType.MySQL;

  private static QueryEngineType engineType = QueryEngineType.SQL;

  public MySQLImportDumpTest() throws Exception {
    super(dbType, engineType);
  }

  public static void main(String[] args) throws Exception {
    MySQLImportDumpTest.initialize();
    MySQLImportDumpTest test = new MySQLImportDumpTest();
    doTests(test);
  }

  @BeforeClass
  public static void initialize() throws Exception {
    initialize(dbType, engineType, MySQLImportDumpTest.class);
  }
}
