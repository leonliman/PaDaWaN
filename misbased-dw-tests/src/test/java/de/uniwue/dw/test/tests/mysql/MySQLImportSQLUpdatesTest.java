package de.uniwue.dw.test.tests.mysql;

import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.test.tests.ImportSQLUpdatesTest;
import de.uniwue.misc.sql.DBType;
import org.junit.BeforeClass;

public class MySQLImportSQLUpdatesTest extends ImportSQLUpdatesTest {

  private static final DBType dbType = DBType.MySQL;

  private static final QueryEngineType engineType = QueryEngineType.SQL;

  public MySQLImportSQLUpdatesTest() throws Exception {
    super(dbType, engineType);
  }

  public static void main(String[] args) throws Exception {
    MySQLImportSQLUpdatesTest.initialize();
    MySQLImportSQLUpdatesTest test = new MySQLImportSQLUpdatesTest();
    doTests(test);
  }

  @BeforeClass
  public static void initialize() throws Exception {
    initialize(dbType, engineType, MySQLImportSQLUpdatesTest.class);
  }

}
