package de.uniwue.dw.test.tests.mysql;

import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.test.tests.engines.SQLQueryLogicTest;
import de.uniwue.misc.sql.DBType;
import org.junit.BeforeClass;

public class MySQLSQLQueryLogicTest extends SQLQueryLogicTest {

  private static final DBType dbType = DBType.MySQL;

  private static final QueryEngineType engineType = QueryEngineType.SQL;

  public MySQLSQLQueryLogicTest() throws Exception {
    super(dbType, engineType);
  }

  public static void main(String[] args) throws Exception {
    MySQLSQLQueryLogicTest.initialize();
    MySQLSQLQueryLogicTest test = new MySQLSQLQueryLogicTest();
    doTests(test);
  }

  @BeforeClass
  public static void initialize() throws Exception {
    initialize(dbType, engineType, testPropertiesFile, ImportDumpMode.ExchangeFormat, MySQLSQLQueryLogicTest.class);
  }

}
