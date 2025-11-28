package de.uniwue.dw.test.tests.mssql;

import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.test.tests.engines.SQLQueryLogicTest;
import de.uniwue.misc.sql.DBType;
import org.junit.BeforeClass;

public class MSSQLSQLQueryLogicTest extends SQLQueryLogicTest {

  private static DBType dbType = DBType.MSSQL;

  private static QueryEngineType engineType = QueryEngineType.SQL;

  public MSSQLSQLQueryLogicTest() throws Exception {
    super(dbType, engineType);
  }

  public static void main(String[] args) throws Exception {
    MSSQLSQLQueryLogicTest.initialize();
    MSSQLSQLQueryLogicTest test = new MSSQLSQLQueryLogicTest();
    doTests(test);
  }

  @BeforeClass
  public static void initialize() throws Exception {
    initialize(dbType, engineType, testPropertiesFile, ImportDumpMode.ExchangeFormat, MSSQLSQLQueryLogicTest.class);
  }

}
