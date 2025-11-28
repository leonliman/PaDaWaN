package de.uniwue.dw.test.tests.mssql;

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
public class MSSQLImportSQLDumpTest extends ImportSQLDumpTest {

  private static DBType dbType = DBType.MSSQL;

  private static QueryEngineType engineType = QueryEngineType.SQL;

  public MSSQLImportSQLDumpTest() throws Exception {
    super(dbType, engineType);
  }

  public static void main(String[] args) throws Exception {
    MSSQLImportSQLDumpTest.initialize();
    MSSQLImportSQLDumpTest test = new MSSQLImportSQLDumpTest();
    doTests(test);
  }

  @BeforeClass
  public static void initialize() throws Exception {
    initialize(dbType, engineType, MSSQLImportSQLDumpTest.class);
  }

}
