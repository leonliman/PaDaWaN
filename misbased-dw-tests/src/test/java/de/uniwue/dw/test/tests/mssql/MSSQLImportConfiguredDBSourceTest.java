package de.uniwue.dw.test.tests.mssql;

import org.junit.BeforeClass;

import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.test.tests.ImportConfiguredTest;
import de.uniwue.dw.tests.SystemTestParameters;
import de.uniwue.misc.sql.DBType;

public abstract class MSSQLImportConfiguredDBSourceTest extends ImportConfiguredTest {

  private static DBType dbType = DBType.MSSQL;

  private static QueryEngineType engineType = QueryEngineType.SQL;

  public MSSQLImportConfiguredDBSourceTest() throws Exception {
    super(dbType, engineType);
  }

  @BeforeClass
  public static void initialize() throws Exception {
    // fill the first database with data
    MSSQLImportDumpTest.initialize();
    MSSQLImportDumpTest.finish();
    // suck the data from the first database and push it into the seconds database
    initialize(dbType, engineType, SystemTestParameters.getInstance().getCONFIGURED_DBSOURCE_IMPORT_TEST_CONFIG_FILE(),
            ImportDumpMode.None, MSSQLImportConfiguredDBSourceTest.class);
  }

//  public static void main(String[] args) throws Exception {
//    MSSQLImportConfiguredDBSourceTest.initialize();
//    MSSQLImportConfiguredDBSourceTest test = new MSSQLImportConfiguredDBSourceTest();
//    doTests(test);
//  }

}
