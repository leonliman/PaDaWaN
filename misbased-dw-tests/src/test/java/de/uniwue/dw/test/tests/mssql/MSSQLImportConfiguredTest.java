package de.uniwue.dw.test.tests.mssql;

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

public class MSSQLImportConfiguredTest extends ImportConfiguredTest {

  private static DBType dbType = DBType.MSSQL;

  private static QueryEngineType engineType = QueryEngineType.SQL;

  public MSSQLImportConfiguredTest() throws Exception {
    super(dbType, engineType);
  }

  @BeforeClass
  public static void initialize() throws Exception {
    initialize(dbType, engineType, testPropertiesFile, ImportDumpMode.None, MSSQLImportConfiguredTest.class);
  }

  public static void main(String[] args) throws Exception {
    MSSQLImportConfiguredTest.initialize();
    MSSQLImportConfiguredTest test = new MSSQLImportConfiguredTest();
    doTests(test);
  }

}
