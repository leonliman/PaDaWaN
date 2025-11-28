package de.uniwue.dw.test.tests.mssql;

import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.dw.test.tests.CreateTablesTest;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.util.ConfigException;
import org.junit.BeforeClass;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

public class MSSQLCreateTablesTest extends CreateTablesTest {

  private static DBType dbType = DBType.MSSQL;

  public MSSQLCreateTablesTest() {
    super(dbType);
  }

  public static void main(String[] args) throws Exception {
    MSSQLCreateTablesTest.initialize();
    new MSSQLCreateTablesTest();
    doTests();
  }

  @BeforeClass
  public static void initialize()
          throws Exception {
    initialize(dbType, engineType, testPropertiesFile, ImportDumpMode.None, MSSQLCreateTablesTest.class);
  }

}
