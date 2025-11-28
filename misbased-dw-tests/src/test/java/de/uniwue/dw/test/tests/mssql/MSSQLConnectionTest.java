package de.uniwue.dw.test.tests.mssql;

import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.dw.test.tests.ConnectionTest;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.util.ConfigException;
import org.junit.BeforeClass;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

public class MSSQLConnectionTest extends ConnectionTest {

  private static DBType dbType = DBType.MSSQL;

  public MSSQLConnectionTest() {
    super(dbType);
  }

  public static void main(String[] args) throws Exception {
    MSSQLConnectionTest.initialize();
    MSSQLConnectionTest test = new MSSQLConnectionTest();
    doTests(test);
  }

  @BeforeClass
  public static void initialize() throws Exception {
    initialize(dbType, engineType, testPropertiesFile, ImportDumpMode.None, MSSQLConnectionTest.class);
  }

}
