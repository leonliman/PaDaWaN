package de.uniwue.dw.test.tests.mysql;

import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.dw.test.tests.CreateTablesTest;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.util.ConfigException;
import org.junit.BeforeClass;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

public class MySQLCreateTablesTest extends CreateTablesTest {

  private static DBType dbType = DBType.MySQL;

  public MySQLCreateTablesTest() {
    super(dbType);
  }

  public static void main(String[] args) throws Exception {
    MySQLCreateTablesTest.initialize();
    new MySQLCreateTablesTest();
    doTests();
  }

  @BeforeClass
  public static void initialize() throws Exception {
    initialize(dbType, engineType, testPropertiesFile, ImportDumpMode.None, MySQLCreateTablesTest.class);
  }

}
