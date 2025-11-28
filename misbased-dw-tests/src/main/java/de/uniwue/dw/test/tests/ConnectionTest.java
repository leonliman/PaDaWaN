package de.uniwue.dw.test.tests;

import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.tests.AbstractTest;
import de.uniwue.dw.tests.SystemTestParameters;
import de.uniwue.misc.sql.DBType;
import org.junit.Test;

import java.io.File;

public abstract class ConnectionTest extends AbstractTest {

  protected static File testPropertiesFile = SystemTestParameters.getInstance()
          .getCONNECTION_TEST_CONFIG_FILE();

  protected static QueryEngineType engineType = QueryEngineType.SQL;

  public ConnectionTest(DBType aDBType) {
    super(aDBType, engineType);
  }

  public static void doTests(ConnectionTest test) throws Exception {
    test.testConnection();
    finish();
  }

  @Test
  public void testConnection() {
  }

}
