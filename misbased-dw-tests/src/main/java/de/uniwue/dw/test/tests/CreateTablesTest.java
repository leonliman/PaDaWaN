package de.uniwue.dw.test.tests;

import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.tests.AbstractTest;
import de.uniwue.dw.tests.SystemTestParameters;
import de.uniwue.misc.sql.DBType;
import org.junit.Test;

import java.io.File;

public abstract class CreateTablesTest extends AbstractTest {

  protected static File testPropertiesFile = SystemTestParameters.getInstance().getCREATE_TABLES_TEST_CONFIG_FILE();

  protected static QueryEngineType engineType = QueryEngineType.SQL;

  public CreateTablesTest(DBType aType) {
    super(aType, engineType);
  }

  public static void doTests() throws Exception {
    finish();
  }

  @Test
  public void testCreateTables() {
  }

}
