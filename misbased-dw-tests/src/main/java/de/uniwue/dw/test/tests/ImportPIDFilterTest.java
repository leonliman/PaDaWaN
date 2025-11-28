package de.uniwue.dw.test.tests;

import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.tests.AbstractTest;
import de.uniwue.dw.tests.SystemTestParameters;
import de.uniwue.misc.sql.DBType;

import java.io.File;

public abstract class ImportPIDFilterTest extends AbstractTest {

  protected static File testPropertiesFile = SystemTestParameters.getInstance()
          .getPID_FILTER_TEST_CONFIG_FILE();

  public ImportPIDFilterTest(DBType aType, QueryEngineType engineType) {
    super(aType, engineType);
  }

}
