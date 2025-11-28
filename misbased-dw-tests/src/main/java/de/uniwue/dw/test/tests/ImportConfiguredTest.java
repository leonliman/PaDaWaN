package de.uniwue.dw.test.tests;

import java.io.File;

import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.tests.SystemTestParameters;
import de.uniwue.misc.sql.DBType;

public abstract class ImportConfiguredTest extends ImportTest {

  protected static File testPropertiesFile = SystemTestParameters.getInstance()
          .getCONFIGURED_IMPORT_TEST_CONFIG_FILE();

  public ImportConfiguredTest(DBType aType, QueryEngineType engineType) throws Exception {
    super(aType, engineType);
  }

}
