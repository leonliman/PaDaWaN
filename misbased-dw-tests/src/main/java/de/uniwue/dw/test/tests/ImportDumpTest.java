package de.uniwue.dw.test.tests;

import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.tests.SystemTestParameters;
import de.uniwue.misc.sql.DBType;

import java.io.File;

/*
 * This test imports bulk insert dumps that look like the bulk insert files that are regularly imported
 */
public abstract class ImportDumpTest extends ImportTest {

  protected static File testPropertiesFile = SystemTestParameters.getInstance()
          .getIMPORT_DUMP_TEST_CONFIG_FILE();

  public ImportDumpTest(DBType aType, QueryEngineType engineType) throws Exception {
    super(aType, engineType);
  }

  public static void initialize(DBType dbType, QueryEngineType engineType, Class aClass)
          throws Exception {
    initialize(dbType, engineType, testPropertiesFile, ImportDumpMode.ClassicFormat, aClass);
  }

}
