package de.uniwue.dw.test.tests;

import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.tests.AbstractTest;
import de.uniwue.dw.tests.SystemTestParameters;
import de.uniwue.misc.sql.DBType;
import org.junit.Test;

import javax.security.auth.login.AccountException;
import java.io.File;

public abstract class ImportSQLUpdatesTest extends AbstractTest {

  protected static File testPropertiesFile1 = SystemTestParameters.getInstance()
          .getIMPORT_SQLUPDATES_TEST_CONFIG_FILE1();

  protected static File testPropertiesFile2 = SystemTestParameters.getInstance()
          .getIMPORT_SQLUPDATES_TEST_CONFIG_FILE2();

  public ImportSQLUpdatesTest(DBType aType, QueryEngineType engineType) {
    super(aType, engineType);
  }

  public static void initialize(DBType dbType, QueryEngineType engineType, Class aClass)
          throws Exception {
    initAbstractTest(testPropertiesFile1, dbType, engineType, ImportDumpMode.ClassicFormat, aClass);
    initAbstractTest(testPropertiesFile2, dbType, engineType, ImportDumpMode.None, aClass, true);
    createGUIClient(dbType, engineType);
  }

  public static void doTests(ImportSQLUpdatesTest test) throws Exception {
    test.testLabor();
    finish();
  }

  @Test
  public void testLabor() throws QueryException, AccountException {
    startTest("ImportTests/LaborSQLUpdatesTest", queryClient);
  }

  @Test
  public void testCatheter() throws QueryException, AccountException {
    startTest("ImportTests/CatheterUpdatesTest", queryClient);
  }

}
