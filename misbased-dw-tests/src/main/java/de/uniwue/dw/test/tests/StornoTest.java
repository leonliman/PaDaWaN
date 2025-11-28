package de.uniwue.dw.test.tests;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.manager.DataSourceException;
import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.tests.AbstractTest;
import de.uniwue.dw.tests.SystemTestParameters;
import de.uniwue.misc.sql.DBType;
import org.junit.Assert;
import org.junit.Test;

import javax.security.auth.login.AccountException;
import java.io.File;
import java.sql.SQLException;

public abstract class StornoTest extends AbstractTest {

  protected static File testPropertiesFile1 = SystemTestParameters.getInstance()
          .getSTORNO_TEST_CONFIG_FILE1();

  protected static File testPropertiesFile2 = SystemTestParameters.getInstance()
          .getSTORNO_TEST_CONFIG_FILE2();

  public StornoTest(DBType aType, QueryEngineType engineType) {
    super(aType, engineType);
  }

  public static void initialize(DBType dbType, QueryEngineType engineType, Class aClass)
          throws Exception {
    initAbstractTest(testPropertiesFile1, dbType, engineType, ImportDumpMode.ClassicFormat, aClass);
    initAbstractTest(testPropertiesFile2, dbType, engineType, ImportDumpMode.None, aClass);
    createGUIClient(dbType, engineType);
  }

  public static void doTests(StornoTest test) throws Exception {
    test.testDiagStorno();
    test.testGeschlechtStorno();
    test.testFaelleStorno();
    test.testCatalogCount();
    finish();
  }

  @Test
  public void testCatalogCount() throws DataSourceException, AccountException, SQLException {
    if (canPerformTest(dbType, engineType)) {
      CatalogEntry entry = queryClient.getCatalogClientProvider().getEntryByRefID("S42.21",
              "Diagnose", getTestUser());
      Assert.assertEquals("Diag Count", 0, entry.getCountDistinctPID(), 0.0);
      Assert.assertEquals("Diag Count", 0, entry.getCountDistinctCaseID(), 0.0);
      entry = queryClient.getCatalogClientProvider().getEntryByRefID("Geschlecht", "Geschlecht",
              getTestUser());
      Assert.assertEquals("Sex Count", 2, entry.getCountDistinctPID(), 0.0);
      Assert.assertEquals("Sex Count", 5, entry.getCountDistinctCaseID(), 0.0);
      entry = queryClient.getCatalogClientProvider().getEntryByRefID("CaseID", "MetaDaten",
              getTestUser());
      Assert.assertEquals("CaseID Count", 2, entry.getCountDistinctPID(), 0.0);
      Assert.assertEquals("CaseID Count", 5, entry.getCountDistinctCaseID(), 0.0);
    }
  }

  @Test
  public void testDiagStorno() throws QueryException, AccountException {
    startTest("StornoTests/DiagStornoTest", queryClient);
  }

  @Test
  public void testGeschlechtStorno() throws QueryException, AccountException {
    startTest("StornoTests/GeschlechtStornoTest", queryClient);
  }

  @Test
  public void testFaelleStorno() throws QueryException, AccountException {
    startTest("StornoTests/FaelleStornoTest", queryClient);
  }

}
