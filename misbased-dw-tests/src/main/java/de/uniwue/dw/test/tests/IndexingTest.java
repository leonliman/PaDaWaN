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

public abstract class IndexingTest extends AbstractTest {

  protected static File testPropertiesFile = SystemTestParameters.getInstance()
          .getINDEX_TEST_CONFIG_FILE();

  public IndexingTest(DBType aType, QueryEngineType engineType) {
    super(aType, engineType);
  }

  public static void doTests(IndexingTest test) throws Exception {
    test.testArztbriefe();
    test.testAlter();
    test.testAufnahmeEntlass();
    test.testDiag();
    test.testEcho();
    test.testFallArt();
    test.testGeschlecht();
    test.testGestorben();
    test.testHerzkatheder();
    test.testLabor();
    test.testOPS();
    test.testStation();
    test.testPID();
    test.testCaseID();
    test.testCatalog();
    finish();
  }

  @Test
  public void testAlter() throws QueryException, AccountException {
    startTest("ImportTests/AlterTest", queryClient);
  }

  @Test
  public void testCatalog()
          throws DataSourceException, AccountException, SQLException {
    if (!canPerformTest(dbType, engineType)) {
      return;
    }
    CatalogEntry stammdatenEntry = queryClient.getCatalogClientProvider()
            .getEntryByRefID("stammdaten", "", getTestUser());
    Assert.assertNotNull(stammdatenEntry);
    CatalogEntry alterEntry = queryClient.getCatalogClientProvider().getEntryByRefID("alter",
            "alter", getTestUser());
    Assert.assertNotNull(alterEntry);
  }

  @Test
  public void testPID() throws QueryException, AccountException {
    startTest("ImportTests/PIDTest", queryClient);
  }

  @Test
  public void testCaseID() throws QueryException, AccountException {
    startTest("ImportTests/CaseIDTest", queryClient);
  }

  @Test
  public void testArztbriefe() throws QueryException, AccountException {
    startTest("ImportTests/ArztbriefTest", queryClient);
  }

  @Test
  public void testAufnahmeEntlass() throws QueryException, AccountException {
    startTest("ImportTests/AufnahmeEntlassTest", queryClient);
  }

  @Test
  public void testGeschlecht() throws QueryException, AccountException {
    startTest("ImportTests/GeschlechtTest", queryClient);
  }

  @Test
  public void testGestorben() throws QueryException, AccountException {
    startTest("ImportTests/GestorbenTest", queryClient);
  }

  @Test
  public void testLabor() throws QueryException, AccountException {
    startTest("ImportTests/LaborTest", queryClient);
  }

  @Test
  public void testDiag() throws QueryException, AccountException {
    startTest("ImportTests/DiagTest1", queryClient);
  }

  @Test
  public void testOPS() throws QueryException, AccountException {
    startTest("ImportTests/OPSTest1", queryClient);
  }

  @Test
  public void testEcho() throws QueryException, AccountException {
    startTest("ImportTests/EchoTest1", queryClient);
  }

  @Test
  public void testStation() throws QueryException, AccountException {
    startTest("ImportTests/StationTest", queryClient);
  }

  @Test
  public void testFallArt() throws QueryException, AccountException {
    startTest("ImportTests/FallArtTest", queryClient);
  }

  @Test
  public void testHerzkatheder() throws QueryException, AccountException {
    startTest("ImportTests/HerzkatheterTest", queryClient);
  }

}
