package de.uniwue.dw.test.tests;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.manager.DataSourceException;
import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.tests.AbstractTest;
import de.uniwue.misc.sql.DBType;
import org.junit.Assert;
import org.junit.Test;

import javax.security.auth.login.AccountException;
import java.sql.SQLException;

public abstract class ImportTest extends AbstractTest {

  public ImportTest(DBType aType, QueryEngineType engineType) {
    super(aType, engineType);
  }

  public static void doTests(ImportTest test) throws Exception {
    test.testAlter();
    test.testArztbriefe();
    test.testAufnahmeEntlass();
    test.testDiag();
    test.testDiagType();
    test.testEcho();
    test.testFallArt();
    test.testGeschlecht();
    test.testGestorben();
    test.testHerzkatheder();
    test.testLabor();
    test.testOPS();
    test.testStation();
    test.testLaborKatalogMetaNumInfos();
    test.testGeschlechtChoices();
    finish();
  }

  @Test
  public void testLaborKatalogMetaNumInfos()
          throws DataSourceException, AccountException, SQLException {
    if (canPerformTest(dbType, engineType)) {
      CatalogEntry entry = queryClient.getCatalogClientProvider().getEntryByRefID("0000000002",
              "Labor", getTestUser());
      Assert.assertEquals("Lowerbound", 12.2, entry.getLowBound(), 0.0);
    }
  }

  @Test
  public void testGeschlechtChoices()
          throws DataSourceException, AccountException, SQLException {
    if (canPerformTest(dbType, engineType)) {
      CatalogEntry entry = queryClient.getCatalogClientProvider().getEntryByRefID("Geschlecht",
              "Geschlecht", getTestUser());
      Assert.assertEquals(2, entry.getSingleChoiceChoice().size());
    }
  }

  @Test
  public void testAlter() throws QueryException, AccountException {
    startTest("ImportTests/AlterTest", queryClient);
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
  public void testDiagType() throws QueryException, AccountException {
    startTest("ImportTests/DiagTypeTest", queryClient);
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
