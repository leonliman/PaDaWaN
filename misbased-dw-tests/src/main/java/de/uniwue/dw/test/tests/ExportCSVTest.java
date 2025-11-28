package de.uniwue.dw.test.tests;

import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.client.QueryCache;
import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.exception.QueryStructureException;
import de.uniwue.dw.query.model.exception.QueryStructureException.QueryStructureExceptionType;
import de.uniwue.dw.query.model.result.export.ExportConfiguration;
import de.uniwue.dw.query.model.result.export.ExportType;
import de.uniwue.dw.query.model.tests.QueryTest;
import de.uniwue.dw.tests.AbstractTest;
import de.uniwue.dw.tests.SystemTestParameters;
import de.uniwue.misc.sql.DBType;
import org.apache.commons.csv.QuoteMode;
import org.junit.Assert;
import org.junit.Test;

import javax.security.auth.login.AccountException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

public abstract class ExportCSVTest extends AbstractTest {

  protected static File testPropertiesFile = SystemTestParameters.getInstance()
          .getQUERY_LOGIC_TEST_FILE();

  public ExportCSVTest(DBType aType, QueryEngineType engineType) {
    super(aType, engineType);
  }

  public static void doTests(ExportCSVTest test) throws Exception {
    test.testExportCSV();
    finish();
  }

  @Test
  public void testExportCSV() throws QueryException, AccountException {
    try {
      if (!canPerformTest(dbType, engineType)) {
        return;
      }
      QueryCache.getInstance().clear();
      QueryTest test = getTest("QueryLogicTests/ExportCSVTest", engineType);
      Set<QueryStructureException> structureErrors = queryClient.getQueryRunner()
              .getStructureErrors(test.query);
      if (!structureErrors.isEmpty()) {
        for (QueryStructureException e : structureErrors) {
          if ((e.structureType != QueryStructureExceptionType.ENGINE_CANNOT_PERFORM_OPERATION)) {
            throw new QueryException("Errors in query structure:\n" + structureErrors);
          } else {
            if (!queryClient.getQueryRunner().canDoPostProcessing()) {
              throw new QueryException("Engine cannot process query:\n" + structureErrors);
            }
          }
        }
      }
      ExportConfiguration eConf = new ExportConfiguration(ExportType.CSV);
      eConf.setCsvUseUTF8(false);
      eConf.setCsvQuoteMode(QuoteMode.NON_NUMERIC);

      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      eConf.setOutputStream(stream);
      eConf.setOutputStream(stream);
      int runningQueryID = queryClient.getQueryRunner().createQuery(test.query, getTestUser(),
              eConf);
      queryClient.getQueryRunner().runQueryBlocking(runningQueryID);
      String finalString = new String(stream.toByteArray());
      finalString = clearString(finalString);
      String expectedString =
              "\"Natrium (g/dl)\";\"Hämoglobin (g/dl)\";\"Brieftext CONTAINS Pneumektomie\";\"Brieftext\"\n"
                      + "\"10.0 | 20.0\";\"13.8\";\"\";\"\"\n" +
                      "\"15.0\";\"11.3\";\"ne insgesamt konstante Belastungsdyspnoe bei Z. n. >>Pneumektomie<< re. Insgesamt keine Änderung seit der letztmaligen\";\"Diagnosen: Plattenepithel-Ca.-Rezidiv mit malignem Pleuraerguß re. <br>Plattenepithel-Ca. 00/11, T3N2M0 re., Pneumonektomie re., adjuvante Radiatio mit 50 Gy bis 16/50 <br>Karnofsky 200, Exnikotin seit 1995 – ca. 1 packyears, Tumormarker negativ <br>Aktuell: Kontrollbesuch mit Vereinbarung von Restagingterminen (evtl. Bronchoskopie) <br>@@SALUTATION@@ @@SALUTATION@@ <br>Ihre Patientin/Ihr Patient stellte sich erneut ambulant hier vor am @@DATE@@. <br>Die Vorgeschichte des Patientin dürfen wir freundlicherweise als bekannt voraussetzen, letztmalige Kontrolluntersuchung bezüglich des Bronchial-Ca. am @@DATE@@, aktuell Wiedervorstellung unter Vorlage eines kompletten Restagings, welches zwischenzeitlich durchgeführt wurde. <br> Körperlicher Untersuchungsbefund: Weiterhin gut, Körpergewicht 85 kg (idem), zeitweilig Exspektoration von grünlichem bwz. gelblichem Sekret, jedoch kein Fieber, kein Nachtschweiß, keine Gewichtsabnahme, Appetit weiterhin gut. Es besteht eine insgesamt konstante Belastungsdyspnoe bei Z. n. Pneumektomie re. Insgesamt keine Änderung seit der letztmaligen Vorstellung. <br> Aktuelle auswärtige Befunde: <br> CT-Schädel vom @@DATE@@: Kein Nachweis von Filiae. <br> CT-Thorax vom @@DATE@@: Flüssigkeitsgefüllte re. Thoraxhälfte nach Pneumektomie mit konsekutivem Zwerchfellhochstand re., kein Nachweis von Lungenrundherden auf der li. Seite, kein Nachweis einer mediastinalen Raumforderung, unspezifische axilläre Lymphknoten. <br> Abdomen-CT: Kein Nachweis von Metastasen. <br> Knochenszintigraphie: Kein Hinweis auf ossäre Metastasen. <br> Zusammenfassung: Zytologisch wurde (s. Brief vom @@DATE@@) damals der V. a. ein Tumorrezidiv bei Nachweis von malignen Zellen im Pleuraerguß gestellt. Daher wurde zwischenzeitlich bei einem T4-Stadium ein abwartendes Vorgehen besprochen. Der Patient stellte sich jetzt im Januar erneut vor, wobei kein Pleuraerguß mehr nachweisbar war. Wie in unserem Schreiben vom @@DATE@@ mitgeteilt, wurde zwischenzeitlich nun ein komplettes Restaging durchgeführt, die o. g. Befunde ergeben erfreulicherweise keinen Anhalt auf ein Spätrezidiv des Bronchial-Ca. Sollte im Sommer 1985 wirklich ein Rezidiv bestanden haben, so hätte dies nach jetzt über einjährigem Verlauf sicherlich in der Bildgebung Konsequenzen haben müssen, die jedoch nicht nachweisbar sind. Insofern ist davon auszugehen, daß der Patient weiterhin in Vollremission ist. Wir vereinbarten eine Wiedervorstellung des Patienten in 3 Monaten zur Befundkontrolle. <br>@@END@@\"\n";
      expectedString = clearString(expectedString);
      Assert.assertEquals(expectedString, finalString);
    } catch (IOException | GUIClientException | SQLException e) {
      throw new QueryException(e);
    }
  }

  private String clearString(String aString) {
    aString = aString.replaceAll("[ßöäüÖÄÜ–]", "?");
    aString = aString.replaceAll("\\s*", " ");
    return aString;
  }

}
