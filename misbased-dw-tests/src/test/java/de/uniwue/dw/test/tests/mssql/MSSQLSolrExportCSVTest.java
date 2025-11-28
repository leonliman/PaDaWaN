package de.uniwue.dw.test.tests.mssql;

import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.dw.test.tests.ExportCSVTest;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.util.ConfigException;
import org.junit.BeforeClass;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

public class MSSQLSolrExportCSVTest extends ExportCSVTest {

  private static DBType dbType = DBType.MSSQL;

  private static QueryEngineType engineType = QueryEngineType.Solr;

  public MSSQLSolrExportCSVTest() {
    super(dbType, engineType);
  }

  public static void main(String[] args) throws Exception {
    MSSQLSolrExportCSVTest.initialize();
    MSSQLSolrExportCSVTest test = new MSSQLSolrExportCSVTest();
    doTests(test);
  }

  @BeforeClass
  public static void initialize()
          throws Exception {
    initialize(dbType, engineType, testPropertiesFile, ImportDumpMode.ClassicFormat, MSSQLSolrExportCSVTest.class);
  }
}
