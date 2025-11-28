package de.uniwue.dw.test.tests.mysql;

import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.test.tests.ExportCSVTest;
import de.uniwue.misc.sql.DBType;
import org.junit.BeforeClass;

public class MySQLSolrExportCSVTest extends ExportCSVTest {

  private static final DBType dbType = DBType.MySQL;

  private static final QueryEngineType engineType = QueryEngineType.Solr;

  public MySQLSolrExportCSVTest() {
    super(dbType, engineType);
  }

  public static void main(String[] args) throws Exception {
    MySQLSolrExportCSVTest.initialize();
    MySQLSolrExportCSVTest test = new MySQLSolrExportCSVTest();
    doTests(test);
  }

  @BeforeClass
  public static void initialize()
          throws Exception {
    initialize(dbType, engineType, testPropertiesFile, ImportDumpMode.ClassicFormat, MySQLSolrExportCSVTest.class);
  }
}
