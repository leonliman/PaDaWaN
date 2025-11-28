package de.uniwue.dw.test.tests.mysql;

import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.test.tests.IndexingTest;
import de.uniwue.misc.sql.DBType;
import org.junit.BeforeClass;

public class MySQLSolrIndexingTest extends IndexingTest {

  protected static QueryEngineType engineType = QueryEngineType.Solr;

  private static final DBType dbType = DBType.MySQL;

  public MySQLSolrIndexingTest() {
    super(dbType, engineType);
  }

  public static void main(String[] args) throws Exception {
    MySQLSolrIndexingTest.initialize();
    MySQLSolrIndexingTest test = new MySQLSolrIndexingTest();
    doTests(test);
  }

  @BeforeClass
  public static void initialize() throws Exception {
    initialize(dbType, engineType, testPropertiesFile, ImportDumpMode.ClassicFormat, MySQLSolrIndexingTest.class);
  }
}
