package de.uniwue.dw.test.tests.mysql;

import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.solr.api.DWSolrConfig;
import de.uniwue.dw.test.tests.QueryLogicTest;
import de.uniwue.misc.sql.DBType;
import org.junit.BeforeClass;

public class MySQLSolrQueryLogicTest extends QueryLogicTest {

  private static final DBType dbType = DBType.MySQL;

  private static final QueryEngineType engineType = QueryEngineType.Solr;

  public MySQLSolrQueryLogicTest() {
    super(dbType, engineType);
  }

  public static void main(String[] args) throws Exception {
    MySQLSolrQueryLogicTest.initialize();
    MySQLSolrQueryLogicTest test = new MySQLSolrQueryLogicTest();
    doTests(test);
    finish();
    DWSolrConfig.clearAllConfigurations();
  }

  @BeforeClass
  public static void initialize()
          throws Exception {
    initialize(dbType, engineType, testPropertiesFile, ImportDumpMode.ExchangeFormat, MySQLSolrQueryLogicTest.class);
  }

}
