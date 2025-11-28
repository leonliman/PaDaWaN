package de.uniwue.dw.test.tests.mysql;

import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.test.tests.StornoTest;
import de.uniwue.misc.sql.DBType;
import org.junit.BeforeClass;

public class MySQLSolrStornoTest extends StornoTest {

  private static final DBType dbType = DBType.MySQL;

  private static final QueryEngineType engineType = QueryEngineType.Solr;

  public MySQLSolrStornoTest() {
    super(dbType, QueryEngineType.SQL);
  }

  public static void main(String[] args) throws Exception {
    MySQLSolrStornoTest.initialize();
    MySQLSolrStornoTest test = new MySQLSolrStornoTest();
    doTests(test);
  }

  @BeforeClass
  public static void initialize() throws Exception {
    initialize(dbType, engineType, MySQLSolrStornoTest.class);
  }

}
