package de.uniwue.dw.test.tests.mssql;

import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.dw.test.tests.StornoTest;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.util.ConfigException;
import org.junit.BeforeClass;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

public class MSSQLSolrStornoTest extends StornoTest {

  private static DBType dbType = DBType.MSSQL;

  private static QueryEngineType engineType = QueryEngineType.Solr;

  public MSSQLSolrStornoTest() {
    super(dbType, QueryEngineType.SQL);
  }

  public static void main(String[] args) throws Exception {
    MSSQLSolrStornoTest.initialize();
    MSSQLSolrStornoTest test = new MSSQLSolrStornoTest();
    doTests(test);
  }

  @BeforeClass
  public static void initialize() throws Exception {
    initialize(dbType, engineType, MSSQLSolrStornoTest.class);
  }

}
