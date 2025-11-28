package de.uniwue.dw.test.tests.mssql;

import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.dw.test.tests.IndexingTest;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.util.ConfigException;
import org.junit.BeforeClass;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

public class MSSQLSolrIndexingTest extends IndexingTest {

  private static DBType dbType = DBType.MSSQL;

  protected static QueryEngineType engineType = QueryEngineType.Solr;

  public MSSQLSolrIndexingTest() {
    super(dbType, engineType);
  }

  public static void main(String[] args) throws Exception {
    MSSQLSolrIndexingTest.initialize();
    MSSQLSolrIndexingTest test = new MSSQLSolrIndexingTest();
    doTests(test);
  }

  @BeforeClass
  public static void initialize() throws Exception {
    initialize(dbType, engineType, testPropertiesFile, ImportDumpMode.ClassicFormat, MSSQLSolrIndexingTest.class);
  }
}
