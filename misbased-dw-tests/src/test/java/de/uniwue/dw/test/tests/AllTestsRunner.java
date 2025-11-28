package de.uniwue.dw.test.tests;

import de.uniwue.dw.test.tests.mssql.*;
import de.uniwue.dw.test.tests.mysql.*;

public class AllTestsRunner {

  public static void main(String[] args) throws Exception {
    testMSSQL();
    testMySQL();
  }

  public static void testMSSQL() throws Exception {
    MSSQLConnectionTest.main(null);
    MSSQLCreateTablesTest.main(null);
//    MSSQLImportConfiguredDBSourceTest.main(null);
    MSSQLImportConfiguredParallelImportTest.main(null);
    MSSQLImportConfiguredTest.main(null);
    MSSQLImportDumpTest.main(null);
    MSSQLImportPIDFilterTest.main(null);
    MSSQLImportSQLDumpTest.main(null);
    MSSQLImportSQLUpdatesTest.main(null);
    MSSQLSolrExportCSVTest.main(null);
    MSSQLSolrIndexingTest.main(null);
    MSSQLSolrQueryLogicTest.main(null);
    MSSQLSolrStornoTest.main(null);
  }

  public static void testMySQL() throws Exception {
    MySQLConnectionTest.main(null);
    MySQLCreateTablesTest.main(null);
    MySQLImportConfiguredTest.main(null);
    MySQLImportDumpTest.main(null);
    MySQLImportSQLDumpTest.main(null);
    MySQLImportSQLUpdatesTest.main(null);
    MySQLSolrExportCSVTest.main(null);
    MySQLSolrIndexingTest.main(null);
    MySQLSolrQueryLogicTest.main(null);
    MySQLSolrStornoTest.main(null);
  }

}
