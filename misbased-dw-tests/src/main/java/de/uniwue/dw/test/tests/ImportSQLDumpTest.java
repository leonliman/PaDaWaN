package de.uniwue.dw.test.tests;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.tests.SystemTestParameters;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;
import de.uniwue.misc.util.FileUtilsUniWue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * This test imports a big SQL statement (instead of bulk insert dumps)
 */
public abstract class ImportSQLDumpTest extends ImportTest {

  protected static File testPropertiesFile = SystemTestParameters.getInstance()
          .getIMPORT_SQL_DUMP_TEST_CONFIG_FILE();

  public ImportSQLDumpTest(DBType aType, QueryEngineType engineType) throws Exception {
    super(aType, engineType);
  }

  public static void initialize(DBType dbType, QueryEngineType engineType, Class aClass)
          throws Exception {
    initialize(dbType, engineType, testPropertiesFile, ImportDumpMode.None, aClass);
    if (canPerformTestFlag) {
      importSQLDump(dbType);
      DwClientConfiguration.getInstance().getCatalogManager().initializeData();
      queryClient.getCatalogClientProvider().reinitialize();
    }
  }

  public static void importSQLDump(DBType aDBType) throws IOException, SQLException {
    // create the managers so the tables are definitely created
    DwClientConfiguration.getInstance().getCatalogManager();
    DwClientConfiguration.getInstance().getInfoManager();
    String dumpSQLString = getDumpString(aDBType);
    SQLManager sqlManager = SQLPropertiesConfiguration.getInstance().getSQLManager();
    Statement st = sqlManager.createStatement();
    st.execute(dumpSQLString);
    st.close();
  }

  protected static String getDumpString(DBType aDBType) throws IOException {
    URL resource;
    if (aDBType == DBType.MSSQL) {
      resource = ImportSQLDumpTest.class.getResource("/Dumps/DWExport_MSSQL.sql");
    } else if (aDBType == DBType.MySQL) {
      resource = ImportSQLDumpTest.class.getResource("/Dumps/DWExport_MySQL.sql");
    } else {
      throw new IOException("No dump available for engine " + aDBType.toString());
    }
    File queryFile = new File(resource.getFile()).getCanonicalFile();
    String absQueryFilename = queryFile.getCanonicalPath().replaceAll("%20", " ");
    File aFile = new File(absQueryFilename);
    return FileUtilsUniWue.file2String(aFile);
  }

}
