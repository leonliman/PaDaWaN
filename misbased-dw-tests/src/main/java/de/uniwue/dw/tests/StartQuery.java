package de.uniwue.dw.tests;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.query.model.QueryReader;
import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.result.Result;
import de.uniwue.dw.query.sql.util.SQLGUIClient;
import de.uniwue.misc.util.ConfigException;
import de.uniwue.misc.util.FileUtilsUniWue;

import javax.security.auth.login.AccountException;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

public class StartQuery {

  /*
   * Runs a query with the given runConfiguration and the given query xml file.
   */
  public static void main(String[] args) throws IOException, ConfigException, SQLException,
          GUIClientException, AccountException {
    File configFile = new File(args[0]);
    String mxqlQuery = FileUtilsUniWue.file2String(new File(args[1]));
    DwClientConfiguration.loadProperties(configFile);
    QueryRoot queryRoot = QueryReader.read(mxqlQuery);
    SQLGUIClient guiClient = new SQLGUIClient();
    int queryID = guiClient.getQueryRunner().createQuery(queryRoot, AbstractTest.getTestUser());
    Result result = guiClient.getQueryRunner().runQueryBlocking(queryID);
    System.out.println(result);
  }

}
