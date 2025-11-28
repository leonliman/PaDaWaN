package de.uniwue.dw.query.solr;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.DWAuthenticator;
import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.query.model.client.IGUIClient;
import de.uniwue.dw.query.model.lang.ContentOperator;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.result.Result;

import java.io.File;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class TestSetup {

  public static void main(String[] args) throws Exception {
    File propertiesFile = new File(URLDecoder.decode(TestSetup.class.getResource("/TestSetup.properties").getFile(),
            StandardCharsets.UTF_8));
    DwClientConfiguration.loadProperties(propertiesFile);

    CatalogEntry catalogEntry = DwClientConfiguration.getInstance().getCatalogManager()
            .getEntryByRefID("alter", "alter");
    User user = DWAuthenticator.getUser("demo", "demouser");

    QueryRoot queryRoot = new QueryRoot();
    QueryAttribute queryAttribute = new QueryAttribute(queryRoot, catalogEntry);
    queryAttribute.setContentOperator(ContentOperator.MORE);
    queryAttribute.setDesiredContent(50);
    System.out.println(queryRoot.toXML());

    IGUIClient guiClient = new SolrGUIClient();
    int queryID = guiClient.getQueryRunner().createQuery(queryRoot, user);
    Result result = guiClient.getQueryRunner().runQueryBlocking(queryID);
    System.out.println(result.getDocsFound());
    guiClient.dispose();
  }
}
