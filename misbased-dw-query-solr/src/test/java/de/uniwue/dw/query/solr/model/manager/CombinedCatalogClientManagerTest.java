package de.uniwue.dw.query.solr.model.manager;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;

import org.junit.AfterClass;

import de.uniwue.dw.core.model.manager.CatalogClientManagerTest;
import de.uniwue.dw.core.model.manager.DataSourceException;
import de.uniwue.dw.query.model.SolrEnvironmentDataLoader;
import de.uniwue.dw.query.model.TestEnvironmentDataLoader;
import de.uniwue.dw.query.model.client.GUIClientException;

public class CombinedCatalogClientManagerTest extends CatalogClientManagerTest {

  private static SolrEnvironmentDataLoader dataLoader;

  @Override
  public TestEnvironmentDataLoader getTestEnvironmentDataLoader() throws DataSourceException {
    try {
      if (dataLoader == null) {
        dataLoader = new SolrEnvironmentDataLoader();
      }
      return dataLoader;
    } catch (GUIClientException | SQLException | IOException | URISyntaxException e) {
      e.printStackTrace();
      throw new DataSourceException(e);
    }
  }

  @AfterClass
  public static void disposeSolrEnvironmentDataLoader() throws GUIClientException {
    dataLoader.dispose();
    dataLoader = null;
  }

}
