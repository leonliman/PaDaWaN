package de.uniwue.dw.query.solr.model.manager;

import de.uniwue.dw.query.solr.client.SolrManager;
import de.uniwue.dw.solr.api.DWSolrConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Properties;

public class EmbeddedSolrManager extends SolrManager {

  private static Logger logger = LogManager.getLogger(EmbeddedSolrManager.class);

  private CoreContainer container;

  private EmbeddedSolrServer server;

  public EmbeddedSolrManager(String user, String password, String serverURL) {
    super(user, password, serverURL);
  }

  public EmbeddedSolrManager() {
    this(null, null, null);
  }

  @Override
  protected void initServer() {
    try {
      String solrHomePath;
      if (DWSolrConfig.getEmbeddedSolrHomeDir() != null) {
        solrHomePath = DWSolrConfig.getEmbeddedSolrHomeDir();
      } else {
        String solrDir = "solrEmbedded";
        URL resource = getClass().getClassLoader().getResource(solrDir);
        File file = Paths.get(resource.toURI()).toFile();
        solrHomePath = file.getAbsolutePath();
      }
      logger.debug("using solr home dir: " + solrHomePath);
      System.setProperty("solr.install.dir",
              solrHomePath); // Should be no longer necessary in the next update after Solr 9.1.0
      container = new CoreContainer(Paths.get(solrHomePath), new Properties());
      container.load();
      server = new EmbeddedSolrServer(container, "v2");
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
  }

  @Override
  public SolrClient getServer() {
    if (server == null)
      initServer();
    return server;
  }

  @Override
  public void shutdown() {
    if (server != null) {
      try {
        server.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      server = null;
      container.shutdown();
      container = null;
    }
  }
}
