package de.uniwue.dw.query.model;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.client.IGUIClient;
import de.uniwue.dw.query.model.data.QueryEngineType;

public class GUIClientFactory {

  protected static final Logger logger = LogManager.getLogger(GUIClientFactory.class);

  private static GUIClientFactory instance = null;

  public static GUIClientFactory getInstance() {
    if (instance == null)
      instance = new GUIClientFactory();
    return instance;
  }

  private GUIClientFactory() {
    super();
  }

  public IGUIClient getGUIClient() throws SQLException, GUIClientException {
    return getGUIClient(null);
  }

  public IGUIClient getGUIClient(QueryEngineType type) throws SQLException, GUIClientException {
    QueryEngineType engineType = null;

    if (type != null) {
      engineType = type;
    } else {
      String guiClientName = DwClientConfiguration.getInstance()
              .getParameter(IQueryKeys.PARAM_GUI_CLIENT_TYPE, QueryEngineType.Solr.toString());

      try {
        engineType = QueryEngineType.valueOf(guiClientName);
      } catch (IllegalArgumentException e) {
        logger.debug("Wrong ClientType in Settingsfile");
        throw new GUIClientException("Wrong Client Type!");
      }
    }
    IGUIClient guiClient = null;

    switch (engineType) {
      case Solr:
        guiClient = getGUIClientFactory("de.uniwue.dw.query.solr.SolrGUIClient");
        break;
      case SQL:
        guiClient = getGUIClientFactory("de.uniwue.dw.query.sql.util.SQLGUIClient");
        break;
      case I2B2:
        guiClient = getGUIClientFactory("de.uniwue.dw.query.i2b2.search.I2B2GUIClient");
        break;
      case Neo4J:
        guiClient = getGUIClientFactory("de.uniwue.dw.query.neo4j.N4JGUIClient");
        break;
      default:
        logger.debug("No Client Type in Settingsfile. Use Default SolrGUIClient.");
        guiClient = getGUIClientFactory("de.uniwue.dw.query.solr.SolrGUIClient");
        break;
    }

    return guiClient;
  }

  private IGUIClient getGUIClientFactory(String className) throws SQLException {
    IGUIClient guiClient;
    try {
      guiClient = (IGUIClient) getClass().getClassLoader().loadClass(className).getConstructor()
              .newInstance();
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
            | InvocationTargetException | NoSuchMethodException | SecurityException
            | ClassNotFoundException e) {
      throw new SQLException(e);
    }
    return guiClient;
  }
}
