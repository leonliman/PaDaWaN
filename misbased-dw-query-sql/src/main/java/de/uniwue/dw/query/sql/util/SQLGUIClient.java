package de.uniwue.dw.query.sql.util;

import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.client.GUIQueryClient;
import de.uniwue.dw.query.model.client.IGUIClient;
import de.uniwue.dw.query.model.client.IQueryRunner;

public class SQLGUIClient extends GUIQueryClient implements IGUIClient {

  public SQLGUIClient() throws GUIClientException {
    super();
  }

  @Override
  public IQueryRunner createQueryRunner() {
    return new SQLQueryRunner(this);
  }

}
