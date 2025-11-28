package de.uniwue.dw.query.model;

import de.uniwue.dw.query.model.client.GUIClientException;

public interface IConnectionExpiredListener {

  void connectionExpired() throws GUIClientException;
  
}
