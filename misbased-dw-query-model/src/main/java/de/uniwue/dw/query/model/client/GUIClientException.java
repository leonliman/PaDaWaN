package de.uniwue.dw.query.model.client;

public class GUIClientException extends Exception {

  private static final long serialVersionUID = 4657956911363182251L;

  public GUIClientException(String message) {
    super(message);
  }
  
  
  public GUIClientException(Exception innerException) {
    super(innerException);
  }
  
}
