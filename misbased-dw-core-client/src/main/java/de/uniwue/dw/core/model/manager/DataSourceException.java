package de.uniwue.dw.core.model.manager;

public class DataSourceException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = 5348121437379547871L;

  public DataSourceException() {
  }

  public DataSourceException(String message, Throwable cause) {
    super(message, cause);
  }

  public DataSourceException(String message) {
    super(message);
  }

  public DataSourceException(Throwable cause) {
    super(cause);
  }

  public Throwable getRootCause() {
    Throwable t = this;
    while (true) {
      Throwable cause = t.getCause();
      if (cause != null) {
        t = cause;
      } else {
        break;
      }
    }
    return t;
  }

}
