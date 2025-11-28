package de.uniwue.dw.core.model.manager;

public class UnauthorizedException extends DataSourceException {

  /**
   * 
   */
  private static final long serialVersionUID = 8731196996709837621L;

  public UnauthorizedException() {
    super();
  }

  public UnauthorizedException(String message, Throwable cause) {
    super(message, cause);
  }

  public UnauthorizedException(String message) {
    super(message);
  }

  public UnauthorizedException(Throwable cause) {
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
