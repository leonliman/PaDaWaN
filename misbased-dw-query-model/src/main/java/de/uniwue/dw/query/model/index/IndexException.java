package de.uniwue.dw.query.model.index;

public class IndexException extends Exception {

  private static final long serialVersionUID = 1879533038978820807L;

  public long pid, caseID;

  public IndexException(String aMessage) {
    super(aMessage);
  }

  public IndexException(String aMessage, long aPID, long aCaseID) {
    super(aMessage);
    pid = aPID;
    caseID = aCaseID;
  }

  public IndexException(Exception e, long aPID, long aCaseID) {
    super(e);
    pid = aPID;
    caseID = aCaseID;
  }

  public IndexException(Exception e) {
    super(e);
  }

  public IndexException(String aMessage, Exception e) {
    super(aMessage, e);
  }

}
