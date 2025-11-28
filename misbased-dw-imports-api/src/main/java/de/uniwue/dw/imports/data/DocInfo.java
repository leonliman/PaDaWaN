package de.uniwue.dw.imports.data;

import java.sql.Timestamp;

public class DocInfo {

  public long caseID;

  public long PID;

  public long docID;

  public boolean storno;

  public Timestamp creationTime;

  public DocInfo(long aDocID, long aPID, long aCaseID, Timestamp aCreationTime, boolean aStornoFlag) {
    docID = aDocID;
    PID = aPID;
    caseID = aCaseID;
    storno = aStornoFlag;
    creationTime = aCreationTime;
  }

  @Override
  public String toString() {
    return "" + docID + "|" + PID + "|" + caseID + "|" + storno + "|" + creationTime + "|";
  }

}
