package de.uniwue.dw.query.model.data;

import java.sql.Timestamp;

public class IndexLogEntry {

  String serverID, message;

  long id, pid, caseID;

  Timestamp occTime;

  public IndexLogEntry(String serverID, String message, long id, long pid, long caseID, Timestamp occTime) {
    this.serverID = serverID;
    this.message = message;
    this.id = id;
    this.pid = pid;
    this.caseID = caseID;
    this.occTime = occTime;
  }

  public String getServerID() {
    return serverID;
  }

  public void setServerID(String serverID) {
    this.serverID = serverID;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getPid() {
    return pid;
  }

  public void setPid(long pid) {
    this.pid = pid;
  }

  public long getCaseID() {
    return caseID;
  }

  public void setCaseID(long caseID) {
    this.caseID = caseID;
  }

  public Timestamp getOccTime() {
    return occTime;
  }

  public void setOccTime(Timestamp occTime) {
    this.occTime = occTime;
  }
}
