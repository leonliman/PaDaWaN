package de.uniwue.dw.query.model.data;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class LoggedQuery {

  private int logID;

  private String xml;

  private String machineName;

  private String userName;

  private String queryTime;

  public int getLogID() {
    return logID;
  }

  public LoggedQuery setLogID(int logID) {
    this.logID = logID;
    return this;
  }

  @JsonIgnore
  public String getXml() {
    return xml;
  }

  public LoggedQuery setXml(String xml) {
    this.xml = xml;
    return this;
  }

  @JsonIgnore
  public String getMachineName() {
    return machineName;
  }

  public LoggedQuery setMachineName(String machineName) {
    this.machineName = machineName;
    return this;
  }

  public String getUserName() {
    return userName;
  }

  public LoggedQuery setUserName(String userName) {
    this.userName = userName;
    return this;
  }

  public String getQueryTime() {
    return queryTime;
  }

  public LoggedQuery setQueryTime(String queryTime) {
    this.queryTime = queryTime;
    return this;
  }

}
