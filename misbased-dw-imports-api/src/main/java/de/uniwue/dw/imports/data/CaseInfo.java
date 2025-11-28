package de.uniwue.dw.imports.data;

import java.sql.Timestamp;

public class CaseInfo {

  public long caseID, pid;

  public Timestamp admission;

  public Timestamp discharge;

  public Timestamp firstMovement;

  public Timestamp lastMovement;

  public boolean storno;

  public Timestamp measureTime;

  public String fallart = null;

  public String importFile;

  public Timestamp importTime;

  public CaseInfo(long aPid, long aFallID, Timestamp anAdmissionTime, Timestamp anDischargeTime,
          Timestamp aFirstMovementTime, Timestamp aLastMovementTime, Timestamp aMeasureTime,
          boolean aStornoFlag, String casetype, String importFile, Timestamp importTime) {
    pid = aPid;
    caseID = aFallID;
    admission = anAdmissionTime;
    discharge = anDischargeTime;
    firstMovement = aFirstMovementTime;
    lastMovement = aLastMovementTime;
    measureTime = aMeasureTime;
    storno = aStornoFlag;
    fallart = casetype;
    this.importFile = importFile;
    this.importTime = importTime;
  }

}
