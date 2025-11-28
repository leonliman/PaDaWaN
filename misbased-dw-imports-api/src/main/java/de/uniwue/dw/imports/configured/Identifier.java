package de.uniwue.dw.imports.configured;

import java.sql.Timestamp;

import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.Importer;
import de.uniwue.dw.imports.configured.data.ConfigData;
import de.uniwue.dw.imports.data.CaseInfo;
import de.uniwue.dw.imports.data.DocInfo;
import de.uniwue.dw.imports.data.PatientInfo;

public class Identifier {

  public PatientInfo patientInfo;

  public long pid;

  public CaseInfo caseInfo;

  public long caseID;

  public DocInfo docInfo;

  public long docID;

  public long groupID;

  public long refID;

  public Timestamp measureTime;

  public Identifier(long aPid, long aCaseId, long aDocID, long aGroupID, long aRefID, Importer importer, ConfigData configData)
          throws ImportException {
    pid = aPid;
    caseID = aCaseId;
    docID = aDocID;
    groupID = aGroupID;
    refID = aRefID;
    if (configData.useMetaData) {
      update(importer);
    }
  }

  public void update(Importer importer) throws ImportException {
    if ((docInfo == null) && (docID != 0)) {
      docInfo = importer.getDocInfo(docID);
      pid = docInfo.PID;
      if (docInfo.caseID != 0) {
        caseID = docInfo.caseID;
      }
      measureTime = docInfo.creationTime;
    }
    if ((caseInfo == null) && (caseID != 0)) {
      caseInfo = importer.getCaseInfo(caseID);
      pid = caseInfo.pid;
      if (measureTime == null) {
        measureTime = caseInfo.measureTime;
      }
    }
    if ((patientInfo == null) && (pid != 0)) {
      patientInfo = importer.getPatientInfo(pid);
    }
  }

  public boolean isStorno(Importer importer) throws ImportException {
    if ((patientInfo != null) && (patientInfo.storno)) {
      return true;
    }
    if ((caseInfo != null) && (caseInfo.storno)) {
      return true;
    }
    if ((docInfo != null) && (docInfo.storno)) {
      return true;
    }
    return false;
  }

}
