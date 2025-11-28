package de.uniwue.dw.core.model.data;

import de.uniwue.misc.util.TimeUtil;

import java.sql.Timestamp;
import java.util.Date;

/**
 * This class is a data model class for facts which are stored in the DWInfo-table
 */
public class Information {

  public static final long DOCID_DEFAULT_VALUE = 0;

  public static final long GROUPID_DEFAULT_VALUE = 0;

  public static final String COMPOUNDID_DEFAULT_VALUE = "" + GROUPID_DEFAULT_VALUE;

  public CatalogEntry modifiedEntry;

  // The row ID in the DWInfo table. This member should only be used internally
  private long infoID;

  // The catalogEntry ID this fact belongs to
  private int attrID;

  // The patient identifier for this fact
  private long pid;

  // The case's ID for this fact
  private long caseID;

  // When was this fact measured
  private Timestamp measureTime;

  // When was this fact imported into the database
  private Timestamp importTime;

  // When was this fact last modified in the database
  private Timestamp updateTime;

  // The full value of this fact
  private String value;

  // The truncated value (100 characters) of this fact
  private String valueShort;

  // The value of this fact converted into a number data type (only when the fact's catalogEntry if
  // of data type "number")
  private Double valueDec;

  // The reference value of this fact (e.g. a document ID). The semantics of this member depends on
  // the catalogEntrie's domain
  private long ref;

  private long docID = DOCID_DEFAULT_VALUE;

  private long groupID = GROUPID_DEFAULT_VALUE;

  private boolean storno = false;

  public Information() {
  }

  public Information(Information anInfoToCopy) {
    this(anInfoToCopy.infoID, anInfoToCopy.attrID, anInfoToCopy.pid, anInfoToCopy.caseID,
            anInfoToCopy.measureTime, anInfoToCopy.importTime, anInfoToCopy.updateTime, anInfoToCopy.value,
            anInfoToCopy.valueShort, anInfoToCopy.valueDec, anInfoToCopy.ref, anInfoToCopy.docID, anInfoToCopy.groupID);
  }

  public Information(long infoID, int attrID, long pid, long caseID, Timestamp measureTime,
          Timestamp importTime, Timestamp updateTime, String value, String valueShort, Double valueDec, long ref,
          long docID, long groupID) {
    this.setInfoID(infoID);
    this.setAttrID(attrID);
    this.setPid(pid);
    this.setCaseID(caseID);
    this.setMeasureTime(measureTime);
    this.setImportTime(importTime);
    this.setUpdateTime(updateTime);
    this.setValue(value);
    this.setValueShort(valueShort);
    this.setValueDec(valueDec);
    this.setRef(ref);
    this.setDocID(docID);
    this.setGroupID(groupID);
  }

  @Override
  public String toString() {
    return "Information [infoID=" + getInfoID() + ", attrID=" + getAttrID() + ", pid=" + getPid()
            + ", caseID=" + getCaseID() + ", valueShort=" + getValueShort() + ", valueDec="
            + getValueDec() + ", ref=" + getRef() + ", docID=" + getDocID() + ", groupID="
            + getGroupID() + ", measureTime=" + getMeasureTime() + ", importTime=" + getImportTime()
            + ", value=" + getValue() + "]";
  }

  public long getInfoID() {
    return infoID;
  }

  public void setInfoID(long infoID) {
    this.infoID = infoID;
  }

  public int getAttrID() {
    return attrID;
  }

  public void setAttrID(int attrID) {
    this.attrID = attrID;
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

  public Timestamp getMeasureTime() {
    return measureTime;
  }

  public void setMeasureTime(Timestamp measureTime) {
    this.measureTime = measureTime;
  }

  public Timestamp getImportTime() {
    return importTime;
  }

  public void setImportTime(Timestamp importTime) {
    this.importTime = importTime;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public String getValueShort() {
    return valueShort;
  }

  public void setValueShort(String valueShort) {
    this.valueShort = valueShort;
  }

  public Double getValueDec() {
    return valueDec;
  }

  public void setValueDec(Double valueDec) {
    this.valueDec = valueDec;
  }

  public Date getValueDate() {
    return TimeUtil.parseDate(value);
  }

  public long getRef() {
    return ref;
  }

  public void setRef(long ref) {
    this.ref = ref;
  }

  public Long getDocID() {
    return docID;
  }

  public void setDocID(long docID) {
    this.docID = docID;
  }

  public long getGroupID() {
    return groupID;
  }

  public void setGroupID(long groupID) {
    this.groupID = groupID;
  }

  public boolean isStorno() {
    return storno;
  }

  public void setStorno(boolean storno) {
    this.storno = storno;
  }

  public Timestamp getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(Timestamp updateTime) {
    this.updateTime = updateTime;
  }

  // this ID is used for the grouping of facts, e.g. for the creation of compound solr documents
  public String getCompoundID() {
    if (getGroupID() == GROUPID_DEFAULT_VALUE) {
      return COMPOUNDID_DEFAULT_VALUE;
    } else {
      return getPid() + "_" + getCaseID() + "_" + getDocID() + "_" + getGroupID() + "_" + getMeasureTime();
    }
  }

}
