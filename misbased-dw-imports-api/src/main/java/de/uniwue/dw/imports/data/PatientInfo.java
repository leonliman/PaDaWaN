package de.uniwue.dw.imports.data;

public class PatientInfo {

  public Long pid;

  public boolean storno = false;
  
  public String sex;
  
  public int yob;

  public PatientInfo(Long aPID, boolean aStornoFlag, String aSex, int aYob) {
    pid = aPID;
    storno = aStornoFlag;
    sex = aSex;
    yob = aYob;
  }

}
