package de.uniwue.dw.core.model.data;

import java.util.ArrayList;
import java.util.List;

public class InfoGroup {

  private String compoundID;

  private List<Information> infos=new ArrayList<>();;

  public InfoGroup(String compoundID) {
    this.compoundID = compoundID;
    this.infos = new ArrayList<>();
  }

  public InfoGroup(Information info) {
    this.compoundID = info.getCompoundID();
    add(info);
  }

  public InfoGroup(String compoundID, List<Information> infos) {
    this.compoundID = compoundID;
    this.infos = infos;
  }

  private void add(Information info) {
    infos.add(info);
  }

  public String getCompoundID() {
    return compoundID;
  }

  public void setGroupID(String compoundID) {
    this.compoundID = compoundID;
  }

  public List<Information> getInfos() {
    return infos;
  }

  public void setInfos(List<Information> infos) {
    if (infos != null)
      this.infos = infos;
  }

  public List<Information> getAllContainingInfos() {
    return infos;
  }

}
