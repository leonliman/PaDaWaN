package de.uniwue.dw.core.model.data;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Document {

  private long docID;

  private List<InfoGroup> infoGroups = new ArrayList<>();

  private List<Information> infos = new ArrayList<>();

  public Document(long docID) {
    this.docID = docID;
  }

  public long getDocID() {
    return docID;
  }

  public void setDocID(long docID) {
    this.docID = docID;
  }

  public List<InfoGroup> getInfoGroups() {
    return infoGroups;
  }

  public void setInfoGroups(List<InfoGroup> groups) {
    if (groups != null)
      this.infoGroups = groups;
  }

  public List<Information> getInfos() {
    return infos;
  }

  public void setInfos(List<Information> infos) {
    if (infos != null)
      this.infos = infos;
  }

  public List<Information> getAllContainingInfos() {
    List<Information> containingInfos = infoGroups.stream()
            .flatMap(n -> n.getAllContainingInfos().stream()).collect(Collectors.toList());
    containingInfos.addAll(infos);
    return containingInfos;
  }

  public List<InfoGroup> getAllContainingInfoGroups() {
    List<InfoGroup> containingGroups = infos.stream().map(n -> new InfoGroup(n))
            .collect(Collectors.toList());
    containingGroups.addAll(infoGroups);
    return containingGroups;
  }

}
