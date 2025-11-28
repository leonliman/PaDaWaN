package de.uniwue.dw.core.model.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class Case {

  private long caseID;

  private List<Document> documents=new ArrayList<>();;

  private List<InfoGroup> infoGroups=new ArrayList<>();;

  private List<Information> infos=new ArrayList<>();;

  public Case(long caseID) {
    this.caseID = caseID;
  }

  public long getCaseID() {
    return caseID;
  }

  public void setCaseID(long caseID) {
    this.caseID = caseID;
  }

  public List<Document> getDocuments() {
    return documents;
  }

  public void setDocuments(List<Document> documents) {
    if (documents != null)
      this.documents = documents;
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
    List<Information> containingInfos = documents.stream()
            .flatMap(n -> n.getAllContainingInfos().stream()).collect(Collectors.toList());
    List<Information> groupInfos = infoGroups.stream()
            .flatMap(n -> n.getAllContainingInfos().stream()).collect(Collectors.toList());
    containingInfos.addAll(groupInfos);
    containingInfos.addAll(infos);
    return containingInfos;
  }

  public List<InfoGroup> getAllContainingInfoGroups() {
    List<InfoGroup> containingGroups = documents.stream()
            .flatMap(n -> n.getAllContainingInfoGroups().stream()).collect(Collectors.toList());
    containingGroups.addAll(infoGroups);
    List<InfoGroup> infoGroups = infos.stream().map(n -> new InfoGroup(n))
            .collect(Collectors.toList());
    containingGroups.addAll(infoGroups);
    return containingGroups;
  }

}
