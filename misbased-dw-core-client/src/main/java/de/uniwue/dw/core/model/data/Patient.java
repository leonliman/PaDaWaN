package de.uniwue.dw.core.model.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Patient {

  private long pid;

  private List<Case> cases;

  public Patient() {
    cases = new ArrayList<>();
  }

  public Patient(List<Case> cases) {
    this.cases = cases;
  }

  public Patient(long pid, List<Case> cases) {
    this.pid = pid;
    this.cases = cases;
  }

  public List<Case> getCases() {
    return cases;
  }

  public void setCases(List<Case> cases) {
    if (cases != null)
      this.cases = cases;
  }

  public long getPid() {
    return pid;
  }

  public void setPid(long pid) {
    this.pid = pid;
  }

  public List<Document> getAllContainingDocuments() {
    return cases.stream().flatMap(c -> c.getDocuments().stream()).collect(Collectors.toList());
  }

  public List<List<Information>> getAllInfosWithoutDocumentID() {
    ArrayList<List<Information>> infosWithoutDocumentID = new ArrayList<>();
    for (Case c : cases) {
      if (!c.getInfos().isEmpty()) {
        infosWithoutDocumentID.add(new ArrayList<>(c.getInfos()));
      }
      if (!c.getInfoGroups().isEmpty()) {
        for (InfoGroup infoGroup : c.getInfoGroups()) {
          if (Objects.equals(infoGroup.getCompoundID(), Information.COMPOUNDID_DEFAULT_VALUE))
            infosWithoutDocumentID.add(new ArrayList<>(infoGroup.getInfos()));
          else
            infosWithoutDocumentID.add(Collections.emptyList());
        }
      }
    }
    return infosWithoutDocumentID;
  }

  public List<List<InfoGroup>> getAllInfoGroupsWithoutDocumentID() {
    ArrayList<List<InfoGroup>> infoGroupsWithoutDocumentID = new ArrayList<>();
    for (Case c : cases) {
      if (!c.getInfos().isEmpty()) {
        infoGroupsWithoutDocumentID.add(Collections.emptyList());
      }
      if (!c.getInfoGroups().isEmpty()) {
        for (InfoGroup infoGroup : c.getInfoGroups()) {
          if (Objects.equals(infoGroup.getCompoundID(), Information.COMPOUNDID_DEFAULT_VALUE))
            infoGroupsWithoutDocumentID.add(Collections.emptyList());
          else
            infoGroupsWithoutDocumentID.add(Collections.singletonList(infoGroup));
        }
      }
    }
    return infoGroupsWithoutDocumentID;
  }

  public List<Information> getAllContainingInfos() {
    return cases.stream().flatMap(n -> n.getAllContainingInfos().stream())
            .collect(Collectors.toList());
  }

  public List<InfoGroup> getAllContainingInfoGroups() {
    return cases.stream().flatMap(n -> n.getAllContainingInfoGroups().stream())
            .collect(Collectors.toList());
  }

  public String toString() {
    return pid + "";
  }

}
