package de.uniwue.dw.core.model.data;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PatientStructureBuilder {

  public static List<Patient> buildPatients(List<Information> infos) {
    Map<Long, List<Information>> pid2infos = infos.stream()
            .collect(Collectors.groupingBy(Information::getPid, Collectors.toList()));
    List<Patient> patients = pid2infos.entrySet().stream().map(n -> buildPatient(n.getKey(), n.getValue()))
            .collect(Collectors.toList());
    return patients;
  }

  public static Patient buildPatient(long pid, List<Information> infos) {
    List<Case> cases = buildCases(infos);
    Patient patient = new Patient(pid, cases);
    return patient;
  }

  public static List<Case> buildCases(List<Information> infos) {
    Map<Long, List<Information>> caseID2infos = infos.stream()
            .collect(Collectors.groupingBy(Information::getCaseID, Collectors.toList()));

    List<Case> cases = caseID2infos.entrySet().stream().map(n -> buildCase(n.getKey(), n.getValue()))
            .collect(Collectors.toList());
    return cases;
  }

  public static Case buildCase(long caseID, List<Information> infos) {
    Case aCase = new Case(caseID);

    Map<Long, List<Information>> docID2infos = infos.stream()
            .collect(Collectors.groupingBy(Information::getDocID, Collectors.toList()));
    List<Information> infosWithoutDocID = docID2infos.get(Information.DOCID_DEFAULT_VALUE);
    if (infosWithoutDocID != null) {
      docID2infos.remove(Information.DOCID_DEFAULT_VALUE);
      Map<String, List<Information>> groupID2infos = infosWithoutDocID.stream()
              .collect(Collectors.groupingBy(Information::getCompoundID, Collectors.toList()));
      List<Information> infosWithNoGroupID = groupID2infos.get(Information.COMPOUNDID_DEFAULT_VALUE);
      if (infosWithNoGroupID != null) {
        groupID2infos.remove(Information.COMPOUNDID_DEFAULT_VALUE);
        aCase.setInfos(infosWithNoGroupID);
      }
      List<InfoGroup> groups = buildGroups(groupID2infos);
      aCase.setInfoGroups(groups);
    }
    List<Document> docs = buildDocuments(docID2infos);
    aCase.setDocuments(docs);

    return aCase;
  }

  private static List<Document> buildDocuments(Map<Long, List<Information>> docID2infos) {
    List<Document> docs = docID2infos.entrySet().stream().map(n -> createDocument(n.getKey(), n.getValue()))
            .collect(Collectors.toList());
    return docs;

  }

  public static Document createDocument(long docID, List<Information> infos) {
    Document document = new Document(docID);
    Map<String, List<Information>> groupID2infos = infos.stream()
            .collect(Collectors.groupingBy(Information::getCompoundID, Collectors.toList()));
    List<Information> infosWithNoGroupID = groupID2infos.get(Information.COMPOUNDID_DEFAULT_VALUE);
    if (infosWithNoGroupID != null) {
      groupID2infos.remove(Information.COMPOUNDID_DEFAULT_VALUE);
      document.setInfos(infosWithNoGroupID);
    }
    List<InfoGroup> groups = buildGroups(groupID2infos);
    document.setInfoGroups(groups);

    return document;
  }

  private static List<InfoGroup> buildGroups(Map<String, List<Information>> groupID2infos) {
    List<InfoGroup> groups = groupID2infos.entrySet().stream().map(n -> createGroup(n.getKey(), n.getValue()))
            .collect(Collectors.toList());
    return groups;
  }

  public static InfoGroup createGroup(String compoundID, List<Information> infos) {
    InfoGroup group = new InfoGroup(compoundID);
    group.setInfos(infos);
    return group;
  }

}
