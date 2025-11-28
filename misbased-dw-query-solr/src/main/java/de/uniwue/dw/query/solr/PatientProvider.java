package de.uniwue.dw.query.solr;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import de.uniwue.dw.core.sql.IDwSqlSchemaConstant;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;

public class PatientProvider implements IDwSqlSchemaConstant {

  private static PatientProvider instance = null;

  private HashMap<Long, Long> caseID2PatientID = new HashMap<Long, Long>();

  private PatientProvider() throws SQLException {
    createCase2PatientHm();
  }

  private void createCase2PatientHm() throws SQLException {
    System.out.print("Creating Case2PatientHm .. ");
    long start = System.currentTimeMillis();
    Statement stmt = SQLPropertiesConfiguration.getInstance().getSQLManager().createStatement();
    String sql = "Select [caseid], [pid] from [" + T_IMPORT_CASES + "]";
    ResultSet rs = stmt.executeQuery(sql);
    while (rs.next()) {
      long patientID = rs.getLong("pid");
      long caseID = rs.getLong("caseid");
      caseID2PatientID.put(caseID, patientID);
    }
    rs.close();
    stmt.close();
    long end = System.currentTimeMillis();
    System.out.println("Finished. Duration: " + (end - start) + " ms");
  }

  public static PatientProvider getInstance() throws SQLException {
    if (instance == null)
      instance = new PatientProvider();
    return instance;
  }

  public int getNumPatients(SolrDocumentList docs) {
    System.out.print("Computing number of patients .. ");
    long start = System.currentTimeMillis();
    Set<Long> patients = new HashSet<Long>();
    for (SolrDocument doc : docs) {
      Object idObject = doc.getFirstValue("id");
      if (idObject instanceof Long) {
        long caseID = (Long) idObject;
        Long patientID = caseID2PatientID.get(caseID);
        patients.add(patientID);
      }
    }

    long end = System.currentTimeMillis();
    System.out.println("Finished. Duration: " + (end - start) + " ms");
    return patients.size();
  }

}
