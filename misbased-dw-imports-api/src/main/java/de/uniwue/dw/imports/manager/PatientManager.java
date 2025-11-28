package de.uniwue.dw.imports.manager;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.PIDFilterException;
import de.uniwue.dw.imports.PIDImportFilter;
import de.uniwue.dw.imports.api.DWImportsConfig;
import de.uniwue.dw.imports.data.PatientInfo;
import de.uniwue.dw.imports.manager.adapter.IPatientImportAdapter;

public class PatientManager {

  private IPatientImportAdapter patientAdapter;

  private Map<Long, PatientInfo> patients = new HashMap<Long, PatientInfo>();

  public PatientManager(ImporterManager anImporterManager) throws SQLException {
    initializeAdapters();
  }


  public void initializeAdapters() throws SQLException {
    patientAdapter = DWImportsConfig.getInstance().getImportsAdapterFactory().createPatientImportAdapter(this);
  }


  public void commit() throws SQLException {
    patientAdapter.commit();
  }


  public void truncatePatientTables() throws SQLException {
    patientAdapter.truncateTable();
    clear();
  }


  public void clear() {
    patients.clear();
  }


  public void dispose() {
    clear();
    patientAdapter.dispose();
  }


  public void read() throws SQLException {
    ImportLogManager.info("Loading patient meta infos");
    patients.clear();
    patientAdapter.read();
  }


  public void deleteInfosOfStornoPIDs() throws SQLException {
    patientAdapter.deleteInfosOfStornoPIDs();
  }


  public PatientInfo getPatient(Long aPID) throws ImportException {
    if (!PIDImportFilter.getInstance().checkPID(aPID)) {
      throw new PIDFilterException();
    }
    PatientInfo result = null;
    result = patients.get(aPID);
    if ((result == null) && DWImportsConfig.getLoadMetaDataLazy()) {
      try {
        result = patientAdapter.getPatient(aPID);
      } catch (SQLException e) {
        throw new ImportException(ImportExceptionType.SQL_ERROR, e);
      }
    }
    if (result == null) {
      throw new ImportException(ImportExceptionType.NO_PID, "patient for PID '" + aPID + "' does not exist");
    }
    return result;
  }


  public PatientInfo createPatient(long pid, boolean storno, String sex, Integer yob) {
    PatientInfo anInfo = new PatientInfo(pid, storno, sex, yob);
    return anInfo;
  }


  public void addPatient(PatientInfo aPatient) {
    patients.put(aPatient.pid, aPatient);
    if (patients.size() % 100000 == 0) {
      System.out.print(".");
    }
  }


  public void insert(long pid, boolean storno, String sex, Integer yob, String importFile)
          throws SQLException, ImportException {
    patientAdapter.insert(pid, storno, sex, yob, importFile);
    if (!patients.containsKey(pid)) {
      PatientInfo anInfo = createPatient(pid, storno, sex, yob);
      addPatient(anInfo);
    } else {
      PatientInfo anInfo = getPatient(pid);
      anInfo.sex = sex;
      anInfo.yob = yob;
      anInfo.storno = storno;
    }
  }

}
