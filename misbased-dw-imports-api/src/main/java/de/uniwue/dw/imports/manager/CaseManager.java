package de.uniwue.dw.imports.manager;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.PIDFilterException;
import de.uniwue.dw.imports.PIDImportFilter;
import de.uniwue.dw.imports.api.DWImportsConfig;
import de.uniwue.dw.imports.data.CaseInfo;
import de.uniwue.dw.imports.data.PatientInfo;
import de.uniwue.dw.imports.manager.adapter.ICaseImportAdapter;
import de.uniwue.misc.sql.SQLInsertMode;

public class CaseManager {

  public HashMap<Long, CaseInfo> case2Info = new HashMap<Long, CaseInfo>(); // fallID -> CaseInfo

  // PID -> CaseInfo
  public HashMap<Long, HashSet<CaseInfo>> pid2Infos = new HashMap<Long, HashSet<CaseInfo>>();

  public ICaseImportAdapter caseAdapter;

  private ImporterManager importerManager;

  private HashSet<CaseInfo> updatedCases = new HashSet<CaseInfo>();

  public CaseManager(ImporterManager anImporterManager) throws SQLException {
    importerManager = anImporterManager;
    initializeAdapters();
  }


  public void initializeAdapters() throws SQLException {
    caseAdapter = DWImportsConfig.getInstance().getImportsAdapterFactory().createCaseImportAdapter(this);
  }


  public void commit() throws SQLException {
    caseAdapter.commit();
    updateUpdatedCases();
  }


  private void updateUpdatedCases() throws SQLException {
    SQLInsertMode insertMode = caseAdapter.getInsertMode();
    caseAdapter.setInsertMode(SQLInsertMode.bulkInsertTmpTableMerge);
    for (CaseInfo aCaseInfo : updatedCases) {
      caseAdapter.insert(aCaseInfo.pid, aCaseInfo.caseID, aCaseInfo.storno, aCaseInfo.admission, aCaseInfo.discharge,
              aCaseInfo.firstMovement, aCaseInfo.lastMovement, aCaseInfo.importFile, aCaseInfo.importTime,
              aCaseInfo.fallart);
    }
    caseAdapter.commit();
    updatedCases.clear();
    caseAdapter.setInsertMode(insertMode);
  }


  public void truncateCaseTables() throws SQLException {
    caseAdapter.truncateTable();
    clear();
  }


  public void clear() {
    case2Info.clear();
    pid2Infos.clear();
  }


  public void dispose() {
    clear();
    caseAdapter.dispose();
  }


  public void deleteInfosOfStornoCases() throws SQLException {
    caseAdapter.deleteInfosOfStornoCases();
  }


  public HashSet<CaseInfo> getCasesForPID(long aPID) throws ImportException, SQLException {
    HashSet<CaseInfo> result = null;

    result = pid2Infos.get(aPID);
    if ((result == null) && DWImportsConfig.getLoadMetaDataLazy()) {
      result = caseAdapter.readCasesForPID(aPID);
    }
    if (result == null) {
      // no exception here as there are plenty of PIDs without cases
      result = new HashSet<CaseInfo>();
    }
    return result;
  }


  public Long searchCaseIdForDate(Long PID, Timestamp applicationDateTime, HashSet<CaseInfo> caseInfos,
          boolean inPatientsOnly, boolean alsoCheckForClosestBoundaries) throws ImportException {
    return searchCaseIdForDate(PID, applicationDateTime, caseInfos, inPatientsOnly, alsoCheckForClosestBoundaries,
            3 * 30);
  }


  public Long searchCaseIdForDate(Long PID, Timestamp applicationDateTime, HashSet<CaseInfo> caseInfos,
          boolean inPatientsOnly, boolean alsoCheckForClosestBoundaries, long boundaryInDays) throws ImportException {

    CaseInfo inPatClosestCase = null;
    Long inPatClosestDistA = Long.MAX_VALUE;
    CaseInfo outPatClosestCase = null;
    Long outPatClosestDistA = Long.MAX_VALUE;

    CaseInfo inPatClosestCaseBeforeD = null;
    Long inPatClosestDistBeforeD = Long.MAX_VALUE;
    CaseInfo inPatClosestCaseAfterD = null;
    Long inPatClosestDistAfterD = Long.MAX_VALUE;
    CaseInfo inPatClosestCaseBeforeA = null;
    Long inPatClosestDistBeforeA = Long.MAX_VALUE;
    CaseInfo inPatClosestCaseAfterA = null;
    Long inPatClosestDistAfterA = Long.MAX_VALUE;

    CaseInfo outPatClosestCaseBeforeD = null;
    Long outPatClosestDistBeforeD = Long.MAX_VALUE;
    CaseInfo outPatClosestCaseAfterD = null;
    Long outPatClosestDistAfterD = Long.MAX_VALUE;
    CaseInfo outPatClosestCaseBeforeA = null;
    Long outPatClosestDistBeforeA = Long.MAX_VALUE;
    CaseInfo outPatClosestCaseAfterA = null;
    Long outPatClosestDistAfterA = Long.MAX_VALUE;

    Long aDayInMs = 24L * 60L * 60L * 1000L;

    Long boundaryInMS = aDayInMs * boundaryInDays;
    boolean restrictBoundary = boundaryInDays >= 0;

    for (CaseInfo caseInfo : caseInfos) {

      if (caseInfo.storno) {
        continue;
      }

      Timestamp inPatfrom = caseInfo.admission != null ? DataUtil.setMinTimePerDay(caseInfo.admission) : null;
      Timestamp inPatUntil = caseInfo.discharge != null ? DataUtil.setMinTimePerDay(caseInfo.discharge) : null;
      Timestamp outPatfrom = caseInfo.firstMovement != null ? DataUtil.setMinTimePerDay(caseInfo.firstMovement) : null;
      Timestamp outPatUntil = caseInfo.lastMovement != null ? DataUtil.setMinTimePerDay(caseInfo.lastMovement) : null;

      // check if searched time is within a inpatient or outpatient case (a day +/- after/before the
      // case is ok)
      if (inPatfrom != null && inPatUntil != null) {
        Long distA = applicationDateTime.getTime() - (inPatfrom.getTime() - aDayInMs);
        Long distD = applicationDateTime.getTime() - (inPatUntil.getTime() + aDayInMs);

        if (distA >= 0 && distD <= 0 && Math.abs(distA) < Math.abs(inPatClosestDistA)) {
          inPatClosestCase = caseInfo;
          inPatClosestDistA = distA;
        }

      }

      if (outPatfrom != null && outPatUntil != null) {
        Long distA = applicationDateTime.getTime() - (outPatfrom.getTime() - aDayInMs);
        Long distD = applicationDateTime.getTime() - (outPatUntil.getTime() + aDayInMs);

        if (distA >= 0 && distD <= 0 && Math.abs(distA) < Math.abs(outPatClosestDistA)) {
          outPatClosestCase = caseInfo;
          outPatClosestDistA = distA;
        }
      }

      if (alsoCheckForClosestBoundaries) {

        // check for closest time after an admission
        if (inPatfrom != null) {
          Long distA = applicationDateTime.getTime() - inPatfrom.getTime();
          if (!restrictBoundary || (restrictBoundary && Math.abs(distA) <= boundaryInMS)) {

            if (distA >= 0 && Math.abs(distA) < Math.abs(inPatClosestDistAfterA)) {
              inPatClosestCaseAfterA = caseInfo;
              inPatClosestDistAfterA = distA;
            }
            if (distA < 0 && Math.abs(distA) < Math.abs(inPatClosestDistBeforeA)) {
              inPatClosestDistBeforeA = distA;
              inPatClosestCaseBeforeA = caseInfo;
            }
          }

        }

        if (outPatfrom != null) {
          Long distA = applicationDateTime.getTime() - outPatfrom.getTime();
          if (!restrictBoundary || (restrictBoundary && Math.abs(distA) <= boundaryInMS)) {
            if (distA >= 0 && Math.abs(distA) < Math.abs(outPatClosestDistAfterA)) {
              outPatClosestCaseAfterA = caseInfo;
              outPatClosestDistAfterA = distA;
            }
            if (distA < 0 && Math.abs(distA) < Math.abs(outPatClosestDistBeforeA)) {
              outPatClosestDistBeforeA = distA;
              outPatClosestCaseBeforeA = caseInfo;
            }
          }
        }

        // check for closest time after discharge
        if (inPatUntil != null) {
          Long distD = applicationDateTime.getTime() - inPatUntil.getTime();
          if (!restrictBoundary || (restrictBoundary && Math.abs(distD) <= boundaryInMS)) {
            if (distD > 0 && Math.abs(distD) < Math.abs(inPatClosestDistAfterD)) {
              inPatClosestCaseAfterD = caseInfo;
              inPatClosestDistAfterD = distD;
            }
            if (distD <= 0 && Math.abs(distD) < Math.abs(inPatClosestDistBeforeD)) {
              inPatClosestDistBeforeD = distD;
              inPatClosestCaseBeforeD = caseInfo;
            }
          }

        }

        if (outPatUntil != null) {
          Long distD = applicationDateTime.getTime() - outPatUntil.getTime();
          if (!restrictBoundary || (restrictBoundary && Math.abs(distD) <= boundaryInMS)) {

            if (distD > 0 && Math.abs(distD) < Math.abs(outPatClosestDistAfterD)) {
              outPatClosestCaseAfterD = caseInfo;
              outPatClosestDistAfterD = distD;
            }
            if (distD <= 0 && Math.abs(distD) < Math.abs(outPatClosestDistBeforeD)) {
              outPatClosestDistBeforeD = distD;
              outPatClosestCaseBeforeD = caseInfo;
            }
          }
        }
      }
    }

    // return direct inpatient or outpatient result right away
    if (inPatClosestCase != null) {
      return inPatClosestCase.caseID;
    }
    if (!inPatientsOnly && outPatClosestCase != null) {
      return outPatClosestCase.caseID;
    }

    if (alsoCheckForClosestBoundaries) {

      // closest after inpatient stay
      if (inPatClosestCaseAfterD != null) {
        return inPatClosestCaseAfterD.caseID;
      }

      // closest after outpatient stay
      if (!inPatientsOnly && outPatClosestCaseAfterD != null) {
        return outPatClosestCaseAfterD.caseID;
      }

      // closest before inpatient stay
      if (inPatClosestCaseBeforeA != null) {
        return inPatClosestCaseBeforeA.caseID;
      }

      // closest before outpatient stay
      if (!inPatientsOnly && outPatClosestCaseBeforeA != null) {
        return outPatClosestCaseBeforeA.caseID;
      }

      // closest after inpatient admin
      if (inPatClosestCaseAfterA != null) {
        return inPatClosestCaseAfterA.caseID;
      }

      // 6. closest after outpatient admin
      if (!inPatientsOnly && outPatClosestCaseAfterA != null) {
        return outPatClosestCaseAfterA.caseID;
      }
      /*
       * // 1. closest after inpatient stay if (Math.abs(inPatClosestDistAfterD) <
       * Math.abs(inPatClosestDistBeforeA) && inPatClosestCaseAfterD != null) { return
       * inPatClosestCaseAfterD.caseID; }
       * 
       * // 2. closest after outpatient stay if (!inPatientsOnly &&
       * Math.abs(outPatClosestDistAfterD) < Math.abs(outPatClosestDistBeforeA) &&
       * outPatClosestCaseAfterD != null) { return outPatClosestCaseAfterD.caseID; }
       * 
       * // 3. closest before inpatient stay if (Math.abs(inPatClosestDistBeforeA) <
       * Math.abs(inPatClosestDistAfterD) && inPatClosestCaseBeforeA != null) { return
       * inPatClosestCaseBeforeA.caseID; }
       * 
       * // 4. closest before outpatient stay if (!inPatientsOnly &&
       * Math.abs(outPatClosestDistBeforeA) < Math.abs(outPatClosestDistAfterD) &&
       * outPatClosestCaseBeforeA != null) { return outPatClosestCaseBeforeA.caseID; }
       * 
       * // 5. closest after inpatient admin if (inPatClosestCaseAfterA != null) { return
       * inPatClosestCaseAfterA.caseID; }
       * 
       * // 6. closest after outpatient admin if (!inPatientsOnly && outPatClosestCaseAfterA !=
       * null) { return outPatClosestCaseAfterA.caseID; }
       */
    }

    throw new ImportException(ImportExceptionType.NO_CASE,
            " No caseID found for PID '" + PID + "' and measuredate  '" + applicationDateTime + "'");
  }


  public CaseInfo getCase(long aCaseID) throws ImportException {
    CaseInfo result = null;
    result = case2Info.get(aCaseID);
    if ((result == null) && DWImportsConfig.getLoadMetaDataLazy()) {
      try {
        result = caseAdapter.readCase(aCaseID);
      } catch (SQLException e) {
        throw new ImportException(ImportExceptionType.SQL_ERROR, e);
      }
    }
    if (result == null) {
      throw new ImportException(ImportExceptionType.NO_CASE, "found no case for caseID '" + aCaseID + "'");
    } else {
      if (!PIDImportFilter.getInstance().checkPID(result.pid)) {
        throw new PIDFilterException();
      }
    }
    return result;
  }


  public void saveCase(long pid, long aCaseID, boolean storno, String casetype, String importFile,
          Timestamp creationDate) throws ImportException, SQLException {
    caseAdapter.insert(pid, aCaseID, storno, null, null, null, null, importFile, creationDate, casetype);
    if (!case2Info.containsKey(aCaseID)) {
      CaseInfo anInfo = createCaseInfo(pid, aCaseID, null, null, null, null, creationDate, storno, casetype, importFile,
              creationDate);
      addCase(anInfo);
    } else {
      CaseInfo anInfo = case2Info.get(aCaseID);
      anInfo.storno = storno;
      anInfo.fallart = casetype;
      // die Timestamps sollten hier besser nicht angepasst werden weil die durch den komplizierten
      // Prozess mit den Movements berechnet werden. Da sollte hier besser nichts kaputt gemacht
      // werden.
    }
  }


  public void updateIfEarlierOrLater(long caseID, Timestamp newTime) throws ImportException, SQLException {
    CaseInfo anInfo = getCase(caseID);
    if ((anInfo.firstMovement == null) || (anInfo.firstMovement.getTime() > newTime.getTime())) {
      anInfo.firstMovement = newTime;
      updatedCases.add(anInfo);
    }
    if ((anInfo.lastMovement == null) || (anInfo.lastMovement.getTime() < newTime.getTime())) {
      anInfo.lastMovement = newTime;
      updatedCases.add(anInfo);
    }
  }


  public void updateAdmission(long caseID, Timestamp newTime) throws ImportException, SQLException {
    CaseInfo anInfo = getCase(caseID);
    if ((anInfo.admission == null) || (anInfo.admission.getTime() > newTime.getTime())) {
      anInfo.admission = newTime;
      updatedCases.add(anInfo);
    }
  }


  public void updateDischarge(long caseID, Timestamp newTime) throws ImportException, SQLException {
    CaseInfo anInfo = getCase(caseID);
    if ((anInfo.discharge == null) || (anInfo.discharge.getTime() < newTime.getTime())) {
      anInfo.discharge = newTime;
      updatedCases.add(anInfo);
    }
  }


  public void read() throws SQLException {
    ImportLogManager.info("Loading case meta infos");
    case2Info.clear();
    pid2Infos.clear();
    caseAdapter.read();
  }


  public CaseInfo createCaseInfo(long pid, long fallid, Timestamp admission, Timestamp discharge,
          Timestamp firstMovement, Timestamp lastMovement, Timestamp measureTime, boolean storno, String casetype,
          String importFile, Timestamp importDate) throws ImportException, SQLException {
    CaseInfo info = new CaseInfo(pid, fallid, admission, discharge, firstMovement, lastMovement, measureTime, storno,
            casetype, importFile, importDate);
    checkStornoOfCase(info);
    return info;
  }


  // this is only for the ImportAdapter to mark the info as storno right after it is loaded. This
  // method shouldn't be called by anyone else
  public void checkStornoOfCase(CaseInfo anInfo) throws ImportException {
    PatientInfo pInfo = importerManager.patientManager.getPatient(anInfo.pid);
    if (pInfo.storno) {
      anInfo.storno = true;
    }
  }


  public void addCase(CaseInfo aCaseInfo) {
    case2Info.put(aCaseInfo.caseID, aCaseInfo);
    if (!pid2Infos.containsKey(aCaseInfo.pid)) {
      pid2Infos.put(aCaseInfo.pid, new HashSet<CaseInfo>());
    }
    Set<CaseInfo> infos = pid2Infos.get(aCaseInfo.pid);
    infos.add(aCaseInfo);
    if (case2Info.size() % 100000 == 0) {
      System.out.print(".");
    }
  }

}
