package de.uniwue.dw.imports.manager;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.api.DWImportsConfig;
import de.uniwue.dw.imports.data.CaseInfo;
import de.uniwue.dw.imports.data.DocInfo;
import de.uniwue.dw.imports.data.PatientInfo;
import de.uniwue.dw.imports.manager.adapter.IDocImportAdapter;

/**
 * Cached access to doc infos through SQLDocImportAdapter.
 * 
 * @see SQLDocImportAdapter
 */
public class DocManager {

  private IDocImportAdapter docAdapter;

  private Map<Long, DocInfo> docs = new HashMap<Long, DocInfo>();

  private ImporterManager importerManager;

  public DocManager(ImporterManager anImporterManager) throws SQLException {
    importerManager = anImporterManager;
    initializeAdapters();
  }


  public void initializeAdapters() throws SQLException {
    docAdapter = DWImportsConfig.getInstance().getImportsAdapterFactory().createDocImportAdapter(this);
  }


  public void insert(long docID, long fallID, Timestamp creationTime, long pid, String type, boolean storno,
          String importFile) throws ImportException, SQLException {
    docAdapter.insert(docID, fallID, creationTime, pid, type, storno, importFile);
    if (!docs.containsKey(docID)) {
      DocInfo anInfo = createDoc(docID, fallID, creationTime, pid, storno);
      addDoc(anInfo);
    } else {
      DocInfo anInfo = getDoc(docID);
      anInfo.caseID = fallID;
      anInfo.creationTime = creationTime;
      anInfo.storno = storno;
    }
  }


  public void deleteInfosOfStornoDocs() throws SQLException {
    docAdapter.deleteInfosOfStornoDocs();
  }


  public void clear() {
    docs.clear();
  }


  public void truncateDocTables() throws SQLException {
    docAdapter.truncateTable();
    clear();
  }


  public void dispose() {
    clear();
    docAdapter.dispose();
  }


  public void commit() throws SQLException {
    docAdapter.commit();
  }


  public DocInfo getDoc(Information anInfo) throws ImportException {
    return getDoc(anInfo.getDocID());
  }


  public DocInfo getDoc(long aDocID) throws ImportException {
    DocInfo result = null;
    result = docs.get(aDocID);
    // either the metadata is not loaded at all or only the docMetaData is not loaded. In both cases
    // the docMetaData is loaded lazy
    if ((result == null) && (DWImportsConfig.getLoadMetaDataLazy() || !DWImportsConfig.getLoadDocMetaData())) {
      try {
        result = docAdapter.getDoc4DocID(aDocID);
      } catch (SQLException e) {
        throw new ImportException(ImportExceptionType.SQL_ERROR, e);
      }
    }
    if (result == null) {
      throw new ImportException(ImportExceptionType.NO_CASE, "found no document for docID '" + aDocID + "'");
    }
    return result;
  }


  // this is only for the DocImportAdapter to mark the info as storno right after it is loaded. This
  // method shouldn't be called by anyone else
  public void checkStornoOfDoc(DocInfo anInfo) throws ImportException {
    if (anInfo.caseID != 0) {
      try {
        CaseInfo cInfo = importerManager.caseManager.getCase(anInfo.caseID);
        if (cInfo.storno) {
          anInfo.storno = true;
        }
      } catch (ImportException e) {
        if (e.getType() != ImportExceptionType.PID_FILTER) {
          ImportLogManager.warn(new ImportException(ImportExceptionType.NO_CASE,
                  "Case with CaseID '" + anInfo.caseID + "' does not exist for doc with docID " + anInfo.docID + "."));
        }
        return;
      }
    } else {
      PatientInfo pInfo = importerManager.patientManager.getPatient(anInfo.PID);
      if (pInfo.storno) {
        anInfo.storno = true;
      }
    }
  }


  public DocInfo createDoc(long docID, long fallID, Timestamp creationTime, long pid, boolean storno)
          throws ImportException, SQLException {
    DocInfo anInfo = new DocInfo(docID, pid, fallID, creationTime, storno);
    checkStornoOfDoc(anInfo);
    return anInfo;
  }


  public void addDoc(DocInfo anInfo) {
    if (DWImportsConfig.getLoadDocMetaData()) {
      docs.put(anInfo.docID, anInfo);
      if (docs.size() % 1000000 == 0) {
        System.out.print(".");
      }
    }
  }


  public void read() throws SQLException {
    ImportLogManager.info("Loading doc meta infos");
    docs.clear();
    docAdapter.readTables();
  }

}
