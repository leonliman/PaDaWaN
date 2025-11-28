package de.uniwue.dw.imports;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.exception.ExceptionUtils;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.imports.ImportException.ImportExceptionLevel;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.configured.data.ConfigCatalogEntry;
import de.uniwue.dw.imports.configured.data.ConfigDataSource;
import de.uniwue.dw.imports.data.CaseInfo;
import de.uniwue.dw.imports.data.DocInfo;
import de.uniwue.dw.imports.data.PatientInfo;
import de.uniwue.dw.imports.manager.ImportLogManager;
import de.uniwue.dw.imports.manager.ImporterManager;

/**
 * Abstract class that provides the architecture for importers that import a whole file, e.g.
 * discharge letters. It adds some more functionality for those files than the superclass Importer.
 */
public abstract class ImporterFullFile extends Importer {

  int successfulFiles = 0;

  boolean requireDocInfo = true;

  boolean testImportMode = false;

  protected Pattern docIDRegex;

  protected Pattern caseIDRegex;

  private List<ImportException> succesfulFiles = new ArrayList<ImportException>();

  public ImporterFullFile(ImporterManager anImporterManager, ConfigCatalogEntry aParentEntry, String projectName,
          ConfigDataSource aDataSource) throws ImportException {
    super(anImporterManager, aParentEntry, projectName, aDataSource);
  }


  /**
   * zero arguments constructor needed for plugin functionality
   */
  public ImporterFullFile() {
  }


  @Override
  protected IDataElemIterator getFilesToProcess() throws ImportException {
    // for the FullFileImporter thge files do not need to be sorted, as there cannot appear differnt
    // versions of the same document in the same directory so no versioning conflicts can happen
    // over time
    return dataSource.getDataElemsToProcess(getProject(), false);
  }


  /**
   * Check for Document needs to be deactivates in some cases. Meona and maybe also other domains do
   * not have a doc info.
   *
   * @param requireIt
   */
  protected void setRequireDocInfo(boolean requireIt) {
    requireDocInfo = requireIt;
  }


  /**
   * Deactivate some checks, in order to allow testing without import. This shall not be activated
   * in productive system.
   *
   * @param activated
   */
  protected void setTestImportMode(boolean activated) {
    setRequireDocInfo(activated ? false : true);
    testImportMode = activated;
  }


  /**
   * Standard behavior in case of non-CSV file -> process a full file the return value indicates if
   * the file was successfully imported
   *
   * @param aFile
   * @param docInfo
   * @return
   * @throws ImportException
   */
  protected boolean processImportInfoFile(IDataElem aFile, DocInfo docInfo) throws ImportException {
    try {
      String text = aFile.getContent();
      CatalogEntry docTextEntry = getDomainRoot();
      insert(docTextEntry, docInfo.PID, text, docInfo.creationTime, docInfo.caseID, docInfo.docID);
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, "", getProject(), e);
    }
    return true;
  }


  /**
   * can be overriden if something has to be initialized before the import
   *
   * @throws ImportException
   */
  @Override
  protected void runBeforeImport() throws ImportException {
    super.runBeforeImport();
    importCatalog();
  }


  @Override
  protected void commit() throws ImportException {
    super.commit();
    ImportLogManager.info(succesfulFiles);
    succesfulFiles.clear();
  }


  @Override
  public void importInfos() throws ImportException {
    runBeforeImport();
    IDataElemIterator filesToImport = getFilesToProcess();
    int skippedFiles = 0;
    do {
      IDataElem aFile = filesToImport.next();
      if (aFile == null) {
        break;
      }
      try {
        if (ImportLogManager.isDataElemAlreadyImported(aFile, getProject())) {
          skippedFiles++;
          if (skippedFiles % 1000 == 0) {
            System.out.print(";");
          }
//          System.out.print("skip file " + aFile.getName());
          continue;
        }
        boolean importFile = true;
        DocInfo docInfo = null;
        if (requireDocInfo) {
          docInfo = getDocInfo(aFile);
          if (!PIDImportFilter.getInstance().checkPID(docInfo.PID)) {
            continue;
          }
          if (docInfo.storno) {
            importFile = false;
          }
        }
        if (testImportMode) {
          if (docInfo == null) {
            Long docID = getDocID(aFile.getName());
            Long caseID = getCaseID(aFile.getName());
            docInfo = new DocInfo(docID, caseID, caseID, null, false);
          }
        }
        boolean success = true;
        if (importFile) {
          success = processImportInfoFile(aFile, docInfo);
        }
        if (success) {
          if (!(aFile instanceof DBDataElemIterator)) {
            ImportException fileImportSuccessNotification = ImportLogManager.getFileImportSuccessNotification(aFile,
                    getProject());
            succesfulFiles.add(fileImportSuccessNotification);
          }
          successfulFiles++;
          if (successfulFiles % 100 == 0) {
            System.out.print(".");
            aFile.logLatestRowNumber();
            commit();
          }
        }
      } catch (ImportException e) {
        if (e.getType() == ImportExceptionType.PID_FILTER) {
          continue;
        }
        e.setFile(aFile);
        e.setProject(getProject());
        if (e.getLevel() == ImportExceptionLevel.warning) {
          ImportLogManager.warn(e);
          ImportLogManager.fileImportSuccess(aFile, getProject());
        } else {
          ImportLogManager.error(e);
        }
      } catch (NullPointerException e) {
        ImportException x = new ImportException(ImportExceptionType.NULLPOINTER, ExceptionUtils.getStackTrace(e));
        x.setFile(aFile);
        x.setProject(getProject());
        ImportLogManager.error(x);
      } catch (Exception e) {
        ImportException x = new ImportException(ImportExceptionType.UNKNOWN, ExceptionUtils.getStackTrace(e));
        x.setFile(aFile);
        x.setProject(getProject());
        ImportLogManager.error(x);
      }
    } while (true);
    try {
      commit();
    } catch (ImportException e) {
      ImportLogManager.error(e);
    }
    ImportLogManager.info(new ImportException(ImportExceptionType.IMPORT_PROCESS,
            getProject() + ": Successfully imported " + successfulFiles + " files", getProject()));
  }


  @Override
  public PatientInfo getPatientInfo(long PID) throws ImportException {
    PatientInfo pInfo = getPatientManager().getPatient(PID);
    return pInfo;
  }


  protected DocInfo getDocInfo(IDataElem file) throws ImportException {
    Long docID = getDocID(file.getName());
    DocInfo docInfo = getDocManager().getDoc(docID);
    // some filenames do not have a case, which is correct and shall not raise an exception
    /*
     * if (docInfo.caseID <= 0L) { Long caseID = getCaseID(file.getName()); if (caseID > 0L &&
     * docInfo.caseID != caseID) { throw new ImportException(ImportExceptionType.DATA_MISMATCH,
     * "caseid from file and doc-table do not match"); } docInfo.caseID = caseID; }
     */
    return docInfo;
  }


  protected CaseInfo getCaseInfo(File file) throws ImportException {
    Long caseID = getCaseID(file.getName());
    CaseInfo caseInfo = getCaseManager().getCase(caseID);
    return caseInfo;
  }


  protected long getDocID(String filename) throws ImportException {
    long docID;

    if (docIDRegex == null) {
      docIDRegex = Pattern.compile(UniWueStdFileCodes.docIDRegexString);
    }
    Matcher matcher = docIDRegex.matcher(filename);
    if (matcher.find()) {
      String docIDString = matcher.group(1);
      docID = Long.parseLong(docIDString);
    } else {
      throw new ImportException(ImportExceptionType.FILE_FORMAT_UNKNOWN, "filename format unknown");
    }
    return docID;
  }


  protected long getCaseID(String filename) throws ImportException {
    long caseID;

    if (caseIDRegex == null) {
      caseIDRegex = Pattern.compile(UniWueStdFileCodes.caseIDRegexString);
    }
    Matcher matcher = caseIDRegex.matcher(filename);
    if (matcher.find()) {
      String caseIDString = matcher.group(1);
      caseID = Long.parseLong(caseIDString);
    } else {
      throw new ImportException(ImportExceptionType.FILE_FORMAT_UNKNOWN, "filename format unknown");
    }
    return caseID;
  }

}
