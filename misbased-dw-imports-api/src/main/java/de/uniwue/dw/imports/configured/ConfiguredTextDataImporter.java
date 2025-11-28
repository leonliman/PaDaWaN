package de.uniwue.dw.imports.configured;

import java.sql.SQLException;
import java.util.regex.Pattern;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.imports.IDataElem;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.ImporterFullFile;
import de.uniwue.dw.imports.configured.data.ConfigDataText;
import de.uniwue.dw.imports.data.DocInfo;
import de.uniwue.dw.imports.manager.ImporterManager;

public class ConfiguredTextDataImporter extends ImporterFullFile {

  private ConfigDataText config;

  public ConfiguredTextDataImporter(ImporterManager anImportManager, ConfigDataText aConfig)
          throws ImportException {
    super(anImportManager, aConfig.getParentEntry(), aConfig.getProject(), aConfig.getDataSource());
    config = aConfig;
    if (config.docRegex != null) {
      docIDRegex = Pattern.compile(config.docRegex);
    }
    if (config.caseRegex != null) {
      caseIDRegex = Pattern.compile(config.caseRegex);
    }
    getCatalogImporter().useAbstractNameForRootProject = false;
  }

  @Override
  protected boolean processImportInfoFile(IDataElem aFile, DocInfo docInfo) throws ImportException {
    try {
      String text = aFile.getContent();
      CatalogEntry docTextEntry;
      if (config.extID != null) {
        docTextEntry = getCatalogManager().getEntryByRefID(config.extID, getProject());
      } else {
        docTextEntry = getDomainRoot();
      }
      insert(docTextEntry, docInfo.PID, text, docInfo.creationTime, docInfo.caseID, docInfo.docID);
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, "", getProject(), e);
    }
    return true;
  }

}
