package de.uniwue.dw.imports;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringEscapeUtils;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.configured.data.ConfigCatalogEntry;
import de.uniwue.dw.imports.configured.data.ConfigDataSource;
import de.uniwue.dw.imports.data.DocInfo;
import de.uniwue.dw.imports.manager.ImportLogManager;
import de.uniwue.dw.imports.manager.ImporterManager;

public class ImporterPMDBefund extends ImporterFullFile {

  protected CatalogEntry docTextEntry, befundTextEntry, defektEntry;

  public static final String befundTextExtID = "BefundText";

  private String befundTagName;

  /**
   * zero arguments constructor needed for service loader
   */
  public ImporterPMDBefund() {
  }

  public ImporterPMDBefund(ImporterManager anImporterManager, ConfigCatalogEntry aParentEntry,
          String projectName, ConfigDataSource aDataSource, String aBefundTagName)
          throws ImportException {
    super(anImporterManager, aParentEntry, projectName, aDataSource);
    befundTagName = aBefundTagName;
  }

  @Override
  protected void runBeforeImport() throws ImportException {
    importCatalog();
  }

  @Override
  public void importCatalog() throws ImportException {
    super.importCatalog();
    try {
      docTextEntry = getCatalogManager().getEntryByRefID(getProject(),
              CatalogImporter.ROOT_PROJECT_NAME);
      befundTextEntry = getOrCreateEntry(ImporterPMDBefund.befundTextExtID, CatalogEntryType.Text,
              befundTextExtID);
      defektEntry = getOrCreateEntry("defekt", CatalogEntryType.Bool, "defekt", befundTextEntry);
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, "", getProject(), e);
    }
  }

  @Override
  protected boolean processImportInfoFile(IDataElem aFile, DocInfo docInfo) throws ImportException {
    String befundText = "";
    try {
      String xml = aFile.getContent();
      if (xml.matches(".*txt nicht gefunden.*")) {
        insert(defektEntry, docInfo.PID, xml, docInfo.creationTime, docInfo.caseID, docInfo.docID);
      }
      Pattern pattern = Pattern.compile(".*<" + befundTagName + ">(.*)<" + befundTagName + ">.*",
              Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
      Matcher matcher = pattern.matcher(xml);
      if (matcher.find()) {
        befundText = matcher.group(1);
      } else {
        // if the file was exported with a defect and the closing tag is missing...
        Pattern pattern2 = Pattern.compile(".*<" + befundTagName + ">(.*)",
                Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
        Matcher matcher2 = pattern2.matcher(xml);
        if (matcher2.find()) {
          befundText = matcher2.group(1);
        } else {
          if (xml.toLowerCase().contains("<" + befundTagName.toLowerCase() + "/>")) {
            ImportLogManager.warn("Import tag <" + befundTagName + "> was empty",
                    ImportExceptionType.DATA_MALFORMED, getProject(), aFile, 0);
          } else {
            throw new ImportException(ImportExceptionType.XML_ELEMENT_NOT_FOUND,
                    "no befund section found in file with id: " + docInfo.docID);
          }
        }
      }
      befundText = StringEscapeUtils.unescapeXml(befundText);
      insert(befundTextEntry, docInfo.PID, befundText, docInfo.creationTime, docInfo.caseID,
              docInfo.docID);
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, "", getProject(), e);
    }
    return true;
  }

}
