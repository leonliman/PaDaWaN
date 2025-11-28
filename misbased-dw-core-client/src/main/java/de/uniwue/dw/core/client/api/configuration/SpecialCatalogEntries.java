package de.uniwue.dw.core.client.api.configuration;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.misc.util.ConfigException;

public class SpecialCatalogEntries {

  private static final Logger logger = LogManager.getLogger(SpecialCatalogEntries.class);

  private CatalogEntry documentID, documentGroupId, documentTime, suggester;

  private List<CatalogEntry> mostCommonTextFields = new ArrayList<>();

  public SpecialCatalogEntries() {
  }

  public CatalogEntry getDocumentID() throws ConfigException {
    checkNull(documentID, "DocumentId");
    return documentID;
  }

  public SpecialCatalogEntries setDocumentID(CatalogEntry documentID) {
    this.documentID = documentID;
    return this;
  }

  public CatalogEntry getDocumentGroupId() throws ConfigException {
    checkNull(documentGroupId, "DocumentGroupId");
    return documentGroupId;
  }

  public SpecialCatalogEntries setDocumentGroupId(CatalogEntry documentGroupId) {
    this.documentGroupId = documentGroupId;
    return this;
  }

  public CatalogEntry getDocumentTime() {
    return documentTime;
  }

  public SpecialCatalogEntries setDocumentTime(CatalogEntry documentTime) {
    this.documentTime = documentTime;
    return this;
  }

  public CatalogEntry getSuggester() throws ConfigException {
    checkNull(suggester, "Suggester");
    return suggester;
  }

  public SpecialCatalogEntries setSuggester(CatalogEntry suggester) {
    this.suggester = suggester;
    return this;
  }

  public List<CatalogEntry> getMostCommonTextFields() {
    checkEmpty(mostCommonTextFields, "MostCommonTextFields");
    return mostCommonTextFields;
  }

  private void checkEmpty(List<CatalogEntry> list2Check, String entryName) {
    if (list2Check.isEmpty()) {
      logger.warn(entryName + " is empty. this can lead to problems. Maybe update your config");
      // throw new IllegalArgumentException(entryName + " not set. Update your config.");
    }
  }

  public SpecialCatalogEntries setMostCommonTextFields(List<CatalogEntry> mostCommonTextFields) {
    this.mostCommonTextFields = mostCommonTextFields;
    return this;
  }

  private void checkNull(CatalogEntry entry2Check, String entryName) throws ConfigException {
    if (entry2Check == null) {
      throw new ConfigException(entryName + " not set. Update your config.");
    }
  }

}
