package de.uniwue.dw.core.client.api.configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.manager.DataSourceException;
import de.uniwue.dw.core.model.manager.ICatalogClientManager;

public class SpecialCatalogEntriesBuilder {

  private static final Logger logger = LogManager.getLogger(SpecialCatalogEntries.class);

  private CatalogEntry documentID, documentGroupId, documentTime, suggester;

  private List<CatalogEntry> mostCommonTextFields = new ArrayList<>();

  private ICatalogClientManager catalogClientManager;

  public SpecialCatalogEntriesBuilder(ICatalogClientManager catalogClientManager) {
    this.catalogClientManager = catalogClientManager;
  }

  public SpecialCatalogEntriesBuilder setDocumentID(String extId, String project) {
    documentID = getEntry(extId, project);
    return this;
  }

  public SpecialCatalogEntriesBuilder setDocumentGroupId(String extId, String project) {
    documentGroupId = getEntry(extId, project);
    return this;
  }

  public SpecialCatalogEntriesBuilder setDocumentTime(String extId, String project) {
    documentTime = getEntry(extId, project);
    return this;
  }

  public SpecialCatalogEntriesBuilder setSuggester(String extId, String project) {
    suggester = getEntry(extId, project);
    return this;
  }

  public SpecialCatalogEntriesBuilder setMostCommonTextFields(String[]... extIdAndProjectPairs) {
    return setMostCommonTextFields(Arrays.asList(extIdAndProjectPairs));
  }

  public SpecialCatalogEntries build() {
    // parameters can be null. the getter will throw an exception.
    // if (documentID == null || documentGroupId == null || documentTime == null || suggester ==
    // null
    // || mostCommonTextFields.isEmpty())
    // throw new IllegalArgumentException("not all parameters are set");
    return new SpecialCatalogEntries().setDocumentID(documentID).setDocumentGroupId(documentGroupId)
            .setDocumentTime(documentTime).setSuggester(suggester)
            .setMostCommonTextFields(mostCommonTextFields);
  }

  public SpecialCatalogEntriesBuilder setMostCommonTextFields(
          List<String[]> extIdAndProjtectPairs) {
    for (String[] extIdAndProject : extIdAndProjtectPairs) {
      if (extIdAndProject.length != 2) {
        throw new IllegalArgumentException("Expecting extID and Project");
      } else {
        CatalogEntry commonTextField = getEntry(extIdAndProject[0], extIdAndProject[1]);
        if (commonTextField != null)
          mostCommonTextFields.add(commonTextField);
      }
    }
    return this;
  }

  private CatalogEntry getEntry(String extId, String project) {
    try {
      if (catalogClientManager == null) {
        return null;
      } else {
        return catalogClientManager.getEntryByRefID(extId, project, null, false);
      }
    } catch (DataSourceException e) {
      logger.debug("special entry does not exist", e);
      return null;
    }
  }
}
