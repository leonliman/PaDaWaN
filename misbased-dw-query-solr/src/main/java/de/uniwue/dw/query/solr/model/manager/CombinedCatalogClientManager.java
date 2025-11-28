package de.uniwue.dw.query.solr.model.manager;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import de.uniwue.dw.core.client.authentication.CountType;
import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.manager.AbstractCatalogClientManager;
import de.uniwue.dw.core.model.manager.CompleteCatalogClientManager;
import de.uniwue.dw.core.model.manager.DataSourceException;
import de.uniwue.dw.query.solr.client.ISolrConstants;

public class CombinedCatalogClientManager extends AbstractCatalogClientManager {

  private CompleteCatalogClientManager ccc;

  private SolrCatalogClientManager scc;

  public CombinedCatalogClientManager() throws SQLException {
    ccc = new CompleteCatalogClientManager();
    scc = SolrCatalogClientManager.getInst();
  }

  @Override
  public CatalogEntry getRoot() {
    return ccc.getRoot();
  }

  @Override
  public List<CatalogEntry> getChildsOf(CatalogEntry parent, User user) {
    return ccc.getChildsOf(parent, user);
  }

  @Override
  public List<CatalogEntry> getChildsOf(CatalogEntry parent, User user, CountType countType,
          int minOccurrence) {
    List<CatalogEntry> entries = ccc.getChildsOf(parent, user, countType, minOccurrence);
    scc.personaliseCounts(entries, user, countType);
    return entries;
  }

  @Override
  public List<CatalogEntry> getEntriesByWordFilter(String word, User user, int limit) {
    return scc.getEntriesByWordFilter(word, user, limit);
  }

  @Override
  public List<CatalogEntry> getEntriesByWordFilter(String word, User user, CountType countType,
          int minOccurrence, int limit) {
    List<CatalogEntry> entries = scc.getEntriesByWordFilter(word, user, countType, minOccurrence,
            limit);
    scc.personaliseCounts(entries, user, countType);
    return entries;
  }

  @Override
  public CatalogEntry getTreeByWordFilter(String searchPhrase, User user) {
    return scc.getTreeByWordFilter(searchPhrase, user);
  }

  @Override
  public CatalogEntry getTreeByWordFilter(String searchPhrase, User user, CountType countType,
          int minOccurrence) {
    List<CatalogEntry> solrHits = scc.getEntriesByWordFilter(searchPhrase, user, countType,
            minOccurrence, ISolrConstants.MAX_ROWS);
    List<CatalogEntry> dbHits = new ArrayList<>();
    for (CatalogEntry c : solrHits) {
      try {
        CatalogEntry dbHit = ccc.getEntryByID(c.getAttrId(), user);
        if (dbHit != null)
          dbHits.add(dbHit);
      } catch (DataSourceException e) {
      }
    }
    CatalogEntry tree = ccc.buildTreeForHits(dbHits);
    scc.personaliseCountsForTree(tree, user, countType);
    return tree;
    // return scc.getTreeByWordFilter(searchPhrase, user, countType, minOccurrence);
  }

  @Override
  public CatalogEntry getAllAncestorsAndSiblings(CatalogEntry entry, User user, CountType countType,
          int minOccurrence) {
    return ccc.getAllAncestorsAndSiblings(entry, user, countType, minOccurrence);
  }

  @Override
  public CatalogEntry getEntryByID(int attrId, User user) throws DataSourceException {
    return ccc.getEntryByID(attrId, user);
  }

  @Override
  public CatalogEntry getEntryByRefID(String extID, String project, User user)
          throws DataSourceException {
    return ccc.getEntryByRefID(extID, project, user);
  }

  @Override
  public CatalogEntry getEntryByRefID(String extID, String project, User user,
          boolean throwExceptionIfNotExists) throws DataSourceException {
    return ccc.getEntryByRefID(extID, project, user, throwExceptionIfNotExists);
  }

  @Override
  public void dispose() {
    ccc.dispose();
    scc.dispose();
    super.dispose();
  }

  @Override
  public void reinitialize() throws DataSourceException {
    ccc.reinitialize();
  }

}
