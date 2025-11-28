
package de.uniwue.dw.query.solr;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.solr.client.solrj.SolrServerException;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.group.AuthManager;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.model.manager.UniqueNameUtil;
import de.uniwue.dw.query.solr.suggest.CatalogIndexer;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;

public class CatalogUtilFunctions {

  public static void main(String[] args) throws SolrServerException, SQLException {
    try (InputStream in = new FileInputStream("../config/solr18983.props")) {
      Properties props = new Properties();
      props.load(in);
      DwClientConfiguration.mergeProperties(props);
      //
      createAndSaveUniqueCatalogEntryNamesInDB();
      indexAllCatalogEntries();
      // testIncompleteConfig();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void indexAllCatalogEntries()
          throws SQLException, SolrServerException, IOException {
    CatalogManager catalogManager = DwClientConfiguration.getInstance().getCatalogManager();
    AuthManager authManager = DwClientConfiguration.getInstance().getAuthManager();
    CatalogIndexer catalogIndexer = new CatalogIndexer(catalogManager, authManager);
    catalogIndexer.indexAllCatalogEntries();
  }

  public static Set<CatalogEntry> getAllChildsOf(int attrid) throws SQLException {
    Set<CatalogEntry> result = new HashSet<>();
    CatalogEntry parent = DwClientConfiguration.getInstance().getCatalogManager()
            .getEntryByID(attrid);
    result.add(parent);
    for (CatalogEntry child : parent.getChildren())
      result.addAll(getAllChildsOf(child.getAttrId()));

    return result;
  }

  public static void createAndSaveUniqueCatalogEntryNamesInDB() throws SQLException {
    CatalogManager catalogManager = DwClientConfiguration.getInstance().getCatalogManager();
    UniqueNameUtil.uniqueNames.clear();
    SQLManager sqlManager = SQLPropertiesConfiguration.getInstance().getSQLManager();
    Collection<CatalogEntry> entries = catalogManager.getEntries();
    for (CatalogEntry e : entries) {
      String uniqueName = createOrRepairUniqueNameIfNecessary(catalogManager, e);
      // catalogManager.uniqueNames is set. if the name still exists, it doesn't matter
      UniqueNameUtil.uniqueNames.add(uniqueName);
      saveUniqueCatalogEntryNamesInDB(e.getAttrId(), uniqueName);
    }
  }

  private static String createOrRepairUniqueNameIfNecessary(CatalogManager catalogManager,
          CatalogEntry e) throws SQLException {
    if ((e.getProject() != null) && (e.getProject().equalsIgnoreCase("diagnose")
            || e.getProject().equalsIgnoreCase("Procedures")
            || e.getProject().equalsIgnoreCase("EntlassDiagnose")))
      return UniqueNameUtil.getUniqueName(e.getName(), e.getProject(), "");
    else
      return UniqueNameUtil.createOrRepairUniqueNameIfNecessary(e);
  }

  private static void saveUniqueCatalogEntryNamesInDB(int attrid, String uniqueName)
          throws SQLException {
    String sql = "update DWCatalog set UniqueName = ? where AttrID = ?";
    PreparedStatement stmt = SQLPropertiesConfiguration.getInstance().getSQLManager()
            .createPreparedStatement(sql);
    stmt.setString(1, uniqueName);
    stmt.setInt(2, attrid);
    stmt.execute();
  }

}
