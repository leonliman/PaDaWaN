package de.uniwue.dw.query.solr.preprocess.util;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.sql.IDwSqlSchemaConstant;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;

public class Util implements IDwSqlSchemaConstant {

  public static void main(String[] args) throws SQLException {
    SQLPropertiesConfiguration.getInstance().getSQLManager().shrinkAllDBsOnTheServer();
    SQLPropertiesConfiguration.getInstance().getSQLManager().shrinkAllFilesOnServer();
  }

  public static Set<Integer> getAllParents(CatalogManager catalogManager, int attrId)
          throws SQLException {
    CatalogEntry entry = catalogManager.getEntryByID(attrId);
    return getAllParents(catalogManager, entry);
  }

  public static Set<Integer> getAllParents(CatalogManager catalogManager, CatalogEntry entry)
          throws SQLException {
    CatalogEntry parent = entry.getParent();
    if (parent.getProject().equalsIgnoreCase("Untersuchung")) {
      return new HashSet<Integer>();
    } else {
      Set<Integer> parents = getAllParents(catalogManager, parent);
      parents.add(parent.getAttrId());
      return parents;
    }
  }

}
