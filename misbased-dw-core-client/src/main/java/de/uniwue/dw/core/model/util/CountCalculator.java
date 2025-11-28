package de.uniwue.dw.core.model.util;

import java.sql.SQLException;

import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.model.manager.InfoManager;

/*
 * Util class to calculate the counts for the different catalogEntries.
 * This class is not used. Instead the Solr-Index is doing this job
 */
public class CountCalculator {

  /*
   * Counts the distinct cases which have at least one entry in the infoTable and writes the result
   * into the countTable
   */
  public void calculateCountsBasedOnCaseIDs(InfoManager infoManager, CatalogManager catalogManager)
          throws SQLException {
    catalogManager.countAdapter.calculateCountsBasedOnCaseIDs();
  }

  /*
   * Counts the distinct patients which have at least one entry in the infoTable and writes the
   * result into the countTable
   */
  public void calculateCountsBasedOnPIDs(InfoManager infoManager, CatalogManager catalogManager)
          throws SQLException {
    catalogManager.countAdapter.calculateCountsBasedOnPIDs();
  }

  /*
   * Counts the absolute number of entries in the infoTable for each catalogEntry and writes the
   * result into the countTable
   */
  public void calculateAbsoluteCounts(InfoManager infoManager, CatalogManager catalogManager)
          throws SQLException {
    catalogManager.countAdapter.calculateAbsoluteCounts();
  }

}
