package de.uniwue.dw.core.model.util;

import java.sql.SQLException;
import java.util.Collection;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.model.manager.DWIterException;
import de.uniwue.dw.core.model.manager.InfoIterator;
import de.uniwue.dw.core.model.manager.InfoManager;
import de.uniwue.misc.util.RegexUtil;

/*
 * Util class that updates the dec-column in the DWInfo table.
 * This class is not needed if everything works as intended.
 */
public class DecValueCleaner {

  public void updateDecValues(InfoManager infoManager, CatalogManager catalogManager,
          String aProject) throws SQLException, DWIterException {
    Collection<CatalogEntry> entriesOfProject = catalogManager.getEntriesOfProject(aProject);
    int x = 0;
    System.out.println("ToDo: " + entriesOfProject.size());
    for (CatalogEntry anEntry : entriesOfProject) {
      updateDecValues(infoManager, anEntry);
      x++;
      System.out.println("done: " + x + " " + anEntry.getAttrId() + " " + anEntry.getName() + " "
              + anEntry.getDataType());
      infoManager.commit();
    }
  }

  public void updateDecValues(InfoManager infoManager, CatalogEntry anEntry) throws SQLException, DWIterException {
    InfoIterator infosByAttrID = infoManager.getInfosByAttrID(anEntry.getAttrId(), true);
    for (Information anInfo : infosByAttrID) {
      String value = anInfo.getValue();
      if (value == null) {
        continue;
      }
      String cleanedValue = RegexUtil.cleanNumber(value);
      if (!RegexUtil.allNumbersRegex.matcher(cleanedValue).find()) {
        infoManager.infoAdapter.setValueDecNull(anInfo.getInfoID());
      } else {
        if (!RegexUtil.allNumbersRegex.matcher(value).find()) {
          double decValue = Double.valueOf(cleanedValue);
          infoManager.infoAdapter.setValueDec(anInfo.getInfoID(), decValue);
        }
      }
    }
    infosByAttrID.dispose();
  }

}
