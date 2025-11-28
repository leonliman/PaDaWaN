package de.uniwue.dw.core.model.util;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.model.manager.DWIterException;
import de.uniwue.dw.core.model.manager.InfoIterator;
import de.uniwue.dw.core.model.manager.InfoManager;
import de.uniwue.misc.util.RegexUtil;

/*
 * Util class to gues the dataTypes of catalogEntries based on the facts in the infoTable.
 * TODO: The approach used by this class has the drawback that when an entry is splitted 
 * into an entry representing the number facts and a sub entry representing the non number 
 * facts, further imports do not recognize this split and still import into the parent entry
 */
public class DataTypeGuesser {

  public static String nonNumberExtIDPostfix = "_NonNumber";

  public static String nonNumberNamePostfix = " (keine Zahl)";

  public static String dateTimeRegexString = "\\d{2}\\.\\d{2}\\.\\d{4}";

  public void guessAndUpdateDataTypes(InfoManager infoManager, CatalogManager catalogManager,
          String aProject) throws SQLException, DWIterException {
    Collection<CatalogEntry> entriesOfProject = catalogManager.getEntriesOfProject(aProject);
    int x = 0;
    System.out.println("ToDo: " + entriesOfProject.size());
    for (CatalogEntry anEntry : entriesOfProject) {
      guessAndUpdateDataTypes(infoManager, catalogManager, anEntry);
      x++;
      System.out.println("done: " + x + " " + anEntry.getAttrId() + " " + anEntry.getName() + " "
              + anEntry.getDataType());
    }
  }

  /*
   * Gues the dataType of an entry based on a list of fact Strings
   */
  public CatalogEntryType guessAndUpdateDataTypes(List<String> values) {
    // wenn der Typ "Text" werden soll, jedoch weniger als diese Anzahl an verschiedenen Infos
    // vorliegt, wird der Typ zu "SingleChoice"
    final int differentValuesForSingleChoice = 10;
    // wenn weniger als dieser Prozentsatz an Texten in den Infos vorkommt (ansonsten nur Zahlen)
    // wird der Eintrag zum Typ "Number". Alle nicht numerischen Werte werden in einen neu erzeugten
    // Katalogeintrag verschoben, der als Kindknoten des zu ratenden Knotens angelegt wird.
    final double allowedNonNumberFraction = 0.80;
    // für eine Konvertierung in "SingleChoice" müssen jedoch mindestens so viele Infos überhaupt
    // vorliegen, ansonsten bleibt es bei "Text"
    final int toGuessLimit = 20;
    int nonNumberCount = 0;
    int nonDateTimeCount = 0;
    boolean canBeSingleChoice = true;
    Set<String> differentValues = new HashSet<String>();

    int numberOfValues = values.size();
    if (numberOfValues == 0) {
      return CatalogEntryType.Structure;
    }
    CatalogEntryType guessedType = CatalogEntryType.Text;
    for (String value : values) {
      if (!RegexUtil.allNumbersRegex.matcher(value).find()) {
        nonNumberCount++;
      }
      if (!value.matches(dateTimeRegexString)) {
        nonDateTimeCount++;
      }
      if (canBeSingleChoice) {
        differentValues.add(value);
        if (differentValues.size() >= differentValuesForSingleChoice) {
          canBeSingleChoice = false;
        }
      }
    }

    if (nonNumberCount * 1.0 / numberOfValues < allowedNonNumberFraction) {
      guessedType = CatalogEntryType.Number;
    } else if (nonDateTimeCount * 1.0 / numberOfValues < allowedNonNumberFraction) {
      guessedType = CatalogEntryType.DateTime;
    } else if (canBeSingleChoice && (numberOfValues >= toGuessLimit)) {
      if (differentValues.size() == 1) {
        guessedType = CatalogEntryType.Bool;
      } else {
        guessedType = CatalogEntryType.SingleChoice;
      }
    } else {
      guessedType = CatalogEntryType.Text;
    }
    return guessedType;
  }

  // Diese Methode nimmt einen KatalogEintrag und alle dazugehörigen Informationen und versucht den
  // Datentyp des KatalogEintrags aus den Werten der Informationen zu erraten.
  public void guessAndUpdateDataTypes(InfoManager infoManager, CatalogManager catalogManager,
          CatalogEntry anEntry) throws SQLException, DWIterException {
    InfoIterator infosByAttrID = infoManager.getInfosByAttrID(anEntry.getAttrId(), true);
    List<String> valueList = new ArrayList<String>();
    for (Information anInfo : infosByAttrID) {
      String value = anInfo.getValue();
      if (value != null) {
        valueList.add(value);
      }
    }
    CatalogEntryType newType = guessAndUpdateDataTypes(valueList);
    if (anEntry.getDataType() != newType) {
      anEntry.setDataType(newType);
      catalogManager.updateEntry(anEntry);
      if (newType == CatalogEntryType.Number) {
        moveNonNumberInfosToSubEntry(anEntry, infosByAttrID, infoManager, catalogManager);
      }
    }
    infoManager.commit();
    infosByAttrID.dispose();
  }

  /*
   * Deltes non number facts and recreates them as facts of the sub catalog entries which represent
   * the non number facts for the parent entry
   */
  private void moveNonNumberInfosToSubEntry(CatalogEntry numberEntry, InfoIterator infosByAttrID,
          InfoManager infoManager, CatalogManager catalogManager) throws SQLException {
    CatalogEntry nonNumberEntry = createNewNonNumberEntry(numberEntry, catalogManager);
    for (Information anInfo : infosByAttrID) {
      String value = anInfo.getValue();
      if (value == null) {
        continue;
      }
      if (!RegexUtil.allNumbersRegex.matcher(value).find()) {
        infoManager.deleteInfo(numberEntry, anInfo.getPid(), anInfo.getCaseID(),
                anInfo.getMeasureTime());
        infoManager.insert(nonNumberEntry, anInfo.getPid(), anInfo.getValue(),
                anInfo.getMeasureTime(), anInfo.getCaseID(), 0);
      }
    }
  }

  private CatalogEntry createNewNonNumberEntry(CatalogEntry numberEntry,
          CatalogManager catalogManager) throws SQLException {
    String nonNumberExtID = numberEntry.getExtID() + nonNumberExtIDPostfix;
    String nonNumberName = numberEntry.getName() + nonNumberNamePostfix;
    String nonNumberUniqueName = numberEntry.getUniqueName() + nonNumberNamePostfix;
    CatalogEntry nonNumberEntry = catalogManager.getOrCreateEntry(nonNumberName,
            CatalogEntryType.Text, nonNumberExtID, numberEntry, numberEntry.getProject(),
            nonNumberUniqueName, numberEntry.getDescription());
    return nonNumberEntry;
  }

}
