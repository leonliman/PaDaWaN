package de.uniwue.dw.query.model.client;

import java.util.ArrayList;
import java.util.List;

import de.uniwue.dw.core.client.authentication.CountType;
import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.hooks.IDwCatalogHooks;
import de.uniwue.dw.core.model.manager.DataSourceException;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.ContentOperator;
import de.uniwue.dw.query.model.table.QueryTableEntry;

public class QuickSearch {

  private static final int QUICK_SEARCH_CATALOG_ENTRY_LIMIT = 100;

  public static List<QueryTableEntry> runQuickSearch(String queryString, boolean containsPositive,
          User user, CountType countType, int minOccurrence, IGUIClient guiClient)
          throws GUIClientException {
    List<QueryTableEntry> queryEntries = new ArrayList<>();
    if (!queryString.isEmpty()) {
      try {
        CatalogEntry letterCatalogEntry = guiClient.getCatalogClientProvider()
                .getEntryByRefID("brieftext", IDwCatalogHooks.PROJECT_HOOK_LETTER, user);
        QueryTableEntry viewEntry = new QueryTableEntry(letterCatalogEntry, "");
        if (containsPositive)
          viewEntry.setOperator(ContentOperator.CONTAINS_POSITIVE);
        else
          viewEntry.setOperator(ContentOperator.CONTAINS);
        viewEntry.setArgument(queryString);
        queryEntries.add(viewEntry);
      } catch (DataSourceException e) {
        throw new GUIClientException(e);
      }
    }
    List<CatalogEntry> machtedEntries = new ArrayList<CatalogEntry>();
    for (String word : queryString.split("\\s+")) {
      word = word.trim();
      machtedEntries.addAll(guiClient.getCatalogClientProvider().getEntriesByWordFilter(word, user,
              countType, minOccurrence, QUICK_SEARCH_CATALOG_ENTRY_LIMIT));
    }
    if (machtedEntries.size() > QUICK_SEARCH_CATALOG_ENTRY_LIMIT) {
      String exceptionText = "Die Suche ergab im Katalog mehr als "
              + QUICK_SEARCH_CATALOG_ENTRY_LIMIT
              + " Treffer, daher wird nur das Ergebnis für die Suche im Brieftext (Entlassbrief) angezeigt.\n"
              + "Für Beispielanfragen platzieren Sie bitte Ihre Maus auf dem Eingabefeld der Schnellsuche.";
      throw new QueryException(exceptionText);
    } else {
      for (CatalogEntry machtedEntry : machtedEntries) {
        QueryTableEntry booleanViewEntry = new QueryTableEntry(machtedEntry, "");
        booleanViewEntry.setOperator(ContentOperator.EXISTS);
        queryEntries.add(booleanViewEntry);
      }
    }
    return queryEntries;
  }

}
