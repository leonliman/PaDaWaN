package de.uniwue.misbased.vaadin_padawan.data.padawanConnector

import com.vaadin.flow.component.UI
import de.uniwue.dw.core.client.authentication.User
import de.uniwue.dw.core.client.authentication.UserSettings.CATALOG_SEARCH_MAX_ITEMS_DEFAULT
import de.uniwue.dw.core.model.data.CatalogEntry
import de.uniwue.dw.core.model.data.CatalogEntryType
import org.springframework.stereotype.Component

@Component
object PaDaWaNCatalogConnector {

    fun getRootCatalogLevel(user: User, ui: UI): List<CatalogEntry> {
        val ccm = PaDaWaNConnector.getCatalogClientManager()
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(user, ui = ui)
        val rootEntry = ccm.root
        return ccm.getChildsOf(
            rootEntry,
            user,
            userSettings.catalogCountType,
            userSettings.catalogMinOccurrenceThreshold
        )
    }

    fun getCatalogEntryChildren(entry: CatalogEntry, user: User): List<CatalogEntry> {
        val ccm = PaDaWaNConnector.getCatalogClientManager()
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(user)
        return ccm.getChildsOf(
            entry,
            user,
            userSettings.catalogCountType,
            userSettings.catalogMinOccurrenceThreshold
        )
    }

    fun getCatalogEntryByRefIDIfExists(extID: String, project: String, user: User): CatalogEntry? {
        val ccm = PaDaWaNConnector.getCatalogClientManager()
        return ccm.getEntryByRefID(extID, project, user, false)
    }

    fun getExpandedCatalogEntries(expandedCatalogEntries: Set<Int>, user: User): List<CatalogEntry> {
        val ccm = PaDaWaNConnector.getCatalogClientManager()
        return expandedCatalogEntries.map { ccm.getEntryByID(it, user) }
    }

    enum class CatalogSearchResultType {
        NO_MATCHES,
        TOO_MANY_MATCHES,
        MATCHES
    }

    fun getCatalogTreeFilteredByTerm(
        searchTerm: String,
        user: User,
        ui: UI
    ): Pair<CatalogSearchResultType, List<CatalogEntry>> {
        val ccm = PaDaWaNConnector.getCatalogClientManager()
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(user, ui = ui)

        val resultTree = ccm.getTreeByWordFilter(
            searchTerm,
            user,
            userSettings.catalogCountType,
            userSettings.catalogMinOccurrenceThreshold
        )
        val maxSearchItems = if (user.isAdmin)
            user.settings.catalogSearchMaxItems
        else
            CATALOG_SEARCH_MAX_ITEMS_DEFAULT

        val results = mutableListOf<CatalogEntry>()
        val resultType = when {
            resultTree.children.isEmpty() -> CatalogSearchResultType.NO_MATCHES
            resultTree.descendants.size > maxSearchItems -> {
                val dummyEntry = getDummyEntryWithName(resultTree.descendants.size.toString())
                results.add(dummyEntry)
                CatalogSearchResultType.TOO_MANY_MATCHES
            }

            else -> {
                for (anEntry in resultTree.children)
                    results.add(anEntry)
                CatalogSearchResultType.MATCHES
            }
        }
        return Pair(resultType, results)
    }

    fun getDummyEntryWithName(name: String): CatalogEntry {
        return CatalogEntry(
            -1,
            name,
            CatalogEntryType.isA,
            null,
            -1,
            -1.0,
            null,
            null,
            null,
            null
        )
    }
}