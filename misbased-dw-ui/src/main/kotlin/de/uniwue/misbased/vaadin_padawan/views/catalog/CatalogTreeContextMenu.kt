package de.uniwue.misbased.vaadin_padawan.views.catalog

import com.vaadin.flow.component.UI
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import de.uniwue.dw.core.model.data.CatalogEntry
import de.uniwue.dw.core.model.data.CatalogEntryType
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNUserSettingsConnector
import de.uniwue.misbased.vaadin_padawan.views.query.QueryAttributesCustomField

class CatalogTreeContextMenu(catalogTree: CatalogTree, parentLayout: VerticalLayout) :
    CustomGridContextMenu<CatalogEntry>() {

    init {
        addItem(getTranslation("web-padawan.catalog.contextMenu.description")) {
            val catalogEntry = it.item.orElse(null)
            if (catalogEntry != null) {
                val dialog = CatalogEntryDescriptionDialog(catalogEntry)
                dialog.open()
                QueryAttributesCustomField.forceUIResize()
                catalogTree.select(catalogEntry)
            }
        }

        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())

        if (userSettings.catalogShowMetadata) {
            addItem(getTranslation("web-padawan.catalog.contextMenu.properties")) {
                val catalogEntry = it.item.orElse(null)
                if (catalogEntry != null) {
                    val id = "catalogEntry${catalogEntry.attrId}"
                    val popup = CatalogEntryPropertiesPopup(catalogEntry, parentLayout)
                    popup.`for` = id
                    parentLayout.add(popup)
                    popup.open()
                    catalogTree.select(catalogEntry)
                }
            }
        }

        addGridContextMenuOpenedListener { event ->
            val catalogEntry = event.item.orElse(null)
            if (catalogEntry != null && catalogEntry.dataType == CatalogEntryType.isA) {
                close()
            }
        }
    }
}