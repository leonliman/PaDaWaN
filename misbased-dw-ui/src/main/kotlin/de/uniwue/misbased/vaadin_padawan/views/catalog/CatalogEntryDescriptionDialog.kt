package de.uniwue.misbased.vaadin_padawan.views.catalog

import com.vaadin.flow.component.Html
import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.dialog.Dialog
import com.vaadin.flow.component.icon.FontIcon
import com.vaadin.flow.component.orderedlayout.Scroller
import de.uniwue.dw.core.model.data.CatalogEntry
import de.uniwue.misbased.vaadin_padawan.data.FONT_ICON_FAMILY

class CatalogEntryDescriptionDialog(catalogEntry: CatalogEntry) : Dialog() {

    init {
        width = "80%"
        height = "80%"

        headerTitle = "${catalogEntry.name} - ${getTranslation("web-padawan.catalog.contextMenu.description")}"
        header.add(createCloseButton())

        add(createContent(catalogEntry))
    }

    private fun createCloseButton(): Button {
        val closeIcon = FontIcon(FONT_ICON_FAMILY, "fa-times")
        return Button(closeIcon) {
            close()
        }
    }

    private fun createContent(catalogEntry: CatalogEntry): Scroller {
        val scroller = Scroller()
        scroller.setWidthFull()
        scroller.scrollDirection = Scroller.ScrollDirection.VERTICAL
        val description =
            catalogEntry.description.ifBlank { getTranslation("web-padawan.catalog.contextMenu.description.notAvailable") }
        scroller.content = Html("<span>$description</span>")
        return scroller
    }
}