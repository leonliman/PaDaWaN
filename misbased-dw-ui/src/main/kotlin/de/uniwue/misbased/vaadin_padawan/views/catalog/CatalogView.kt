package de.uniwue.misbased.vaadin_padawan.views.catalog

import com.vaadin.flow.component.html.H4
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.server.VaadinSession
import com.vaadin.flow.spring.security.AuthenticationContext
import de.uniwue.misbased.vaadin_padawan.data.SESSION_ATTRIBUTE_CATALOG_SEARCH_TERM
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNUserSettingsConnector

class CatalogView(@Transient private val authContext: AuthenticationContext) : VerticalLayout() {

    val catalogTree: CatalogTree

    init {
        val title = H4(getTranslation("web-padawan.catalogTitle"))

        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        val searchTerm = if (userSettings.userInterfaceUseSessionStorage)
            VaadinSession.getCurrent().getAttribute(SESSION_ATTRIBUTE_CATALOG_SEARCH_TERM) as? String ?: ""
        else
            ""
        catalogTree = CatalogTree(searchTerm, this)
        catalogTree.setWidthFull()

        val catalogTreeTopContent = CatalogTreeTopContent(searchTerm, catalogTree)
        catalogTreeTopContent.setWidthFull()

        val catalogTreeBottomContent = CatalogTreeBottomContent(authContext)
        catalogTreeBottomContent.setWidthFull()

        setSizeFull()
        add(
            title,
            catalogTreeTopContent,
            catalogTree,
            catalogTreeBottomContent
        )
        expand(catalogTree)
    }
}