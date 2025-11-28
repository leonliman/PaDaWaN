package de.uniwue.misbased.vaadin_padawan.views.catalog

import com.vaadin.flow.component.Key
import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.button.ButtonVariant
import com.vaadin.flow.component.icon.FontIcon
import com.vaadin.flow.component.orderedlayout.FlexComponent
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.component.textfield.TextField
import com.vaadin.flow.theme.lumo.LumoUtility
import de.uniwue.dw.core.model.data.CatalogEntry
import de.uniwue.dw.core.model.data.CatalogEntryType
import de.uniwue.misbased.vaadin_padawan.data.FONT_ICON_FAMILY
import de.uniwue.misbased.vaadin_padawan.views.query.QueryAttributesCustomField

class CatalogTreeTopContent(searchTerm: String, catalogTree: CatalogTree) : HorizontalLayout() {

    init {
        addClassNames(
            LumoUtility.Border.TOP,
            LumoUtility.Border.BOTTOM,
            LumoUtility.BorderColor.CONTRAST_30,
            LumoUtility.Padding.Top.MEDIUM,
            LumoUtility.Padding.Bottom.MEDIUM
        )

        val searchField = createSearchTextField(searchTerm, catalogTree)
        val homeButton = createHomeButton(catalogTree, searchField)
        val sortButton = createSortButton(catalogTree)

        add(searchField, homeButton, sortButton)
        defaultVerticalComponentAlignment = FlexComponent.Alignment.CENTER
        expand(searchField)
    }

    private fun createSearchTextField(searchTerm: String, catalogTree: CatalogTree): TextField {
        val searchField = TextField()
        searchField.width = "0px"
        searchField.placeholder = "${getTranslation("web-padawan.catalogSearchPlaceholder")}..."
        searchField.isClearButtonVisible = true
        searchField.suffixComponent = createSearchButton(catalogTree, searchField)
        searchField.value = searchTerm

        searchField.addKeyPressListener {
            if (!it.isRepeat && (it.key == Key.ENTER || it.key == Key.NUMPAD_ENTER)) {
                catalogTree.performCatalogSearch(searchField.value).start()
            }
        }
        searchField.addValueChangeListener { event ->
            if (event.isFromClient && event.value.isBlank() && event.oldValue.isNotBlank()) {
                catalogTree.performCatalogSearch(searchField.value).start()
            }
        }

        catalogTree.addCatalogSearchResetEventListener {
            searchField.clear()
        }
        return searchField
    }

    private fun createSearchButton(catalogTree: CatalogTree, searchField: TextField): Button {
        val searchIcon = FontIcon(FONT_ICON_FAMILY, "fa-search")
        val searchButton = Button(searchIcon)
        searchButton.addClassName(LumoUtility.Background.TRANSPARENT)

        searchButton.addClickListener {
            catalogTree.performCatalogSearch(searchField.value).start()
        }
        return searchButton
    }

    private fun createHomeButton(catalogTree: CatalogTree, searchField: TextField): Button {
        val homeIcon = FontIcon(FONT_ICON_FAMILY, "fa-home")
        val homeButton = Button(homeIcon)

        homeButton.addClickListener {
            val selectedEntry = catalogTree.selectedItems.firstOrNull()
            searchField.clear()
            catalogTree.performCatalogSearch(searchField.value) {
                if (selectedEntry != null && selectedEntry.dataType != CatalogEntryType.isA) {
                    val parentEntries = mutableListOf<CatalogEntry>()
                    if (!selectedEntry.parent.isRoot)
                        parentEntries.add(selectedEntry)
                    while (parentEntries.lastOrNull()?.parent != null && parentEntries.lastOrNull()?.parent?.isRoot == false)
                        parentEntries.add(parentEntries.last().parent)
                    for (parentEntry in parentEntries.reversed())
                        catalogTree.expand(parentEntry)
                    catalogTree.select(selectedEntry)
                    val entryIndices = catalogTree.getIndicesForItems(parentEntries.reversed(), selectedEntry)
                    catalogTree.scrollToIndex(*entryIndices)
                }
                QueryAttributesCustomField.forceUIResize(200)
            }.start()
        }
        return homeButton
    }

    private fun createSortButton(catalogTree: CatalogTree): Button {
        val sortIcon = FontIcon(FONT_ICON_FAMILY, "fa-sort-amount-down")
        val sortButton = Button(sortIcon)
        if (catalogTree.isSortCatalogAlphabetical())
            sortButton.addThemeVariants(ButtonVariant.LUMO_SUCCESS)

        sortButton.addClickListener {
            val sortAlphabetical = !catalogTree.isSortCatalogAlphabetical()
            catalogTree.setCatalogSorting(sortAlphabetical, sortButton)
            QueryAttributesCustomField.forceUIResize(200)
        }
        return sortButton
    }
}