package de.uniwue.misbased.vaadin_padawan.views.catalog

import com.vaadin.flow.component.ComponentEvent
import com.vaadin.flow.component.UI
import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.button.ButtonVariant
import com.vaadin.flow.component.grid.GridSortOrderBuilder
import com.vaadin.flow.component.grid.GridVariant
import com.vaadin.flow.component.html.Div
import com.vaadin.flow.component.icon.FontIcon
import com.vaadin.flow.component.orderedlayout.FlexComponent
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.component.treegrid.TreeGrid
import com.vaadin.flow.data.provider.hierarchy.HierarchicalDataCommunicator
import com.vaadin.flow.data.provider.hierarchy.HierarchyMapper
import com.vaadin.flow.dom.Element
import com.vaadin.flow.server.VaadinSession
import com.vaadin.flow.shared.Registration
import com.vaadin.flow.theme.lumo.LumoUtility
import de.uniwue.dw.core.client.authentication.CountType
import de.uniwue.dw.core.model.data.CatalogEntry
import de.uniwue.dw.core.model.data.CatalogEntryType
import de.uniwue.misbased.vaadin_padawan.data.FONT_ICON_FAMILY
import de.uniwue.misbased.vaadin_padawan.data.SESSION_ATTRIBUTE_CATALOG_SEARCH_TERM
import de.uniwue.misbased.vaadin_padawan.data.SESSION_ATTRIBUTE_EXPANDED_CATALOG_ENTRIES
import de.uniwue.misbased.vaadin_padawan.data.SESSION_ATTRIBUTE_SORT_CATALOG_ALPHABETICAL
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNCatalogConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNUserSettingsConnector
import de.uniwue.misbased.vaadin_padawan.views.query.QueryAttributesCustomField
import java.lang.reflect.Method


class CatalogTree(searchTerm: String, parentLayout: VerticalLayout) : TreeGrid<CatalogEntry>() {

    private val expandedEntriesForNonSessionStorage = mutableSetOf<Int>()
    private var isSortCatalogAlphabeticalForNonSessionStorage = false

    init {
        addThemeVariants(
            GridVariant.LUMO_NO_BORDER,
            GridVariant.LUMO_NO_ROW_BORDERS,
            GridVariant.LUMO_COMPACT
        )
        setSelectionMode(SelectionMode.SINGLE)
        isRowsDraggable = true

        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())

        val searchThread = performCatalogSearch(searchTerm, true)
        addComponentHierarchyColumn { catalogEntry ->
            val spacerBeforeIcon = Div()
            spacerBeforeIcon.width = "0.1rem"
            val iconType = when (catalogEntry.dataType) {
                CatalogEntryType.Structure -> {
                    if (isCatalogEntryExpanded(catalogEntry))
                        "fa-folder-open"
                    else
                        "fa-folder"
                }

                CatalogEntryType.SingleChoice -> "fa-list-ul"
                CatalogEntryType.Text -> "fa-font"
                CatalogEntryType.Number -> "fa-calculator"
                CatalogEntryType.Bool -> "fa-check"
                CatalogEntryType.DateTime -> "fa-calendar-alt"
                CatalogEntryType.isA -> "fa-triangle-exclamation"
                else -> "fa-question"
            }
            val dataTypeIcon = FontIcon(FONT_ICON_FAMILY, iconType)
            val spacerBetweenIconAndText = Div()
            spacerBetweenIconAndText.width = "0.25rem"
            val countToUse = when (userSettings.catalogCountType) {
                CountType.distinctPID -> catalogEntry.countDistinctPID
                CountType.distinctCaseID -> catalogEntry.countDistinctCaseID
                else -> catalogEntry.countAbsolute
            }
            val catalogEntryTitleAppendix =
                if (catalogEntry.dataType == CatalogEntryType.isA)
                    ""
                else
                    " ($countToUse)"
            val catalogEntryTitle = Div("${catalogEntry.name}$catalogEntryTitleAppendix")
            catalogEntryTitle.addClassName(LumoUtility.Overflow.HIDDEN)

            val horizontalLayout = HorizontalLayout()
            horizontalLayout.setId("catalogEntry${catalogEntry.attrId}")
            horizontalLayout.isSpacing = false
            horizontalLayout.defaultVerticalComponentAlignment = FlexComponent.Alignment.CENTER
            horizontalLayout.addClassName(LumoUtility.FontSize.SMALL)
            horizontalLayout.add(spacerBeforeIcon, dataTypeIcon, spacerBetweenIconAndText, catalogEntryTitle)
            horizontalLayout.setFlexShrink(0.0, spacerBeforeIcon, dataTypeIcon, spacerBetweenIconAndText)
            horizontalLayout
        }
        setCatalogSorting(isSortCatalogAlphabetical(), null)

        addExpandListener { event ->
            for (catalogEntry in event.items) {
                setCatalogEntryExpanded(catalogEntry, true)
                event.source.dataProvider.refreshItem(catalogEntry, false)
            }
            QueryAttributesCustomField.forceUIResize(200)
        }
        addCollapseListener { event ->
            for (catalogEntry in event.items) {
                setCatalogEntryExpanded(catalogEntry, false)
                event.source.dataProvider.refreshItem(catalogEntry, false)
            }
            QueryAttributesCustomField.forceUIResize(200)
        }

        addResizeOnScrollToUIElement(element)

        val contextMenu = CatalogTreeContextMenu(this, parentLayout)
        contextMenu.target = this

        searchThread.start()
    }

    companion object {
        fun addResizeOnScrollToUIElement(element: Element) {
            val uiResizeJavascript = QueryAttributesCustomField.getUIResizeJavascript(400)
            UI.getCurrent().page.executeJs(
                """
                var element = $0;
                let wheelEventEndTimeout = null;
                element.addEventListener('wheel', () => {
                    clearTimeout(wheelEventEndTimeout);
                    wheelEventEndTimeout = setTimeout(() => {
                        $uiResizeJavascript
                    }, 200);
                });
            """.trimIndent(),
                element
            )
        }
    }

    private fun setCatalogEntryExpanded(entry: CatalogEntry, expanded: Boolean) {
        val currentlyExpandedEntries = getIDsForExpandedCatalogEntries()
        if (expanded)
            currentlyExpandedEntries.add(entry.attrId)
        else
            currentlyExpandedEntries.remove(entry.attrId)
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        if (userSettings.userInterfaceUseSessionStorage)
            VaadinSession.getCurrent()
                .setAttribute(SESSION_ATTRIBUTE_EXPANDED_CATALOG_ENTRIES, currentlyExpandedEntries)
        else {
            expandedEntriesForNonSessionStorage.clear()
            expandedEntriesForNonSessionStorage.addAll(currentlyExpandedEntries)
        }
    }

    private fun resetCatalogEntryExpansion() {
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        if (userSettings.userInterfaceUseSessionStorage)
            VaadinSession.getCurrent().setAttribute(SESSION_ATTRIBUTE_EXPANDED_CATALOG_ENTRIES, mutableSetOf<Int>())
        else
            expandedEntriesForNonSessionStorage.clear()
    }

    private fun getIDsForExpandedCatalogEntries(): MutableSet<Int> {
        val currentlyExpandedEntries = mutableSetOf<Int>()
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        if (userSettings.userInterfaceUseSessionStorage)
            (VaadinSession.getCurrent().getAttribute(SESSION_ATTRIBUTE_EXPANDED_CATALOG_ENTRIES) as? MutableSet<*>)
                ?.filterIsInstanceTo(currentlyExpandedEntries)
        else
            currentlyExpandedEntries.addAll(expandedEntriesForNonSessionStorage)
        return currentlyExpandedEntries
    }

    private fun isCatalogEntryExpanded(entry: CatalogEntry): Boolean {
        val currentlyExpandedEntries = getIDsForExpandedCatalogEntries()
        return currentlyExpandedEntries.contains(entry.attrId)
    }

    class CatalogSearchResetEvent(source: CatalogTree?) :
        ComponentEvent<CatalogTree>(source, false)

    fun addCatalogSearchResetEventListener(listener: (CatalogSearchResetEvent) -> Unit): Registration =
        eventBus.addListener(CatalogSearchResetEvent::class.java, listener)

    @Suppress("UNCHECKED_CAST")
    private fun getIndexForItem(item: CatalogEntry): Int {
        val dataCommunicator: HierarchicalDataCommunicator<CatalogEntry> = dataCommunicator
        var getHierarchyMapper: Method? = null
        try {
            getHierarchyMapper = HierarchicalDataCommunicator::class.java.getDeclaredMethod("getHierarchyMapper")
            getHierarchyMapper.setAccessible(true)
            val mapper = getHierarchyMapper.invoke(dataCommunicator) as HierarchyMapper<CatalogEntry, *>
            return mapper.getIndex(item)
        } catch (e: Exception) {
            e.printStackTrace()
        }
        return -1
    }

    fun getIndicesForItems(parentEntries: List<CatalogEntry>, catalogEntry: CatalogEntry): IntArray {
        val entryIndices = parentEntries.map { getIndexForItem(it) }.toMutableList()
        entryIndices.add(getIndexForItem(catalogEntry))
        for ((index, value) in entryIndices.reversed().withIndex()) {
            val indexToUse = entryIndices.size - 1 - index
            if (indexToUse > 0) {
                entryIndices[indexToUse] = value - entryIndices[indexToUse - 1] - 1
            }
        }
        return entryIndices.toIntArray()
    }

    fun performCatalogSearch(
        searchTermEntered: String,
        forInitialSetup: Boolean = false,
        endOfThreadAction: () -> Unit = {}
    ): Thread {
        if (!forInitialSetup) {
            deselectAll()
            resetCatalogEntryExpansion()
        }

        val loadingMessage = getTranslation("web-padawan.catalog.loading")
        setItemsToDummyEntryWithMessage(loadingMessage)

        val ui = UI.getCurrent()
        val user = PaDaWaNConnector.getUser()
        val idsForExpandedCatalogEntries = getIDsForExpandedCatalogEntries()
        val searchTerm = searchTermEntered.trim()
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(user)
        val searchThread = if (searchTerm.isBlank()) {
            if (userSettings.userInterfaceUseSessionStorage)
                VaadinSession.getCurrent().setAttribute(SESSION_ATTRIBUTE_CATALOG_SEARCH_TERM, null)
            eventBus.fireEvent(CatalogSearchResetEvent(this))
            Thread {
                val rootCatalogLevel = PaDaWaNCatalogConnector.getRootCatalogLevel(user, ui)
                val expandedCatalogEntries = if (forInitialSetup)
                    PaDaWaNCatalogConnector.getExpandedCatalogEntries(idsForExpandedCatalogEntries, user)
                else
                    emptyList()
                ui.access {
                    if (rootCatalogLevel.isNotEmpty()) {
                        setItems(rootCatalogLevel) { catalogEntry ->
                            PaDaWaNCatalogConnector.getCatalogEntryChildren(catalogEntry, user)
                        }
                        if (forInitialSetup)
                            expand(expandedCatalogEntries)
                    } else {
                        val message = getTranslation("web-padawan.catalog.noEntries")
                        setItemsToDummyEntryWithMessage(message)
                    }
                    endOfThreadAction()
                }
            }
        } else {
            if (userSettings.userInterfaceUseSessionStorage)
                VaadinSession.getCurrent().setAttribute(SESSION_ATTRIBUTE_CATALOG_SEARCH_TERM, searchTerm)
            Thread {
                val (resultType, results) = PaDaWaNCatalogConnector.getCatalogTreeFilteredByTerm(
                    searchTerm,
                    user,
                    ui
                )
                val expandedCatalogEntries = if (forInitialSetup)
                    PaDaWaNCatalogConnector.getExpandedCatalogEntries(idsForExpandedCatalogEntries, user)
                else
                    emptyList()
                ui.access {
                    when (resultType) {
                        PaDaWaNCatalogConnector.CatalogSearchResultType.NO_MATCHES -> {
                            val message = getTranslation("web-padawan.catalogSearch.noResults")
                            setItemsToDummyEntryWithMessage(message)
                        }

                        PaDaWaNCatalogConnector.CatalogSearchResultType.TOO_MANY_MATCHES -> {
                            val numResultsString = results.first().name
                            val message = getTranslation("web-padawan.catalogSearch.tooManyResults", numResultsString)
                            setItemsToDummyEntryWithMessage(message)
                        }

                        PaDaWaNCatalogConnector.CatalogSearchResultType.MATCHES -> {
                            setItems(results) { catalogEntry ->
                                catalogEntry.children
                            }
                            val topEntriesWithChildren = results.filter { it.children.isNotEmpty() }
                            val entriesWithChildren = mutableListOf<CatalogEntry>()
                            entriesWithChildren.addAll(topEntriesWithChildren)
                            for (entry in topEntriesWithChildren)
                                entriesWithChildren.addAll(entry.descendants.filter { it.children.isNotEmpty() })
                            if (forInitialSetup)
                                expand(expandedCatalogEntries)
                            else
                                expand(entriesWithChildren)
                        }
                    }
                    endOfThreadAction()
                }
            }
        }
        return searchThread
    }

    private fun setItemsToDummyEntryWithMessage(message: String) {
        val dummyEntry = PaDaWaNCatalogConnector.getDummyEntryWithName(message)
        setItems(listOf(dummyEntry)) { emptyList() }
    }

    fun setCatalogSorting(sortAlphabetical: Boolean, sortButton: Button?) {
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        if (userSettings.userInterfaceUseSessionStorage)
            VaadinSession.getCurrent().setAttribute(SESSION_ATTRIBUTE_SORT_CATALOG_ALPHABETICAL, sortAlphabetical)
        else
            isSortCatalogAlphabeticalForNonSessionStorage = sortAlphabetical

        if (sortAlphabetical) {
            val dataColumn = columns.first()
            dataColumn.setComparator(CatalogEntry::getName)
            isMultiSort = true // Workaround for changing the sorting order at runtime,
            // as otherwise calling sort in the following line with only one sort order will not change the UI,
            // because the sort order is assumed to be the same as before
            sort(GridSortOrderBuilder<CatalogEntry>().thenAsc(dataColumn).thenAsc(dataColumn).build())
            isMultiSort = false
            sortButton?.addThemeVariants(ButtonVariant.LUMO_SUCCESS)
        } else {
            val dataColumn = columns.first()
            dataColumn.setComparator(CatalogEntry::getOrderValue)
            sort(GridSortOrderBuilder<CatalogEntry>().thenAsc(dataColumn).build())
            sortButton?.removeThemeVariants(ButtonVariant.LUMO_SUCCESS)
        }
    }

    fun isSortCatalogAlphabetical(): Boolean {
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        return if (userSettings.userInterfaceUseSessionStorage)
            VaadinSession.getCurrent().getAttribute(SESSION_ATTRIBUTE_SORT_CATALOG_ALPHABETICAL) as? Boolean == true
        else
            isSortCatalogAlphabeticalForNonSessionStorage
    }
}