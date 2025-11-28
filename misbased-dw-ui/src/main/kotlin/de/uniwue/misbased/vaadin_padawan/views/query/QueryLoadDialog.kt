package de.uniwue.misbased.vaadin_padawan.views.query

import com.vaadin.flow.component.UI
import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.dialog.Dialog
import com.vaadin.flow.component.grid.Grid
import com.vaadin.flow.component.grid.GridVariant
import com.vaadin.flow.component.html.Div
import com.vaadin.flow.component.icon.FontIcon
import com.vaadin.flow.component.orderedlayout.FlexComponent
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.component.treegrid.TreeGrid
import com.vaadin.flow.server.VaadinSession
import com.vaadin.flow.theme.lumo.LumoUtility
import de.uniwue.dw.query.model.lang.*
import de.uniwue.misbased.vaadin_padawan.data.FONT_ICON_FAMILY
import de.uniwue.misbased.vaadin_padawan.data.SESSION_ATTRIBUTE_EXPANDED_SAVED_QUERIES
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributesRow
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributesRowContainer
import de.uniwue.misbased.vaadin_padawan.data.model.SavedQuery
import de.uniwue.misbased.vaadin_padawan.data.model.SavedQueryType
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNQueryConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNUserSettingsConnector

class QueryLoadDialog(
    private val queryViewTopSettingsRow: QueryViewTopSettingsRow,
    private val queryViewAttributesContainer: QueryViewAttributesContainer
) : Dialog() {

    private lateinit var savedQueriesTree: TreeGrid<SavedQuery>

    private var currentlyExpandedQueriesForNonSessionStorage = mutableSetOf<String>()

    init {
        width = "500px"
        isCloseOnOutsideClick = false

        headerTitle = getTranslation("web-padawan.queryActions.load.dialogTitle")

        val contentLayout = createContentLayout()
        contentLayout.setWidthFull()
        add(contentLayout)

        val bottomButtonsLayout = createBottomButtonsLayout()
        bottomButtonsLayout.setWidthFull()
        footer.add(bottomButtonsLayout)
    }

    private fun createContentLayout(): VerticalLayout {
        val contentLayout = VerticalLayout()
        contentLayout.isPadding = false
        contentLayout.isSpacing = true

        createSavedQueriesTree()
        savedQueriesTree.setWidthFull()
        contentLayout.add(savedQueriesTree)

        return contentLayout
    }

    private fun createSavedQueriesTree() {
        savedQueriesTree = TreeGrid<SavedQuery>()
        savedQueriesTree.addThemeVariants(
            GridVariant.LUMO_NO_BORDER,
            GridVariant.LUMO_NO_ROW_BORDERS,
            GridVariant.LUMO_COMPACT
        )
        savedQueriesTree.height = "750px"
        savedQueriesTree.selectionMode = Grid.SelectionMode.SINGLE

        savedQueriesTree.setItems(
            listOf(getDummySavedQuery("web-padawan.queryActions.load.dialogIsLoading")),
        ) { emptyList() }

        val ui = UI.getCurrent()
        val user = PaDaWaNConnector.getUser()
        val loadQueryTreeThread = Thread {
            val savedQueries = PaDaWaNQueryConnector.loadQueryTree(user)
            ui.access {
                resetSavedQueriesExpandedState(savedQueries.second)
                if (savedQueries.first.isNotEmpty())
                    savedQueriesTree.setItems(savedQueries.first) { savedQuery ->
                        savedQuery.children
                    }
                else {
                    savedQueriesTree.setItems(
                        listOf(getDummySavedQuery("web-padawan.queryActions.load.noQueriesSavedYet"))
                    ) { emptyList() }
                }
                if (savedQueries.second != null)
                    savedQueriesTree.expand(savedQueries.second)
            }
        }

        savedQueriesTree.addComponentHierarchyColumn { savedQuery ->
            val spacerBeforeIcon = Div()
            spacerBeforeIcon.width = "0.1rem"
            val iconType = when (savedQuery.type) {
                SavedQueryType.FOLDER -> {
                    if (isSavedQueryExpanded(savedQuery))
                        "fa-folder-open"
                    else
                        "fa-folder"
                }

                SavedQueryType.STATISTICS -> "fa-chart-bar"
                SavedQueryType.INDIVIDUAL_DATA -> "fa-list-ul"
                SavedQueryType.ERROR -> "fa-exclamation-circle"
            }
            val queryTypeIcon = FontIcon(FONT_ICON_FAMILY, iconType)
            val spacerBetweenIconAndText = Div()
            spacerBetweenIconAndText.width = "0.25rem"

            val queryName = Div(savedQuery.name)
            queryName.addClassName(LumoUtility.Overflow.HIDDEN)

            val horizontalLayout = HorizontalLayout()
            horizontalLayout.isSpacing = false
            horizontalLayout.defaultVerticalComponentAlignment = FlexComponent.Alignment.CENTER
            horizontalLayout.addClassName(LumoUtility.FontSize.SMALL)
            horizontalLayout.add(spacerBeforeIcon, queryTypeIcon, spacerBetweenIconAndText, queryName)
            horizontalLayout.setFlexShrink(0.0, spacerBeforeIcon, queryTypeIcon, spacerBetweenIconAndText)
            horizontalLayout
        }

        savedQueriesTree.addExpandListener { event ->
            for (savedQuery in event.items) {
                setSavedQueryExpanded(savedQuery, true)
                event.source.dataProvider.refreshItem(savedQuery, false)
            }
            QueryAttributesCustomField.forceUIResize(200)
        }
        savedQueriesTree.addCollapseListener { event ->
            for (catalogEntry in event.items) {
                setSavedQueryExpanded(catalogEntry, false)
                event.source.dataProvider.refreshItem(catalogEntry, false)
            }
            QueryAttributesCustomField.forceUIResize(200)
        }

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
            savedQueriesTree.element
        )

        loadQueryTreeThread.start()
    }

    private fun resetSavedQueriesExpandedState(rootQueryForCurrentUser: SavedQuery?) {
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        if (userSettings.userInterfaceUseSessionStorage)
            VaadinSession.getCurrent().setAttribute(SESSION_ATTRIBUTE_EXPANDED_SAVED_QUERIES, mutableSetOf<String>())
        else
            currentlyExpandedQueriesForNonSessionStorage.clear()
        if (rootQueryForCurrentUser != null)
            setSavedQueryExpanded(rootQueryForCurrentUser, true)
    }

    private fun getDummySavedQuery(translationKey: String) =
        SavedQuery("", getTranslation(translationKey), SavedQueryType.ERROR)

    private fun setSavedQueryExpanded(query: SavedQuery, expanded: Boolean) {
        val currentlyExpandedQueries = getIDsForExpandedQueries()
        if (expanded)
            currentlyExpandedQueries.add(query.path)
        else
            currentlyExpandedQueries.remove(query.path)
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        if (userSettings.userInterfaceUseSessionStorage)
            VaadinSession.getCurrent().setAttribute(SESSION_ATTRIBUTE_EXPANDED_SAVED_QUERIES, currentlyExpandedQueries)
        else {
            currentlyExpandedQueriesForNonSessionStorage.clear()
            currentlyExpandedQueriesForNonSessionStorage.addAll(currentlyExpandedQueries)
        }
    }

    private fun getIDsForExpandedQueries(): MutableSet<String> {
        val currentlyExpandedQueries = mutableSetOf<String>()
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        if (userSettings.userInterfaceUseSessionStorage)
            (VaadinSession.getCurrent().getAttribute(SESSION_ATTRIBUTE_EXPANDED_SAVED_QUERIES) as? MutableSet<*>)
                ?.filterIsInstanceTo(currentlyExpandedQueries)
        else
            currentlyExpandedQueries.addAll(currentlyExpandedQueriesForNonSessionStorage)
        return currentlyExpandedQueries
    }

    private fun isSavedQueryExpanded(query: SavedQuery): Boolean {
        val currentlyExpandedQueries = getIDsForExpandedQueries()
        return currentlyExpandedQueries.contains(query.path)
    }

    private fun createBottomButtonsLayout(): HorizontalLayout {
        val bottomButtonsLayout = HorizontalLayout()

        val cancelButton = createCancelButton()
        val loadButton = createLoadButton(cancelButton)

        bottomButtonsLayout.addAndExpand(loadButton, cancelButton)
        return bottomButtonsLayout
    }

    private fun createLoadButton(
        cancelButton: Button
    ): Button {
        val loadIcon = FontIcon(FONT_ICON_FAMILY, "fa-check")
        val loadButton = Button(getTranslation("web-padawan.queryActions.load"), loadIcon)

        loadButton.isEnabled = false
        savedQueriesTree.addSelectionListener { event ->
            if (event.firstSelectedItem.isPresent) {
                val type = event.firstSelectedItem.get().type
                loadButton.isEnabled = type != SavedQueryType.ERROR && type != SavedQueryType.FOLDER
            } else
                loadButton.isEnabled = false
        }

        savedQueriesTree.addItemDoubleClickListener { event ->
            if (event.item != null) {
                savedQueriesTree.select(event.item)
                QueryAttributesCustomField.resetUITextSelection()
            }
            if (savedQueriesTree.selectedItems.isNotEmpty() && loadButton.isEnabled) {
                loadButton.click()
            }
        }

        loadButton.addClickListener {
            loadButton.isEnabled = false
            cancelButton.isEnabled = false
            loadButton.text = getTranslation("web-padawan.queryActions.load.progress")
            QueryAttributesCustomField.forceUIResize()

            loadQueryToUI()
        }

        return loadButton
    }

    private fun loadQueryToUI() {
        val ui = UI.getCurrent()
        val user = PaDaWaNConnector.getUser()
        val queryToLoad = savedQueriesTree.selectedItems.first()
        QuerySaveDialog.saveLastUsedQueryName(queryToLoad.path, queryViewTopSettingsRow)
        val queryFurtherSettings = queryViewTopSettingsRow.getFurtherSettings().toMutableSet()
        val loadThread = Thread {
            val query = PaDaWaNQueryConnector.loadSingleQuery(queryToLoad.path, user)

            val queryMode = if (query.isStatisticQuery)
                QueryViewTopSettingsRow.QueryMode.STATISTICS
            else
                QueryViewTopSettingsRow.QueryMode.INDIVIDUAL_DATA

            var previewRows = 10
            when (queryMode) {
                QueryViewTopSettingsRow.QueryMode.STATISTICS -> {
                    queryFurtherSettings.remove(QueryViewTopSettingsRow.FurtherSettings.RETURN_PATIENT_COUNTS)
                    if (query.isDistinct)
                        queryFurtherSettings.add(QueryViewTopSettingsRow.FurtherSettings.RETURN_PATIENT_COUNTS)
                }

                QueryViewTopSettingsRow.QueryMode.INDIVIDUAL_DATA -> {
                    queryFurtherSettings.remove(QueryViewTopSettingsRow.FurtherSettings.RETURN_ONLY_ONE_ROW_PER_PATIENT)
                    if (query.isDistinct)
                        queryFurtherSettings.add(QueryViewTopSettingsRow.FurtherSettings.RETURN_ONLY_ONE_ROW_PER_PATIENT)
                    previewRows = query.limitResult
                }
            }

            val rootNodeToUse = query.idFilterRecursive.firstOrNull() ?: query
            val referenceSet = if (rootNodeToUse is QueryIDFilter)
                when (rootNodeToUse.filterIDType) {
                    QueryIDFilter.FilterIDType.PID -> QueryViewTopSettingsRow.ReferenceSet.PATIENTS
                    QueryIDFilter.FilterIDType.CaseID -> QueryViewTopSettingsRow.ReferenceSet.CASES
                    QueryIDFilter.FilterIDType.DocID -> QueryViewTopSettingsRow.ReferenceSet.DOCUMENTS
                    QueryIDFilter.FilterIDType.GROUP -> QueryViewTopSettingsRow.ReferenceSet.GROUPS
                    else -> {
                        throw IllegalStateException("Unsupported filter ID type: ${rootNodeToUse.filterIDType}")
                    }
                }
            else
                QueryViewTopSettingsRow.ReferenceSet.PATIENTS

            val queryAttributesRows: MutableList<List<QueryAttributesRow>> = mutableListOf()
            when (queryMode) {
                QueryViewTopSettingsRow.QueryMode.STATISTICS -> {
                    val rootNodeSiblings = rootNodeToUse.siblings

                    val rowsRoot =
                        rootNodeSiblings.firstOrNull { it is QueryStatisticRow } as QueryStructureContainingElem?
                    if (rowsRoot != null)
                        queryAttributesRows.add(QueryAttributesRowContainer.fromMXQLQuery(rowsRoot).attributeRows)
                    else
                        queryAttributesRows.add(listOf(QueryAttributesRow()))

                    val columnsRoot =
                        rootNodeSiblings.firstOrNull { it is QueryStatisticColumn } as QueryStructureContainingElem?
                    if (columnsRoot != null)
                        queryAttributesRows.add(QueryAttributesRowContainer.fromMXQLQuery(columnsRoot).attributeRows)
                    else
                        queryAttributesRows.add(listOf(QueryAttributesRow()))

                    val filtersRoot =
                        rootNodeSiblings.firstOrNull { it is QueryStatisticFilter } as QueryStructureContainingElem?
                    if (filtersRoot != null)
                        queryAttributesRows.add(QueryAttributesRowContainer.fromMXQLQuery(filtersRoot).attributeRows)
                    else
                        queryAttributesRows.add(listOf(QueryAttributesRow()))
                }

                QueryViewTopSettingsRow.QueryMode.INDIVIDUAL_DATA -> {
                    queryAttributesRows.add(QueryAttributesRowContainer.fromMXQLQuery(rootNodeToUse).attributeRows)
                }
            }
            ui.access {
                queryViewTopSettingsRow.updateQueryModeRadioButtonGroup(queryMode)
                queryViewTopSettingsRow.updateReferenceSetRadioButtonGroup(referenceSet)
                if (queryMode == QueryViewTopSettingsRow.QueryMode.INDIVIDUAL_DATA)
                    queryViewTopSettingsRow.updatePreviewRowsIntegerField(previewRows)
                queryViewTopSettingsRow.updateFurtherSettingsCheckboxGroup(queryFurtherSettings)
                queryViewAttributesContainer.resetAttributeSectionsForQueryMode(queryMode, queryAttributesRows)

                close()
                QueryAttributesCustomField.forceUIResize()
            }
        }
        loadThread.start()
    }

    private fun createCancelButton(): Button {
        val cancelIcon = FontIcon(FONT_ICON_FAMILY, "fa-ban")
        val cancelButton = Button(getTranslation("web-padawan.settings.cancel"), cancelIcon)
        cancelButton.addClickListener {
            close()
        }
        return cancelButton
    }
}