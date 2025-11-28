package de.uniwue.misbased.vaadin_padawan.views.query

import com.vaadin.flow.component.Component
import com.vaadin.flow.component.Text
import com.vaadin.flow.component.UI
import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.button.ButtonVariant
import com.vaadin.flow.component.confirmdialog.ConfirmDialog
import com.vaadin.flow.component.html.Anchor
import com.vaadin.flow.component.icon.FontIcon
import com.vaadin.flow.component.notification.Notification
import com.vaadin.flow.component.notification.NotificationVariant
import com.vaadin.flow.component.orderedlayout.FlexComponent
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.dom.Style
import com.vaadin.flow.theme.lumo.LumoUtility
import de.uniwue.dw.query.model.result.export.ExportConfiguration
import de.uniwue.dw.query.model.result.export.ExportType
import de.uniwue.misbased.vaadin_padawan.data.*
import de.uniwue.misbased.vaadin_padawan.data.imageConnector.ImageConnector
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributesRowContainer
import de.uniwue.misbased.vaadin_padawan.data.model.QueryExecutionState
import de.uniwue.misbased.vaadin_padawan.data.pacsConnector.PACSConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNQueryConnector
import de.uniwue.misbased.vaadin_padawan.views.query.exportPopup.QueryExportPopup
import de.uniwue.misbased.vaadin_padawan.views.query.images.QueryViewImagesActionButtonsContainer
import de.uniwue.misbased.vaadin_padawan.views.result.ResultView

class QueryViewBottomActionButtonsRow(
    private val queryViewTopSettingsRow: QueryViewTopSettingsRow,
    private val queryViewAttributesContainer: QueryViewAttributesContainer,
    private val queryViewImagesActionButtonsContainer: QueryViewImagesActionButtonsContainer,
    private val resultView: ResultView
) : HorizontalLayout() {

    private lateinit var searchButton: Button
    private lateinit var saveButton: Button
    private lateinit var loadButton: Button
    private lateinit var deleteButton: Button

    private lateinit var excelExportButton: Button
    private lateinit var csvExportButton: Button

    private val excelExportButtonID = "excelExportButton"
    private val csvExportButtonID = "csvExportButton"
    private val downloadAnchor: Anchor

    init {
        addClassNames(
            LumoUtility.Border.ALL,
            LumoUtility.BorderColor.CONTRAST_50,
            LumoUtility.Padding.MEDIUM
        )

        downloadAnchor = Anchor()
        downloadAnchor.element.style.setDisplay(Style.Display.NONE)
        downloadAnchor.element.setAttribute("download", true)

        val queryActionsMultipleButtonsCustomField = createQueryActionsMultipleComponentsCustomField()
        val exportActionsMultipleButtonsCustomField = createExportActionsMultipleComponentsCustomField()
        add(queryActionsMultipleButtonsCustomField, exportActionsMultipleButtonsCustomField)
    }

    companion object {
        fun createNotification(
            messageTranslationKey: String,
            themeVariant: NotificationVariant? = null,
            notificationPosition: Notification.Position = Notification.Position.MIDDLE,
            notificationDuration: Int = 3000
        ): Notification {
            val notification = Notification()
            themeVariant?.let { notification.addThemeVariants(it) }

            val errorMessage = Text(UI.getCurrent().getTranslation(messageTranslationKey))
            val closeIcon = FontIcon(FONT_ICON_FAMILY, "fa-times")
            val closeButton = Button(closeIcon)
            closeButton.addThemeVariants(ButtonVariant.LUMO_TERTIARY_INLINE)
            closeButton.addClickListener { notification.close() }

            val contentLayout = HorizontalLayout(errorMessage, closeButton)
            contentLayout.alignItems = FlexComponent.Alignment.CENTER

            notification.add(contentLayout)
            notification.position = notificationPosition
            notification.duration = notificationDuration

            return notification
        }

        fun createButtonWithIconAndTitle(
            iconName: String,
            title: String,
            buttonID: String? = null,
            onClick: () -> Unit = {}
        ): Button {
            val icon = FontIcon(FONT_ICON_FAMILY, iconName)
            val button = Button(title, icon)
            if (buttonID != null) button.setId(buttonID)
            button.addClickListener { onClick() }
            return button
        }

        fun checkQueryNotEmptyAndPerformAction(
            queryViewTopSettingsRow: QueryViewTopSettingsRow,
            action: (queryData: List<QueryAttributesRowContainer>) -> Unit
        ) {
            val queryMode = queryViewTopSettingsRow.getCurrentQueryMode()
            val queryData = mutableListOf<QueryAttributesRowContainer>()
            when (queryMode) {
                QueryViewTopSettingsRow.QueryMode.INDIVIDUAL_DATA -> {
                    val individualDataQuery =
                        QueryAttributesCustomField.getAttributesRowContainer(
                            SESSION_ATTRIBUTE_QUERY_ROWS_INDIVIDUAL_DATA,
                            queryViewTopSettingsRow
                        )
                    queryData.add(individualDataQuery)
                }

                QueryViewTopSettingsRow.QueryMode.STATISTICS -> {
                    val statisticsRowsQuery =
                        QueryAttributesCustomField.getAttributesRowContainer(
                            SESSION_ATTRIBUTE_QUERY_ROWS,
                            queryViewTopSettingsRow
                        )
                    val statisticsColumnsQuery =
                        QueryAttributesCustomField.getAttributesRowContainer(
                            SESSION_ATTRIBUTE_QUERY_COLUMNS,
                            queryViewTopSettingsRow
                        )
                    val statisticsFilterQuery =
                        QueryAttributesCustomField.getAttributesRowContainer(
                            SESSION_ATTRIBUTE_QUERY_FILTERS,
                            queryViewTopSettingsRow
                        )
                    queryData.addAll(listOf(statisticsRowsQuery, statisticsColumnsQuery, statisticsFilterQuery))
                }
            }
            if (queryData.isNotEmpty()) {
                val queryContainsAtLeastOneAttribute = queryData.any { queryAttributesRowContainer ->
                    queryAttributesRowContainer.attributeRows.any { queryAttributesRow ->
                        queryAttributesRow.attributes.isNotEmpty()
                    }
                }

                if (queryContainsAtLeastOneAttribute.not()) {
                    val notification = createNotification(
                        "web-padawan.queryActions.error.emptyQuery",
                        NotificationVariant.LUMO_ERROR
                    )
                    notification.open()
                } else {
                    action(queryData)
                }
            }
        }

        fun getMultipleComponentsCustomField(
            title: String,
            components: List<Component>,
            withLeftBorder: Boolean,
            downloadAnchor: Anchor? = null
        ): MultipleComponentsCustomField {
            val multipleComponentsCustomField = MultipleComponentsCustomField(components, downloadAnchor)
            multipleComponentsCustomField.label = title

            if (withLeftBorder)
                multipleComponentsCustomField.addClassNames(
                    LumoUtility.Border.LEFT,
                    LumoUtility.BorderColor.CONTRAST_30,
                    LumoUtility.Padding.Start.MEDIUM
                )

            return multipleComponentsCustomField
        }
    }

    private fun createQueryActionsMultipleComponentsCustomField(): MultipleComponentsCustomField {
        searchButton = createButtonWithIconAndTitle(
            "fa-search",
            getTranslation("web-padawan.queryActions.search"),
            onClick = ::doSearchAction
        )
        saveButton = createButtonWithIconAndTitle(
            "fa-save",
            getTranslation("web-padawan.queryActions.save"),
            onClick = ::doSaveAction
        )
        loadButton = createButtonWithIconAndTitle(
            "fa-folder-open",
            getTranslation("web-padawan.queryActions.load"),
            onClick = ::doLoadAction
        )
        deleteButton = createButtonWithIconAndTitle(
            "fa-trash",
            getTranslation("web-padawan.queryActions.delete"),
            onClick = ::doDeleteAction
        )

        val queryActionButtons = listOf(searchButton, saveButton, loadButton, deleteButton)
        return getMultipleComponentsCustomField(
            getTranslation("web-padawan.queryActions.title"),
            queryActionButtons,
            false
        )
    }

    private fun doSearchAction() {
        checkQueryNotEmptyAndPerformAction(queryViewTopSettingsRow) { queryData ->
            if (searchButton.text == getTranslation("web-padawan.queryActions.search"))
                startSearch(queryData)
            else
                cancelSearch()
            QueryAttributesCustomField.forceUIResize()
        }
    }

    private fun startSearch(queryData: List<QueryAttributesRowContainer>) {
        searchButton.text = getTranslation("web-padawan.settings.cancel")
        val cancelIcon = FontIcon(FONT_ICON_FAMILY, "fa-ban")
        searchButton.icon = cancelIcon
        disableNonSearchButtons()

        resultView.startQueryExecution()

        val query = QuerySaveDialog.createMXQLQuery(queryData, queryViewTopSettingsRow)
        val mode = if (query.isStatisticQuery)
            QueryViewTopSettingsRow.QueryMode.STATISTICS
        else
            QueryViewTopSettingsRow.QueryMode.INDIVIDUAL_DATA
        val referenceSet = queryViewTopSettingsRow.getCurrentReferenceSet(mode)
        val referenceSetName = queryViewTopSettingsRow.getReferenceSetName(referenceSet)

        val ui = UI.getCurrent()
        val user = PaDaWaNConnector.getUser()

        val exportConfiguration = ExportConfiguration(ExportType.GUI)
        exportConfiguration.statisticsAllColumnName = getTranslation("web-padawan.queryResult.statistics.allColumnName")
        exportConfiguration.statisticsSumColumnName = getTranslation("web-padawan.queryResult.statistics.sumColumnName")
        exportConfiguration.statisticsDuplicatesColumnName =
            getTranslation("web-padawan.queryResult.statistics.duplicatesColumnName")
        exportConfiguration.filterIDTypePatientsName = getTranslation("web-padawan.referenceSet.patients")
        exportConfiguration.filterIDTypeCasesName = getTranslation("web-padawan.referenceSet.cases")
        exportConfiguration.filterIDTypeDocumentsName = getTranslation("web-padawan.referenceSet.documents")
        exportConfiguration.filterIDTypeGroupedByName =
            getTranslation("web-padawan.queryResult.statistics.groupedByFilterName")
        exportConfiguration.filterIDTypeGroupedByPatientsName =
            getTranslation("web-padawan.queryResult.statistics.groupedByPatientsName")
        exportConfiguration.filterIDTypeGroupedByCasesName =
            getTranslation("web-padawan.queryResult.statistics.groupedByCasesName")
        exportConfiguration.filterIDTypeGroupedByDocumentsName =
            getTranslation("web-padawan.queryResult.statistics.groupedByDocumentsName")

        val searchThread = Thread {
            var queryState = PaDaWaNQueryConnector.startQuery(query, user, exportConfiguration)
            while (queryState.queryState == QueryExecutionState.RUNNING) {
                ui.access {
                    resultView.updateQueryProgress(queryState.progress)
                }
                Thread.sleep(queryState.nextStatusQueryIntervalInMS.toLong())
                if (getQueryExecutionCanceled(ui)) {
                    val cancelThread = Thread {
                        PaDaWaNQueryConnector.cancelQuery(queryState.queryID, user)
                    }
                    cancelThread.start()
                    break
                } else
                    queryState = PaDaWaNQueryConnector.queryStatus(queryState.queryID, user)
            }

            if (!getQueryExecutionCanceled(ui)) {
                val queryResult = if (queryState.queryState == QueryExecutionState.FINISHED_SUCCESSFULLY)
                    PaDaWaNQueryConnector.queryResult(queryState.queryID, user)
                else
                    null
                val pacsSessionKey = if (queryResult != null && !query.isStatisticQuery)
                    PACSConnector.getSessionKeyAndAccessionNumbersForQueryResult(queryResult, user).first
                else
                    null
                val resultContainsImageColumn = if (queryResult != null && !query.isStatisticQuery)
                    ImageConnector.resultContainsImageColumn(queryResult)
                else
                    false
                ui.access {
                    resultView.updateQueryProgress(queryState.progress)

                    if (queryState.queryState == QueryExecutionState.ERROR)
                        if (queryState.errorMessageTranslationKey != null)
                            resultView.displayQueryExecutionError(getTranslation(queryState.errorMessageTranslationKey))
                        else
                            resultView.displayQueryExecutionError(queryState.errorMessage ?: "")
                    else if (queryState.queryState == QueryExecutionState.FINISHED_SUCCESSFULLY) {
                        resultView.displayQueryResults(
                            queryResult,
                            query.isStatisticQuery,
                            referenceSetName,
                            pacsSessionKey
                        )
                        if (pacsSessionKey != null || resultContainsImageColumn)
                            queryViewImagesActionButtonsContainer.enableImagesDownloadAllButton()
                    }

                    resetSearchButton()
                    QueryAttributesCustomField.forceUIResize()
                }
            }
        }
        searchThread.start()
    }

    private fun getQueryExecutionCanceled(ui: UI): Boolean {
        var queryExecutionCanceled = false
        ui.access {
            queryExecutionCanceled = resultView.queryExecutionCancelled()
        }
        return queryExecutionCanceled
    }

    private fun disableNonSearchButtons() {
        saveButton.isEnabled = false
        loadButton.isEnabled = false
        deleteButton.isEnabled = false
        excelExportButton.isEnabled = false
        csvExportButton.isEnabled = false
        queryViewImagesActionButtonsContainer.disableImagesDownloadAllButton()
    }

    private fun enableNonSearchButtons() {
        saveButton.isEnabled = true
        loadButton.isEnabled = true
        deleteButton.isEnabled = true
        excelExportButton.isEnabled = true
        csvExportButton.isEnabled = true
    }

    private fun cancelSearch() {
        resetSearchButton()

        resultView.cancelQueryExecution()
    }

    private fun resetSearchButton() {
        searchButton.text = getTranslation("web-padawan.queryActions.search")
        val searchIcon = FontIcon(FONT_ICON_FAMILY, "fa-search")
        searchButton.icon = searchIcon
        enableNonSearchButtons()
    }

    private fun doSaveAction() {
        checkQueryNotEmptyAndPerformAction(queryViewTopSettingsRow) { queryData ->
            val querySaveDialog = QuerySaveDialog(queryViewTopSettingsRow, queryData)
            querySaveDialog.open()
            QueryAttributesCustomField.forceUIResize()
        }
    }

    private fun doLoadAction() {
        val queryLoadDialog = QueryLoadDialog(queryViewTopSettingsRow, queryViewAttributesContainer)
        queryLoadDialog.open()
        QueryAttributesCustomField.forceUIResize()
    }

    private fun doDeleteAction() {
        checkQueryNotEmptyAndPerformAction(queryViewTopSettingsRow) {
            val deleteConfirmDialog = ConfirmDialog()
            deleteConfirmDialog.isCloseOnEsc = false
            deleteConfirmDialog.setHeader(
                getTranslation("web-padawan.queryActions.delete.dialogTitle")
            )
            deleteConfirmDialog.setText(
                getTranslation("web-padawan.queryActions.delete.dialogContent")
            )

            deleteConfirmDialog.setCancelable(true)
            deleteConfirmDialog.setCancelText(getTranslation("web-padawan.queryActions.save.no"))
            deleteConfirmDialog.addCancelListener {
                QueryAttributesCustomField.forceUIResize()
            }

            deleteConfirmDialog.setConfirmText(getTranslation("web-padawan.queryActions.save.yes"))
            deleteConfirmDialog.addConfirmListener {
                val queryMode = queryViewTopSettingsRow.getCurrentQueryMode()
                queryViewAttributesContainer.resetAttributeSectionsForQueryMode(queryMode)
                val referenceSetToUSe = QueryViewTopSettingsRow.ReferenceSet.CASES
                queryViewTopSettingsRow.updateReferenceSetRadioButtonGroup(referenceSetToUSe)
                val queryFurtherSettings = queryViewTopSettingsRow.getFurtherSettings().toMutableSet()
                when (queryMode) {
                    QueryViewTopSettingsRow.QueryMode.STATISTICS -> {
                        queryFurtherSettings.remove(QueryViewTopSettingsRow.FurtherSettings.RETURN_PATIENT_COUNTS)
                    }

                    QueryViewTopSettingsRow.QueryMode.INDIVIDUAL_DATA -> {
                        queryFurtherSettings.remove(QueryViewTopSettingsRow.FurtherSettings.RETURN_ONLY_ONE_ROW_PER_PATIENT)
                        queryViewTopSettingsRow.updatePreviewRowsIntegerField(10)
                    }
                }
                queryViewTopSettingsRow.updateFurtherSettingsCheckboxGroup(queryFurtherSettings)
                QueryAttributesCustomField.forceUIResize()
            }

            deleteConfirmDialog.open()
            QueryAttributesCustomField.forceUIResize()
        }
    }

    private fun createExportActionsMultipleComponentsCustomField(): MultipleComponentsCustomField {
        excelExportButton = createButtonWithIconAndTitle(
            "fa-file-excel",
            getTranslation("web-padawan.exportActions.Excel"),
            excelExportButtonID,
            ::doExportExcelAction
        )
        csvExportButton = createButtonWithIconAndTitle(
            "fa-file-csv",
            getTranslation("web-padawan.exportActions.CSV"),
            csvExportButtonID,
            ::doExportCSVAction
        )

        val exportActionButtons = listOf(excelExportButton, csvExportButton)
        return getMultipleComponentsCustomField(
            getTranslation("web-padawan.exportActions.title"),
            exportActionButtons,
            true,
            downloadAnchor
        )
    }

    private fun doExportExcelAction() {
        checkQueryNotEmptyAndPerformAction(queryViewTopSettingsRow) { queryData ->
            val popup = QueryExportPopup(queryViewTopSettingsRow, queryData, downloadAnchor, ExportType.EXCEL, this)
            popup.`for` = excelExportButtonID
            this.add(popup)
            popup.open()
        }
    }

    private fun doExportCSVAction() {
        checkQueryNotEmptyAndPerformAction(queryViewTopSettingsRow) { queryData ->
            val popup = QueryExportPopup(queryViewTopSettingsRow, queryData, downloadAnchor, ExportType.CSV, this)
            popup.`for` = csvExportButtonID
            this.add(popup)
            popup.open()
        }
    }
}