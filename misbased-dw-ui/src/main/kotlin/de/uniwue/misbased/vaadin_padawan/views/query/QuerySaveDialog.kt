package de.uniwue.misbased.vaadin_padawan.views.query

import com.vaadin.flow.component.UI
import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.confirmdialog.ConfirmDialog
import com.vaadin.flow.component.dialog.Dialog
import com.vaadin.flow.component.icon.FontIcon
import com.vaadin.flow.component.notification.NotificationVariant
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.component.textfield.TextField
import com.vaadin.flow.data.binder.Binder
import com.vaadin.flow.server.VaadinSession
import de.uniwue.dw.query.model.lang.*
import de.uniwue.misbased.vaadin_padawan.data.FONT_ICON_FAMILY
import de.uniwue.misbased.vaadin_padawan.data.SESSION_ATTRIBUTE_LAST_SAVED_QUERY_NAME
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributesRowContainer
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNQueryConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNUserSettingsConnector


class QuerySaveDialog(
    private val queryViewTopSettingsRow: QueryViewTopSettingsRow,
    queryData: List<QueryAttributesRowContainer>
) : Dialog() {

    init {
        width = "400px"
        isCloseOnOutsideClick = false

        headerTitle = getTranslation("web-padawan.queryActions.save.dialogTitle")

        val titleBinder = Binder(QueryTitle::class.java)
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        val lastTitle = if (userSettings.userInterfaceUseSessionStorage)
            VaadinSession.getCurrent().getAttribute(SESSION_ATTRIBUTE_LAST_SAVED_QUERY_NAME) as String?
        else
            queryViewTopSettingsRow.getLastUsedQueryTitleForNonSessionStorage()
        titleBinder.bean = QueryTitle(lastTitle ?: "")

        val contentLayout = createContentLayout(titleBinder)
        contentLayout.setWidthFull()
        add(contentLayout)

        val bottomButtonsLayout = createBottomButtonsLayout(queryData, titleBinder)
        bottomButtonsLayout.setWidthFull()
        footer.add(bottomButtonsLayout)
    }

    companion object {
        fun saveLastUsedQueryName(queryName: String, queryViewTopSettingsRow: QueryViewTopSettingsRow) {
            val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
            if (userSettings.userInterfaceUseSessionStorage)
                VaadinSession.getCurrent().setAttribute(SESSION_ATTRIBUTE_LAST_SAVED_QUERY_NAME, queryName)
            else
                queryViewTopSettingsRow.updateLastUsedQueryTitleForNonSessionStorage(queryName)
        }


        fun createMXQLQuery(
            queryData: List<QueryAttributesRowContainer>,
            queryViewTopSettingsRow: QueryViewTopSettingsRow
        ): QueryRoot {
            val queryRoot = QueryRoot()

            val mode = if (queryData.singleOrNull() != null) QueryViewTopSettingsRow.QueryMode.INDIVIDUAL_DATA
            else QueryViewTopSettingsRow.QueryMode.STATISTICS

            queryRoot.isDistinct = when (mode) {
                QueryViewTopSettingsRow.QueryMode.STATISTICS -> queryViewTopSettingsRow.getFurtherSettings()
                    .contains(QueryViewTopSettingsRow.FurtherSettings.RETURN_PATIENT_COUNTS)

                QueryViewTopSettingsRow.QueryMode.INDIVIDUAL_DATA -> queryViewTopSettingsRow.getFurtherSettings()
                    .contains(QueryViewTopSettingsRow.FurtherSettings.RETURN_ONLY_ONE_ROW_PER_PATIENT)
            }

            val idFilter = QueryIDFilter(queryRoot)
            idFilter.filterIDType = when (queryViewTopSettingsRow.getCurrentReferenceSet(mode)) {
                QueryViewTopSettingsRow.ReferenceSet.PATIENTS -> QueryIDFilter.FilterIDType.PID
                QueryViewTopSettingsRow.ReferenceSet.CASES -> QueryIDFilter.FilterIDType.CaseID
                QueryViewTopSettingsRow.ReferenceSet.DOCUMENTS -> QueryIDFilter.FilterIDType.DocID
                QueryViewTopSettingsRow.ReferenceSet.GROUPS -> QueryIDFilter.FilterIDType.GROUP
            }

            when (mode) {
                QueryViewTopSettingsRow.QueryMode.STATISTICS -> {
                    if (queryData.size == 3) {
                        val queryRows = QueryStatisticRow(idFilter)
                        queryData[0].fillMXQLQuery(queryRows)

                        val queryColumns = QueryStatisticColumn(idFilter)
                        queryData[1].fillMXQLQuery(queryColumns)

                        val queryFilters = QueryStatisticFilter(idFilter)
                        queryData[2].fillMXQLQuery(queryFilters)
                    }
                }

                QueryViewTopSettingsRow.QueryMode.INDIVIDUAL_DATA -> {
                    queryRoot.limitResult = queryViewTopSettingsRow.getPreviewRows()

                    queryData.single().fillMXQLQuery(idFilter)
                }
            }

            return queryRoot
        }
    }

    private data class QueryTitle(var title: String)

    private fun createContentLayout(binder: Binder<QueryTitle>): VerticalLayout {
        val contentLayout = VerticalLayout()
        contentLayout.isPadding = false
        contentLayout.isSpacing = true

        val titleTextField = createTitleTextField(binder)
        titleTextField.setWidthFull()
        contentLayout.add(titleTextField)

        return contentLayout
    }

    private fun createTitleTextField(binder: Binder<QueryTitle>): TextField {
        val titleTextField = TextField(getTranslation("web-padawan.queryActions.save.dialogTextFieldTitle"))
        titleTextField.isAutofocus = true
        titleTextField.isClearButtonVisible = true

        val errorMessage = getTranslation("web-padawan.queryActions.save.dialogTextFieldTitle.error")
        binder.forField(titleTextField)
            .withConverter(String::trim) { it }
            .asRequired(errorMessage)
            .bind(QueryTitle::title::get, QueryTitle::title::set)
        return titleTextField
    }

    private fun createBottomButtonsLayout(
        queryData: List<QueryAttributesRowContainer>,
        binder: Binder<QueryTitle>
    ): HorizontalLayout {
        val bottomButtonsLayout = HorizontalLayout()

        val cancelButton = createCancelButton()
        val saveButton = createSaveButton(queryData, binder, cancelButton)

        bottomButtonsLayout.addAndExpand(saveButton, cancelButton)
        return bottomButtonsLayout
    }

    private fun createSaveButton(
        queryData: List<QueryAttributesRowContainer>,
        binder: Binder<QueryTitle>,
        cancelButton: Button
    ): Button {
        val saveIcon = FontIcon(FONT_ICON_FAMILY, "fa-check")
        val saveButton = Button(getTranslation("web-padawan.settings.save"), saveIcon)
        saveButton.addClickListener {
            if (binder.validate().isOk) {
                saveButton.isEnabled = false
                cancelButton.isEnabled = false
                saveButton.text = getTranslation("web-padawan.settings.save.progress")
                QueryAttributesCustomField.forceUIResize()
                val query = createMXQLQuery(queryData, queryViewTopSettingsRow)
                val queryName = binder.bean.title

                val ui = UI.getCurrent()
                val user = PaDaWaNConnector.getUser()
                val saveThread = Thread {
                    val saveQueryResult = PaDaWaNQueryConnector.saveQuery(query, queryName, user)
                    ui.access {
                        when (saveQueryResult) {
                            PaDaWaNQueryConnector.GUIQuerySaveResult.SUCCESSFUL -> {
                                showQuerySavingSuccess(queryName)
                            }

                            PaDaWaNQueryConnector.GUIQuerySaveResult.ALREADY_EXISTS -> {
                                val overwriteConfirmDialog = ConfirmDialog()
                                overwriteConfirmDialog.isCloseOnEsc = false
                                overwriteConfirmDialog.setHeader(
                                    getTranslation("web-padawan.queryActions.save.nameAlreadyExists.dialogTitle")
                                )
                                overwriteConfirmDialog.setText(
                                    getTranslation("web-padawan.queryActions.save.nameAlreadyExists.dialogContent")
                                )

                                overwriteConfirmDialog.setCancelable(true)
                                overwriteConfirmDialog.setCancelText(getTranslation("web-padawan.queryActions.save.no"))
                                overwriteConfirmDialog.addCancelListener {
                                    saveButton.isEnabled = true
                                    cancelButton.isEnabled = true
                                    saveButton.text = getTranslation("web-padawan.settings.save")
                                    QueryAttributesCustomField.forceUIResize()
                                }

                                overwriteConfirmDialog.setConfirmText(getTranslation("web-padawan.queryActions.save.yes"))
                                overwriteConfirmDialog.addConfirmListener {
                                    val subSaveThread = Thread {
                                        val overwriteSaveQueryResult =
                                            PaDaWaNQueryConnector.saveQuery(query, queryName, user, true)
                                        if (overwriteSaveQueryResult == PaDaWaNQueryConnector.GUIQuerySaveResult.SUCCESSFUL)
                                            ui.access {
                                                showQuerySavingSuccess(queryName)
                                            }
                                    }
                                    subSaveThread.start()
                                }

                                overwriteConfirmDialog.open()
                                QueryAttributesCustomField.forceUIResize()
                            }

                            PaDaWaNQueryConnector.GUIQuerySaveResult.SAVING_DISABLED -> {
                                close()
                                val errorNotification = QueryViewBottomActionButtonsRow.createNotification(
                                    "web-padawan.queryActions.save.notPossibleInDemoEnvironment",
                                    NotificationVariant.LUMO_ERROR
                                )
                                errorNotification.open()
                            }
                        }
                    }
                }
                saveThread.start()
            }
        }
        return saveButton
    }

    private fun showQuerySavingSuccess(queryName: String) {
        saveLastUsedQueryName(queryName, queryViewTopSettingsRow)
        close()
        val successNotification = QueryViewBottomActionButtonsRow.createNotification(
            "web-padawan.queryActions.save.success",
            NotificationVariant.LUMO_SUCCESS
        )
        successNotification.open()
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