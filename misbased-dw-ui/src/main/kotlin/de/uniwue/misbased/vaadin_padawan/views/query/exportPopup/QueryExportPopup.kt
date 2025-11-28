package de.uniwue.misbased.vaadin_padawan.views.query.exportPopup

import com.vaadin.flow.component.Text
import com.vaadin.flow.component.UI
import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.checkbox.Checkbox
import com.vaadin.flow.component.dialog.Dialog
import com.vaadin.flow.component.html.Anchor
import com.vaadin.flow.component.icon.FontIcon
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.component.popover.Popover
import com.vaadin.flow.component.popover.PopoverVariant
import com.vaadin.flow.data.binder.Binder
import com.vaadin.flow.data.binder.Setter
import com.vaadin.flow.function.ValueProvider
import com.vaadin.flow.server.InputStreamFactory
import com.vaadin.flow.server.StreamResource
import de.uniwue.dw.core.client.authentication.User
import de.uniwue.dw.query.model.result.export.ExportType
import de.uniwue.misbased.vaadin_padawan.DefaultErrorHandler
import de.uniwue.misbased.vaadin_padawan.data.FONT_ICON_FAMILY
import de.uniwue.misbased.vaadin_padawan.data.model.ExportConfiguration
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributesRowContainer
import de.uniwue.misbased.vaadin_padawan.data.model.QueryExecutionState
import de.uniwue.misbased.vaadin_padawan.data.model.QueryState
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNExportConnector
import de.uniwue.misbased.vaadin_padawan.views.query.QueryAttributesCustomField
import de.uniwue.misbased.vaadin_padawan.views.query.QuerySaveDialog
import de.uniwue.misbased.vaadin_padawan.views.query.QueryViewBottomActionButtonsRow
import de.uniwue.misbased.vaadin_padawan.views.query.QueryViewTopSettingsRow
import java.io.FileInputStream


class QueryExportPopup(
    private val queryViewTopSettingsRow: QueryViewTopSettingsRow,
    private val queryData: List<QueryAttributesRowContainer>,
    private val downloadAnchor: Anchor,
    exportType: ExportType,
    parentLayout: QueryViewBottomActionButtonsRow
) : Popover() {

    init {
        isAutofocus = true
        isCloseOnEsc = false
        isCloseOnOutsideClick = true
        isModal = true
        isBackdropVisible = true
        addThemeVariants(PopoverVariant.ARROW)

        val binder = Binder(ExportConfiguration::class.java)
        binder.bean = ExportConfiguration(exportType)

        val contentLayout = if (exportType == ExportType.EXCEL)
            QueryExportPopupContentExcel(binder)
        else
            QueryExportPopupContentCSV(binder)

        val bottomButtonsLayout = createBottomButtonsLayout(binder)
        bottomButtonsLayout.setWidthFull()
        contentLayout.add(bottomButtonsLayout)

        add(contentLayout)

        addOpenedChangeListener {
            if (!it.isOpened)
                parentLayout.remove(this)
            QueryAttributesCustomField.forceUIResize()
        }
    }

    companion object {
        fun createCheckbox(
            binder: Binder<ExportConfiguration>,
            labelKey: String,
            getter: ValueProvider<ExportConfiguration, Boolean>,
            setter: Setter<ExportConfiguration, Boolean>,
            parentLayout: VerticalLayout
        ): Checkbox {
            val shortenTextCheckbox = Checkbox(parentLayout.getTranslation(labelKey))

            binder
                .forField(shortenTextCheckbox)
                .bind(getter, setter)
            return shortenTextCheckbox
        }
    }

    private fun createBottomButtonsLayout(binder: Binder<ExportConfiguration>): HorizontalLayout {
        val bottomButtonsLayout = HorizontalLayout()
        val exportButton = createExportButton(binder)
        val cancelButton = createCancelButton()
        bottomButtonsLayout.addAndExpand(exportButton, cancelButton)
        return bottomButtonsLayout
    }

    private fun createExportButton(binder: Binder<ExportConfiguration>): Button {
        val exportIcon = FontIcon(FONT_ICON_FAMILY, "fa-check")
        val exportButton = Button(getTranslation("web-padawan.exportActions.exportDialog.start"), exportIcon)
        exportButton.addClickListener {
            if (binder.validate().isOk) {
                close()
                val exportRunningDialog = QueryExportRunningDialog()

                val query = QuerySaveDialog.createMXQLQuery(queryData, queryViewTopSettingsRow)

                val ui = UI.getCurrent()
                val user = PaDaWaNConnector.getUser()
                val exportThread = Thread {
                    while (isAttached) {
                        // wait until the popup is no longer attached to the UI,
                        // because otherwise opening the exportRunningDialog will prevent the popup from detaching
                        Thread.sleep(1)
                    }
                    ui.access {
                        exportRunningDialog.open()
                        QueryAttributesCustomField.forceUIResize()
                    }

                    var queryState = PaDaWaNExportConnector.startExport(query, binder.bean, user)
                    while (queryState.queryState == QueryExecutionState.RUNNING) {
                        ui.access {
                            exportRunningDialog.updateQueryState(queryState)
                        }
                        Thread.sleep(queryState.nextStatusQueryIntervalInMS.toLong())
                        if (exportRunningDialog.isOpened)
                            queryState = PaDaWaNExportConnector.exportStatus(queryState.queryID, user)
                        else
                            break
                    }

                    if (exportRunningDialog.isOpened)
                        ui.access {
                            exportRunningDialog.updateQueryState(queryState)
                            exportRunningDialog.close()

                            if (queryState.queryState == QueryExecutionState.ERROR) {
                                val exportErrorDialog = DefaultErrorHandler.createErrorDialog(
                                    ui = ui,
                                    errorTitleKey = "web-padawan.exportActions.exportDialog.error",
                                    errorMessageKey = queryState.errorMessageTranslationKey,
                                    errorMessage = queryState.errorMessage
                                )
                                exportErrorDialog.open()
                            } else if (queryState.queryState == QueryExecutionState.FINISHED_SUCCESSFULLY) {
                                val exportDownloadDialog = createExportDownloadDialog(queryState, user)
                                exportDownloadDialog.open()
                            }

                            QueryAttributesCustomField.forceUIResize()
                        }
                }
                exportThread.start()
            }
        }
        return exportButton
    }

    private fun createExportDownloadDialog(queryState: QueryState, user: User): Dialog {
        val exportDownloadDialog = Dialog()
        exportDownloadDialog.isCloseOnEsc = false
        exportDownloadDialog.isCloseOnOutsideClick = false

        exportDownloadDialog.headerTitle = getTranslation("web-padawan.exportActions.exportDialog.success")
        val dialogContentText = Text(
            getTranslation("web-padawan.exportActions.exportDialog.download", queryState.statusMessage)
        )
        exportDownloadDialog.add(dialogContentText)

        val footerLayout = HorizontalLayout()
        val cancelButton = Button(getTranslation("web-padawan.queryActions.save.no")) {
            exportDownloadDialog.close()
            QueryAttributesCustomField.forceUIResize()
        }
        footerLayout.addAndExpand(cancelButton)

        val exportFile = PaDaWaNExportConnector.getExportResult(queryState.queryID, user)
        val confirmButton = Button(getTranslation("web-padawan.queryActions.save.yes")) {
            if (exportFile != null)
                downloadAnchor.element.callJsFunction("click")
            exportDownloadDialog.close()
            QueryAttributesCustomField.forceUIResize()
        }
        if (exportFile != null) {
            val streamResource = StreamResource(exportFile.name, InputStreamFactory { FileInputStream(exportFile) })
            downloadAnchor.setHref(streamResource)
        }
        footerLayout.addAndExpand(confirmButton)

        exportDownloadDialog.footer.add(footerLayout)

        return exportDownloadDialog
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