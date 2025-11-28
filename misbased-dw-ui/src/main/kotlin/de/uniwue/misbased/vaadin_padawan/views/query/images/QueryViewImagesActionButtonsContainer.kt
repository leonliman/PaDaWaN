package de.uniwue.misbased.vaadin_padawan.views.query.images

import com.vaadin.flow.component.UI
import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.theme.lumo.LumoUtility
import de.uniwue.dw.core.client.authentication.User
import de.uniwue.misbased.vaadin_padawan.data.imageConnector.ImageConnector
import de.uniwue.misbased.vaadin_padawan.data.model.PACSUserSettings
import de.uniwue.misbased.vaadin_padawan.data.pacsConnector.PACSConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.views.query.QueryAttributesCustomField
import de.uniwue.misbased.vaadin_padawan.views.query.QuerySaveDialog
import de.uniwue.misbased.vaadin_padawan.views.query.QueryViewBottomActionButtonsRow
import de.uniwue.misbased.vaadin_padawan.views.query.QueryViewTopSettingsRow
import org.slf4j.LoggerFactory

class QueryViewImagesActionButtonsContainer(
    private val queryViewTopSettingsRow: QueryViewTopSettingsRow
) : HorizontalLayout() {

    private lateinit var imagesDownloadAllButton: Button
    private lateinit var pacsSettingsDialogButton: Button

    init {
        addClassNames(
            LumoUtility.Border.ALL,
            LumoUtility.BorderColor.CONTRAST_50,
            LumoUtility.Padding.MEDIUM
        )

        val pacsActionButtonsCustomField = createPACSActionButtonsCustomField()

        add(pacsActionButtonsCustomField)
    }

    companion object {
        private val usableLogger = LoggerFactory.getLogger(QueryViewImagesActionButtonsContainer::class.java)
    }

    private fun createPACSActionButtonsCustomField(): QueryViewImagesActionButtons {
        imagesDownloadAllButton = QueryViewBottomActionButtonsRow.createButtonWithIconAndTitle(
            "fa-download",
            getTranslation("web-padawan.images.buttons.downloadAllPictures")
        )
        imagesDownloadAllButton.addClickListener {
            QueryViewBottomActionButtonsRow.checkQueryNotEmptyAndPerformAction(queryViewTopSettingsRow) { queryData ->
                val isInPACSMode = pacsSettingsDialogButton.isVisible
                val preDownloadStepTranslationKey = if (isInPACSMode)
                    "web-padawan.pacs.download.receiveAccessionNumbers"
                else
                    "web-padawan.images.download.receiveImageIDs"
                val downloadRunningDialog = ImagesDownloadRunningDialog(preDownloadStepTranslationKey)
                downloadRunningDialog.open()
                QueryAttributesCustomField.forceUIResize()

                val query = QuerySaveDialog.createMXQLQuery(queryData, queryViewTopSettingsRow)

                val ui = UI.getCurrent()
                val user = PaDaWaNConnector.getUser()
                val pacsUserSettings = if (isInPACSMode) PACSConnector.getPACSSettings(user) else null
                val prepareDownloadThread = Thread {
                    val startTime = System.currentTimeMillis()
                    val preDownloadStepResult = if (isInPACSMode)
                        PACSConnector.getDownloadAllURL(user, query)
                    else
                        ImageConnector.getImageIDsForCompleteDownload(user, query)
                    val endTime = System.currentTimeMillis()
                    usableLogger.info("Time for querying the image identifiers: ${endTime - startTime}ms")
                    if (downloadRunningDialog.isOpened)
                        if (preDownloadStepResult.first)
                            performImagesDownload(
                                ui,
                                user,
                                pacsUserSettings,
                                downloadRunningDialog,
                                if (isInPACSMode) preDownloadStepResult.second as PACSConnector.PACSDownloadAllData else null,
                                if (!isInPACSMode) preDownloadStepResult.second as ImageConnector.ImageIDsForDownload else null
                            )
                        else
                            ui.access {
                                downloadRunningDialog.setError(preDownloadStepResult.second as String, null)
                            }
                }
                prepareDownloadThread.start()
            }
        }

        pacsSettingsDialogButton = QueryViewBottomActionButtonsRow.createButtonWithIconAndTitle(
            "fa-cog",
            getTranslation("web-padawan.pacs.buttons.settings")
        )
        pacsSettingsDialogButton.addClickListener {
            val pacsSettingsDialog = PACSSettingsDialog()
            pacsSettingsDialog.open()
            QueryAttributesCustomField.forceUIResize()
        }

        return QueryViewImagesActionButtons(imagesDownloadAllButton, pacsSettingsDialogButton, this)
    }

    private fun performImagesDownload(
        ui: UI,
        user: User,
        pacsUserSettings: PACSUserSettings?,
        downloadRunningDialog: ImagesDownloadRunningDialog,
        pacsDownloadAllData: PACSConnector.PACSDownloadAllData?,
        imageIDsForDownload: ImageConnector.ImageIDsForDownload?
    ) {
        val baseIDs = pacsDownloadAllData?.accessionNumbers ?: imageIDsForDownload!!.imageIDs
        val idsToExclude = ImageConnector.getDownloadIDsToExclude()
        ui.access {
            val queryViewImagesExportConfigDialog = QueryViewImagesExportConfigDialog(
                baseIDs,
                idsToExclude,
                {
                    downloadRunningDialog.close()
                    QueryAttributesCustomField.forceUIResize()
                },
                { idsToDownload ->
                    ui.access {
                        downloadRunningDialog.updateTitle("web-padawan.images.download.running")
                        QueryAttributesCustomField.forceUIResize()
                    }
                    val downloadThread = Thread {
                        val startTime = System.currentTimeMillis()
                        val downloadResult = if (pacsDownloadAllData != null) {
                            val downloadURL = PACSConnector.getDownloadURL(
                                user.username,
                                idsToDownload,
                                pacsDownloadAllData.sessionKey,
                                pacsUserSettings!!
                            )
                            PACSConnector.performPACSDownload(downloadURL)
                        } else
                            ImageConnector.performImagesDownload(idsToDownload)
                        val endTime = System.currentTimeMillis()
                        usableLogger.info("Time for downloading the image(s): ${endTime - startTime}ms")

                        if (downloadRunningDialog.isOpened)
                            if (downloadResult.first)
                                ui.access {
                                    downloadRunningDialog.setSuccess(downloadResult.second!!, pacsUserSettings)
                                }
                            else
                                ui.access {
                                    downloadRunningDialog.setError(null, downloadResult.second)
                                }
                    }
                    downloadThread.start()
                }
            )
            queryViewImagesExportConfigDialog.open()
            QueryAttributesCustomField.forceUIResize()
        }
    }

    fun enableImagesDownloadAllButton() {
        imagesDownloadAllButton.isEnabled = true
    }

    fun disableImagesDownloadAllButton() {
        imagesDownloadAllButton.isEnabled = false
    }
}