package de.uniwue.misbased.vaadin_padawan.views.query.images

import com.vaadin.flow.component.UI
import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.customfield.CustomField
import com.vaadin.flow.component.html.H5
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import de.uniwue.misbased.vaadin_padawan.data.imageConnector.ImageConnector
import de.uniwue.misbased.vaadin_padawan.data.model.PACSStatus
import de.uniwue.misbased.vaadin_padawan.data.pacsConnector.PACSConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.views.query.QueryAttributesCustomField

class QueryViewImagesActionButtons(
    private val imagesDownloadAllButton: Button,
    private val pacsSettingsDialogButton: Button,
    queryViewImagesActionButtonsContainer: QueryViewImagesActionButtonsContainer
) : CustomField<String>() {

    init {
        label = getTranslation("web-padawan.pacs.title")

        val buttonsContainerLayout = HorizontalLayout()
        val placeHolderContainerLayout = VerticalLayout()
        val placeholder = H5(getTranslation("web-padawan.pacs.loading"))
        placeHolderContainerLayout.add(placeholder)
        imagesDownloadAllButton.isEnabled = false
        imagesDownloadAllButton.isVisible = false
        pacsSettingsDialogButton.isVisible = false
        buttonsContainerLayout.add(placeHolderContainerLayout, imagesDownloadAllButton, pacsSettingsDialogButton)

        add(buttonsContainerLayout)

        val ui = UI.getCurrent()
        val user = PaDaWaNConnector.getUser()
        val checkStatusThread = Thread {
            val status = PACSConnector.getPACSStatus(user)
            ui.access {
                when (status) {
                    PACSStatus.CONFIGURATION_ERROR -> {
                        if (ImageConnector.isImageConnectionWorking()) {
                            label = getTranslation("web-padawan.images.title")
                            placeHolderContainerLayout.isVisible = false
                            imagesDownloadAllButton.isVisible = true
                        } else
                            queryViewImagesActionButtonsContainer.isVisible = false
                    }

                    PACSStatus.OFFLINE -> {
                        placeholder.text = getTranslation("web-padawan.pacs.error.offline")
                    }

                    PACSStatus.ONLINE -> {
                        placeHolderContainerLayout.isVisible = false
                        imagesDownloadAllButton.isVisible = true
                        pacsSettingsDialogButton.isVisible = true
                    }
                }
                QueryAttributesCustomField.forceUIResize()
            }
        }
        checkStatusThread.start()
    }

    override fun generateModelValue(): String {
        return "" // not used
    }

    override fun setPresentationValue(newPresentationValue: String?) {
        // not used
    }
}