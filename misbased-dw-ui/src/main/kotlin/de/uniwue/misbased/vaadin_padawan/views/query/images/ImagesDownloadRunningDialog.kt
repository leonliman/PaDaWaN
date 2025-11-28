package de.uniwue.misbased.vaadin_padawan.views.query.images

import com.vaadin.flow.component.button.ButtonVariant
import com.vaadin.flow.component.confirmdialog.ConfirmDialog
import com.vaadin.flow.component.html.Div
import com.vaadin.flow.component.orderedlayout.FlexComponent
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.component.progressbar.ProgressBar
import com.vaadin.flow.dom.Style
import de.uniwue.misbased.vaadin_padawan.data.model.PACSUserSettings
import de.uniwue.misbased.vaadin_padawan.views.query.QueryAttributesCustomField

class ImagesDownloadRunningDialog(initialTitleTranslationKey: String) : ConfirmDialog() {

    private val progressBar: ProgressBar
    private val content: Div

    init {
        isCloseOnEsc = false

        setHeader(getTranslation(initialTitleTranslationKey))

        progressBar = ProgressBar()
        progressBar.isIndeterminate = true

        content = Div()
        content.style.setTextAlign(Style.TextAlign.CENTER)
        content.isVisible = false

        val container = VerticalLayout()
        container.isPadding = false
        container.isSpacing = false
        container.defaultHorizontalComponentAlignment = FlexComponent.Alignment.CENTER
        container.add(progressBar, content)
        setText(container)

        setConfirmText(getTranslation("web-padawan.settings.cancel"))
        addConfirmListener {
            // TODO actually cancel the download if the confirm text is set to "Cancel"
            QueryAttributesCustomField.forceUIResize()
        }
    }

    fun updateTitle(titleTranslationKey: String) {
        setHeader(getTranslation(titleTranslationKey))
    }

    fun setSuccess(path: String, pacsUserSettings: PACSUserSettings?) {
        width = "40%"
        progressBar.isVisible = false
        updateTitle("web-padawan.images.download.success")
        var contentHTML = getTranslation("web-padawan.images.download.success.content", path)
        if (pacsUserSettings != null)
            contentHTML += getTranslation(
                "web-padawan.pacs.download.success.contentSuffix",
                pacsUserSettings.exportType.name.lowercase()
            )
        content.element.setProperty("innerHTML", contentHTML)
        content.isVisible = true
        setConfirmText(getTranslation("web-padawan.errorDialog.ok"))
        QueryAttributesCustomField.forceUIResize()
    }

    fun setError(errorMessageKey: String?, errorMessage: String?) {
        progressBar.isVisible = false
        updateTitle("web-padawan.images.download.error")
        var dialogContent = getTranslation(errorMessageKey ?: "web-padawan.errorDialog.defaultContent")
        if (errorMessage != null)
            dialogContent += " " + getTranslation("web-padawan.errorDialog.contentErrorTitleAppendix", errorMessage)
        content.text = dialogContent
        content.isVisible = true
        setConfirmText(getTranslation("web-padawan.errorDialog.ok"))
        setConfirmButtonTheme(ButtonVariant.LUMO_ERROR.variantName)
        QueryAttributesCustomField.forceUIResize()
    }
}