package de.uniwue.misbased.vaadin_padawan

import com.vaadin.flow.component.UI
import com.vaadin.flow.component.button.ButtonVariant
import com.vaadin.flow.component.confirmdialog.ConfirmDialog
import com.vaadin.flow.server.ErrorEvent
import com.vaadin.flow.server.ErrorHandler
import de.uniwue.misbased.vaadin_padawan.views.query.QueryAttributesCustomField
import org.slf4j.LoggerFactory

class DefaultErrorHandler : ErrorHandler {

    private val usableLogger = LoggerFactory.getLogger(DefaultErrorHandler::class.java)

    companion object {
        fun createErrorDialog(
            ui: UI,
            errorTitleKey: String,
            errorMessageKey: String? = null,
            errorMessage: String? = null,
            reloadPageOnErrorConfirm: Boolean = false
        ): ConfirmDialog {
            val errorConfirmDialog = ConfirmDialog()
            errorConfirmDialog.isCloseOnEsc = false
            errorConfirmDialog.setHeader(ui.getTranslation(errorTitleKey))
            var dialogContent = ui.getTranslation(errorMessageKey ?: "web-padawan.errorDialog.defaultContent")
            if (errorMessage != null) {
                dialogContent += " " + ui.getTranslation(
                    "web-padawan.errorDialog.contentErrorTitleAppendix",
                    errorMessage
                )
            }
            errorConfirmDialog.setText(dialogContent)

            errorConfirmDialog.setConfirmText(ui.getTranslation("web-padawan.errorDialog.ok"))
            errorConfirmDialog.setConfirmButtonTheme(ButtonVariant.LUMO_ERROR.variantName)
            errorConfirmDialog.addConfirmListener {
                if (reloadPageOnErrorConfirm)
                    ui.page.reload()
                else
                    QueryAttributesCustomField.forceUIResize()
            }

            return errorConfirmDialog
        }
    }

    override fun error(event: ErrorEvent?) {
        if (event != null) {
            usableLogger.error("An unhandled error occurred", event.throwable)
            val ui = UI.getCurrent()
            ui?.access {
                val errorConfirmDialog = createErrorDialog(
                    ui = ui,
                    errorTitleKey = "web-padawan.errorDialog.title",
                    errorMessage = event.throwable?.localizedMessage,
                    reloadPageOnErrorConfirm = true
                )
                errorConfirmDialog.open()
                QueryAttributesCustomField.forceUIResize()
            }
        }
    }
}