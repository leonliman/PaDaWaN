package de.uniwue.misbased.vaadin_padawan.views.query.exportPopup

import com.vaadin.flow.component.confirmdialog.ConfirmDialog
import com.vaadin.flow.component.progressbar.ProgressBar
import de.uniwue.misbased.vaadin_padawan.data.model.QueryState
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNExportConnector
import de.uniwue.misbased.vaadin_padawan.views.query.QueryAttributesCustomField

class QueryExportRunningDialog : ConfirmDialog() {

    private val progressBar: ProgressBar
    private var queryState: QueryState? = null

    init {
        isCloseOnEsc = false

        setHeader(getTranslation("web-padawan.exportActions.exportDialog.running"))
        progressBar = ProgressBar()
        progressBar.isIndeterminate = true
        setText(progressBar)

        setConfirmText(getTranslation("web-padawan.settings.cancel"))
        addConfirmListener {
            if (queryState != null) {
                val user = PaDaWaNConnector.getUser()
                val cancelThread = Thread {
                    PaDaWaNExportConnector.cancelExport(queryState!!.queryID, user)
                }
                cancelThread.start()
            }
            QueryAttributesCustomField.forceUIResize()
        }
    }

    fun updateQueryState(queryState: QueryState) {
        this.queryState = queryState
        if (queryState.progress > 0) {
            progressBar.isIndeterminate = false
            progressBar.value = queryState.progress
        } else
            progressBar.isIndeterminate = true
    }
}