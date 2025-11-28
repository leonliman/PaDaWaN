package de.uniwue.misbased.vaadin_padawan.views.result

import com.vaadin.flow.component.html.Div
import com.vaadin.flow.component.html.H4
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.component.progressbar.ProgressBar
import com.vaadin.flow.component.tabs.TabSheet
import com.vaadin.flow.server.VaadinSession
import com.vaadin.flow.theme.lumo.LumoUtility
import de.uniwue.dw.query.model.result.Result
import de.uniwue.misbased.vaadin_padawan.data.SESSION_ATTRIBUTE_LAST_PACS_SESSION_KEY
import de.uniwue.misbased.vaadin_padawan.data.SESSION_ATTRIBUTE_LAST_RESULT
import de.uniwue.misbased.vaadin_padawan.data.SESSION_ATTRIBUTE_LAST_RESULT_IS_FROM_STATISTIC_QUERY
import de.uniwue.misbased.vaadin_padawan.data.SESSION_ATTRIBUTE_LAST_RESULT_REFERENCE_SET_NAME
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNUserSettingsConnector

class ResultView : VerticalLayout() {

    private val tabSheet: TabSheet
    private val resultTable: ResultTable
    private val correlationTab: ResultCorrelationTab
    private val progressBar: ProgressBar
    private val footer: Div

    private var lastResultForNonSessionStorage: Result? = null

    init {
        setSizeFull()
        addClassName(LumoUtility.Gap.Row.XSMALL)

        val title = H4(getTranslation("web-padawan.resultTitle"))
        title.setWidthFull()
        title.addClassNames(
            LumoUtility.Border.BOTTOM,
            LumoUtility.BorderColor.CONTRAST_30,
            LumoUtility.Padding.Bottom.MEDIUM
        )

        tabSheet = TabSheet()
        tabSheet.setWidthFull()

        resultTable = ResultTable()
        resultTable.setHeightFull()
        tabSheet.add(getTranslation("web-padawan.resultTable.title"), resultTable)

        correlationTab = ResultCorrelationTab()
        tabSheet.add(correlationTab, correlationTab.getCorrelationTabContent())

        progressBar = ProgressBar()
        progressBar.isVisible = false

        footer = Div()
        footer.setWidthFull()
        footer.addClassNames(
            LumoUtility.Border.TOP,
            LumoUtility.BorderColor.CONTRAST_30,
            LumoUtility.Padding.Top.SMALL
        )
        footer.isVisible = false

        val lastResult = getLastResult()
        if (lastResult != null) {
            displayQueryResults(
                lastResult,
                isLastResultFromStatisticQuery(),
                getLastResultReferenceSetName(),
                getLastPacsSessionKey()
            )
        }

        add(title, tabSheet, progressBar, footer)
        expand(tabSheet)
    }

    fun startQueryExecution() {
        tabSheet.selectedIndex = 0
        correlationTab.isVisible = false
        progressBar.isIndeterminate = true
        progressBar.isVisible = true
        resultTable.updatePlaceholderText(getTranslation("web-padawan.resultTable.running"))
        footer.isVisible = false
        setLastResult(null, null, null, null)
    }

    fun updateQueryProgress(progress: Double) {
        if (progress > 0) {
            progressBar.isIndeterminate = false
            progressBar.value = progress
        } else
            progressBar.isIndeterminate = true
    }

    fun cancelQueryExecution() {
        progressBar.isVisible = false
        resultTable.updatePlaceholderText(getTranslation("web-padawan.resultTable.canceled"))
    }

    fun displayQueryExecutionError(errorMessage: String) {
        progressBar.isVisible = false
        resultTable.updatePlaceholderText(getTranslation("web-padawan.resultTable.error"), errorMessage)
    }

    fun displayQueryResults(
        result: Result?,
        isStatisticQuery: Boolean,
        referenceSetName: String,
        pacsSessionKey: String?
    ) {
        progressBar.isVisible = false
        resultTable.displayQueryResults(result, isStatisticQuery, pacsSessionKey)
        if (result != null) {
            footer.isVisible = true
            val queryTime = result.queryTime
            val numResults = result.docsFound
            if (isStatisticQuery)
                footer.text = getTranslation("web-padawan.resultFooter.statisticResult", queryTime)
            else
                footer.text = getTranslation(
                    "web-padawan.resultFooter.patientCaseResult",
                    numResults,
                    referenceSetName,
                    queryTime
                )

            if (result.correlationWithPValues != null && result.correlationWithPValues.isNotEmpty())
                correlationTab.displayCorrelationQueryResults(result, referenceSetName)
        }
        setLastResult(result, isStatisticQuery, referenceSetName, pacsSessionKey)
    }

    fun getLastResult(useNonSessionStorageAsFallback: Boolean = false): Result? {
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        return if (userSettings.userInterfaceUseSessionStorage)
            VaadinSession.getCurrent().getAttribute(SESSION_ATTRIBUTE_LAST_RESULT) as? Result
        else if (useNonSessionStorageAsFallback)
            lastResultForNonSessionStorage
        else
            null
    }

    fun isLastResultFromStatisticQuery(): Boolean {
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        return if (userSettings.userInterfaceUseSessionStorage)
            VaadinSession.getCurrent()
                .getAttribute(SESSION_ATTRIBUTE_LAST_RESULT_IS_FROM_STATISTIC_QUERY) as? Boolean == true
        else
            false
    }

    private fun getLastResultReferenceSetName(): String {
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        return if (userSettings.userInterfaceUseSessionStorage)
            VaadinSession.getCurrent().getAttribute(SESSION_ATTRIBUTE_LAST_RESULT_REFERENCE_SET_NAME) as? String ?: ""
        else
            ""
    }

    private fun getLastPacsSessionKey(): String? {
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        return if (userSettings.userInterfaceUseSessionStorage)
            VaadinSession.getCurrent().getAttribute(SESSION_ATTRIBUTE_LAST_PACS_SESSION_KEY) as? String
        else
            null
    }

    private fun setLastResult(
        result: Result?,
        isStatisticQuery: Boolean?,
        referenceSetName: String?,
        pacsSessionKey: String?
    ) {
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        if (userSettings.userInterfaceUseSessionStorage) {
            VaadinSession.getCurrent().setAttribute(SESSION_ATTRIBUTE_LAST_RESULT, result)
            VaadinSession.getCurrent()
                .setAttribute(SESSION_ATTRIBUTE_LAST_RESULT_IS_FROM_STATISTIC_QUERY, isStatisticQuery)
            VaadinSession.getCurrent().setAttribute(SESSION_ATTRIBUTE_LAST_RESULT_REFERENCE_SET_NAME, referenceSetName)
            VaadinSession.getCurrent().setAttribute(SESSION_ATTRIBUTE_LAST_PACS_SESSION_KEY, pacsSessionKey)
        } else
            lastResultForNonSessionStorage = result
    }

    fun queryExecutionCancelled(): Boolean {
        return resultTable.queryExecutionCancelled()
    }
}