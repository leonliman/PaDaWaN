package de.uniwue.misbased.vaadin_padawan.views.result

import com.vaadin.flow.component.Html
import com.vaadin.flow.component.grid.Grid
import com.vaadin.flow.component.grid.GridVariant
import com.vaadin.flow.component.html.Div
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.component.tabs.Tab
import com.vaadin.flow.data.renderer.ComponentRenderer
import de.uniwue.dw.core.client.authentication.UserInterfaceLanguage
import de.uniwue.dw.query.model.result.Result
import de.uniwue.dw.query.model.result.Row
import de.uniwue.dw.query.model.result.export.ExportConfiguration
import de.uniwue.dw.query.model.result.export.ExportType
import de.uniwue.dw.query.model.result.export.MemoryOutputHandler
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNUserSettingsConnector
import java.util.concurrent.ThreadLocalRandom

class ResultCorrelationTab : Tab() {

    private val correlationTabContent: VerticalLayout
    private val correlationTable: Grid<Row>
    private val explanation: Div

    private val outputHandler: MemoryOutputHandler
    private val usedIDsForHeadersToColumnNumber = mutableMapOf<String, Int>()

    init {
        label = getTranslation("web-padawan.resultTable.correlationTab.title")
        isVisible = false

        correlationTabContent = VerticalLayout()
        correlationTabContent.setHeightFull()

        correlationTable = createCorrelationTable()
        correlationTable.setHeightFull()

        explanation = Div()

        val user = PaDaWaNConnector.getUser()
        outputHandler = MemoryOutputHandler(null, ExportConfiguration(ExportType.GUI), user.kAnonymity)

        correlationTabContent.add(correlationTable, explanation)
    }

    fun displayCorrelationQueryResults(result: Result, referenceSetName: String) {
        correlationTable.setItems()
        correlationTable.removeAllColumns()
        usedIDsForHeadersToColumnNumber.clear()

        val columnsToIgnore = setOf(1, 2, result.header.size - 2, result.header.size - 1)
        val rowSubtract = 1
        val columnSubtract = 3
        for ((i, aHeader) in result.header.withIndex()) {
            if (i in columnsToIgnore)
                continue

            var idToUse = aHeader
            while (usedIDsForHeadersToColumnNumber.contains(idToUse))
                idToUse = aHeader + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE)
            usedIDsForHeadersToColumnNumber[idToUse] = i

            var headerToUse = ResultTable.reformatYearIntervalIfNecessary(aHeader)

            correlationTable.addColumn(ComponentRenderer { row ->
                val curCorrelationWithPValue = if (i == 0)
                    ResultTable.getCellValueString(row.getCell(i), outputHandler).first
                else
                    result.correlationWithPValues[row.rowNumber - rowSubtract][i - columnSubtract]

                Html("<span>$curCorrelationWithPValue</span>")
            })
                .setHeader(headerToUse)
                .setKey(idToUse)
                .setAutoWidth(true)
                .setComparator { row ->
                    if (i == 0)
                        outputHandler.getFormattedValue(row.getCell(i))
                    else
                        result.correlationWithPValues[row.rowNumber - rowSubtract][i - columnSubtract]
                }
        }
        correlationTable.setItems(result.rows.drop(rowSubtract))

        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        val referenceSetNameToUse = if (userSettings.userInterfaceLanguage == UserInterfaceLanguage.english)
            referenceSetName.lowercase()
        else
            referenceSetName
        val explanationHTML = getTranslation("web-padawan.resultTable.correlationTab.legend", referenceSetNameToUse)
        explanation.element.setProperty("innerHTML", explanationHTML)

        isVisible = true
    }

    private fun createCorrelationTable(): Grid<Row> {
        val grid = Grid(Row::class.java, false)
        grid.className = "force-focus-outline"
        grid.selectionMode = Grid.SelectionMode.NONE
        grid.addThemeVariants(GridVariant.LUMO_ROW_STRIPES, GridVariant.LUMO_COMPACT)
        return grid
    }

    fun getCorrelationTabContent(): VerticalLayout {
        return correlationTabContent
    }
}