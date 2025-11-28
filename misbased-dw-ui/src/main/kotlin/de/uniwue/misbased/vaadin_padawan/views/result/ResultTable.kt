package de.uniwue.misbased.vaadin_padawan.views.result

import com.vaadin.flow.component.Html
import com.vaadin.flow.component.Text
import com.vaadin.flow.component.UI
import com.vaadin.flow.component.grid.Grid
import com.vaadin.flow.component.grid.GridVariant
import com.vaadin.flow.component.html.H4
import com.vaadin.flow.component.html.H5
import com.vaadin.flow.component.orderedlayout.FlexComponent
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.data.renderer.ComponentRenderer
import de.uniwue.dw.core.model.data.CatalogEntryType
import de.uniwue.dw.query.model.result.Cell
import de.uniwue.dw.query.model.result.Result
import de.uniwue.dw.query.model.result.Row
import de.uniwue.dw.query.model.result.export.ExportConfiguration
import de.uniwue.dw.query.model.result.export.ExportType
import de.uniwue.dw.query.model.result.export.MemoryOutputHandler
import de.uniwue.misbased.vaadin_padawan.data.imageConnector.ImageConnector
import de.uniwue.misbased.vaadin_padawan.data.model.PACSStatus
import de.uniwue.misbased.vaadin_padawan.data.pacsConnector.PACSConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.views.catalog.CatalogTree
import de.uniwue.misbased.vaadin_padawan.views.query.QueryAttributesCustomField
import de.uniwue.misbased.vaadin_padawan.views.query.QueryViewBottomActionButtonsRow
import de.uniwue.misbased.vaadin_padawan.views.query.images.ImagesDownloadRunningDialog
import org.slf4j.LoggerFactory
import java.util.concurrent.ThreadLocalRandom


class ResultTable : VerticalLayout() {

    private val placeholderTitle: H4
    private val placeholderSubTitle: H5

    private val resultTable: Grid<Row>

    private val outputHandler: MemoryOutputHandler
    private val usedIDsForHeadersToColumnNumber = mutableMapOf<String, Int>()
    private val usedIDsForPluginHeaders = mutableSetOf<String>()

    companion object {
        private const val IMAGE_COLUMN_HEADER_SUFFIX = "#IMAGE-COLUMN#"
        private const val IMAGE_COLUMN_NOT_SET_HEADER = "#NOT-SET#"

        private val usableLogger = LoggerFactory.getLogger(ResultTable::class.java)

        fun reformatYearIntervalIfNecessary(valueToCheck: String): String {
            val regex = "(.*)]\\d{1,2}\\.\\d{1,2}\\.\\d{4},.\\d{1,2}\\.\\d{1,2}\\.\\d{4}]".toRegex()
            return if (valueToCheck.matches(regex)) {
                val headerLength = valueToCheck.length
                val yearString = valueToCheck.substring(headerLength - 5, headerLength - 1)
                val year = (yearString.toInt() - 1).toString()
                regex.matchEntire(valueToCheck)?.groupValues?.get(1) + " $year"
            } else
                valueToCheck
        }

        fun getCellValueString(cell: Cell, outputHandler: MemoryOutputHandler): Pair<String, String?> {
            var curValueAsString = outputHandler.getFormattedValue(cell)

            var fullText = if (cell.cellType == Cell.CellType.Value) {
                val catalogEntry = cell.cellData.attribute.catalogEntry
                if (catalogEntry.dataType == CatalogEntryType.DateTime ||
                    catalogEntry.dataType == CatalogEntryType.Number
                ) {
                    curValueAsString
                } else {
                    outputHandler.getFullText(cell)
                }
            } else if (cell.cellType == Cell.CellType.CaseID || cell.cellType == Cell.CellType.PID ||
                cell.cellType == Cell.CellType.DocID || cell.cellType == Cell.CellType.MeasureTime
            ) {
                curValueAsString
            } else if (cell.cellType == Cell.CellType.Statistical) {
                curValueAsString = reformatYearIntervalIfNecessary(curValueAsString)
                curValueAsString
            } else {
                outputHandler.getFullText(cell)
            }
            fullText = if (fullText.isNullOrBlank()) curValueAsString else fullText

            if (curValueAsString.length > 200) {
                while (curValueAsString.length > 196 &&
                    curValueAsString.lastIndexOf("</span") > 190 &&
                    curValueAsString.lastIndexOf("<span", curValueAsString.lastIndexOf("</span")) < 195
                )
                    curValueAsString = curValueAsString.substring(
                        0,
                        curValueAsString.lastIndexOf("<span", curValueAsString.lastIndexOf("</span"))
                    )
                curValueAsString =
                    "${curValueAsString.substring(0, minOf(curValueAsString.length, 197))}..."
            }

            return if (curValueAsString != fullText) {
                if (!curValueAsString.endsWith("..."))
                    curValueAsString = "$curValueAsString..."
                Pair(curValueAsString, fullText)
            } else
                Pair(curValueAsString, null)
        }

        fun getTextHighlightStyle(): String {
            return "background-color: var(--lumo-warning-color); color: var(--lumo-warning-contrast-color);"
        }
    }

    init {
        justifyContentMode = FlexComponent.JustifyContentMode.CENTER
        alignItems = FlexComponent.Alignment.CENTER

        placeholderTitle = H4(getTranslation("web-padawan.resultTable.placeholder"))
        placeholderSubTitle = H5()
        placeholderSubTitle.style.setColor("var(--lumo-error-text-color)")
        placeholderSubTitle.isVisible = false

        resultTable = createResultTable()
        resultTable.setHeightFull()
        resultTable.isVisible = false

        val user = PaDaWaNConnector.getUser()
        outputHandler = MemoryOutputHandler(null, ExportConfiguration(ExportType.GUI), user.kAnonymity)

        add(placeholderTitle, placeholderSubTitle, resultTable)
    }

    fun updatePlaceholderText(title: String, subTitle: String? = null) {
        resultTable.isVisible = false

        placeholderTitle.text = title
        placeholderTitle.isVisible = true

        if (subTitle != null) {
            placeholderSubTitle.text = subTitle
            placeholderSubTitle.isVisible = true

            placeholderTitle.style.setColor("var(--lumo-error-text-color)")
        } else {
            placeholderSubTitle.isVisible = false

            placeholderTitle.style.setColor(null)
        }
    }

    fun queryExecutionCancelled(): Boolean {
        return placeholderTitle.text == getTranslation("web-padawan.resultTable.canceled")
    }

    fun displayQueryResults(result: Result?, isStatisticQuery: Boolean, pacsSessionKey: String?) {
        if (result == null) {
            updatePlaceholderText(getTranslation("web-padawan.resultTable.noResults"))
            return
        }

        resultTable.setItems()
        resultTable.removeAllColumns()
        usedIDsForHeadersToColumnNumber.clear()
        usedIDsForPluginHeaders.clear()

        val user = PaDaWaNConnector.getUser()
        val currentPage = UI.getCurrent().page

        val pacsColumnHeader =
            if (PACSConnector.latestPACSStatus != null && PACSConnector.latestPACSStatus == PACSStatus.ONLINE)
                PACSConnector.getPACSColumnHeader()
            else
                IMAGE_COLUMN_NOT_SET_HEADER
        val imageColumnHeader =
            if (ImageConnector.isImageConnectionWorking())
                ImageConnector.getImageColumnHeader()
            else
                IMAGE_COLUMN_NOT_SET_HEADER

        for ((i, aHeader) in result.header.withIndex()) {
            val isPACSColumn = aHeader == pacsColumnHeader && !isStatisticQuery
            val isImageColumn = !isPACSColumn && aHeader == imageColumnHeader && !isStatisticQuery
            val idBase = if (isPACSColumn || isImageColumn) aHeader + IMAGE_COLUMN_HEADER_SUFFIX else aHeader

            var idToUse = idBase
            while (usedIDsForHeadersToColumnNumber.contains(idToUse))
                idToUse = idBase + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE)
            usedIDsForHeadersToColumnNumber[idToUse] = i

            var headerToUse = aHeader
            if (isStatisticQuery)
                headerToUse = reformatYearIntervalIfNecessary(headerToUse)

            val displayPACSColumn = isPACSColumn && !pacsSessionKey.isNullOrBlank()
            val tableColumn = resultTable.addColumn(ComponentRenderer { row ->
                if (displayPACSColumn || isImageColumn) {
                    var curValueAsString = outputHandler.getFormattedValue(row.getCell(i)) ?: ""
                    val imageIDs = curValueAsString.split(" | ")

                    val horizontalLayout = HorizontalLayout()
                    horizontalLayout.defaultVerticalComponentAlignment = FlexComponent.Alignment.CENTER
                    val downloadButton = QueryViewBottomActionButtonsRow.createButtonWithIconAndTitle(
                        "fa-download",
                        getTranslation("web-padawan.images.buttons.downloadSinglePicture")
                    )
                    downloadButton.addClickListener {
                        val pacsUserSettings = if (displayPACSColumn) PACSConnector.getPACSSettings(user) else null
                        val downloadURL = if (displayPACSColumn) PACSConnector.getDownloadURL(
                            user.username,
                            imageIDs,
                            pacsSessionKey,
                            pacsUserSettings!!
                        ) else null

                        val downloadRunningDialog =
                            ImagesDownloadRunningDialog("web-padawan.images.download.running")
                        downloadRunningDialog.open()
                        QueryAttributesCustomField.forceUIResize()

                        val ui = UI.getCurrent()
                        val downloadThread = Thread {
                            val startTime = System.currentTimeMillis()
                            val downloadResult = if (displayPACSColumn)
                                PACSConnector.performPACSDownload(downloadURL!!)
                            else
                                ImageConnector.performImagesDownload(imageIDs)
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
                    val viewButton = QueryViewBottomActionButtonsRow.createButtonWithIconAndTitle(
                        "fa-image",
                        getTranslation("web-padawan.images.buttons.viewSinglePicture")
                    )
                    viewButton.addClickListener {
                        if (displayPACSColumn) {
                            val pacsUserSettings = PACSConnector.getPACSSettings(user)
                            val viewURL = PACSConnector.getViewURL(imageIDs, pacsSessionKey, pacsUserSettings)
                            currentPage.open(viewURL)
                        } else {
                            val dialog = ImagesDialog(imageIDs)
                            dialog.open()
                            QueryAttributesCustomField.forceUIResize()
                        }
                    }
                    val numImageIDsText = Text("(${imageIDs.size})")
                    horizontalLayout.add(downloadButton, viewButton, numImageIDsText)
                    horizontalLayout
                } else
                    Html("<span>${getCellValueString(row.getCell(i), outputHandler).first}</span>")
            })
                .setHeader(headerToUse)
                .setKey(idToUse)
                .setAutoWidth(true)
            if (!displayPACSColumn && !isImageColumn)
                tableColumn.setComparator { row ->
                    outputHandler.getFormattedValue(row.getCell(i))
                }
        }
        resultTable.setItems(result.rows)

        placeholderTitle.isVisible = false
        placeholderSubTitle.isVisible = false
        resultTable.isVisible = true
        QueryAttributesCustomField.forceUIResize(400)
    }

    private fun createResultTable(): Grid<Row> {
        val grid = Grid(Row::class.java, false)
        grid.className = "force-focus-outline"
        grid.selectionMode = Grid.SelectionMode.NONE
        grid.addThemeVariants(GridVariant.LUMO_ROW_STRIPES, GridVariant.LUMO_COMPACT)

        grid.addItemDoubleClickListener { event ->
            val columnKey = event.column.key
            val cellID = usedIDsForHeadersToColumnNumber[columnKey]
            if (cellID != null && !columnKey.endsWith(IMAGE_COLUMN_HEADER_SUFFIX)) {
                val cell = event.item.getCell(cellID)
                val (_, fullText) = getCellValueString(cell, outputHandler)
                if (!fullText.isNullOrBlank()) {
                    val dialog = ResultFullTextDialog(event.column.headerText, fullText)
                    dialog.open()
                    QueryAttributesCustomField.forceUIResize()
                }
            }
        }

        CatalogTree.addResizeOnScrollToUIElement(grid.element)

        return grid
    }
}