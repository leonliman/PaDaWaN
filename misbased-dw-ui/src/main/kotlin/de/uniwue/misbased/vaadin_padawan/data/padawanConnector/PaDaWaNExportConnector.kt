package de.uniwue.misbased.vaadin_padawan.data.padawanConnector

import de.uniwue.dw.core.client.authentication.User
import de.uniwue.dw.query.model.client.GUIClientException
import de.uniwue.dw.query.model.lang.QueryRoot
import de.uniwue.dw.query.model.result.export.ExportType
import de.uniwue.misbased.vaadin_padawan.data.model.ExportConfiguration
import de.uniwue.misbased.vaadin_padawan.data.model.QueryExecutionState
import de.uniwue.misbased.vaadin_padawan.data.model.QueryState
import org.apache.commons.io.FileUtils
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.ThreadLocalRandom

object PaDaWaNExportConnector {

    private val userNameToQueryIDToExportedFileMap: MutableMap<String, MutableMap<Int, File>> = HashMap()
    private val dateFormat = SimpleDateFormat("yyyy-MM-dd_HH-mm")
    private const val NEXT_STATUS_QUERY_INTERVAL_IN_MS = 1000

    fun startExport(query: QueryRoot, guiExportConfiguration: ExportConfiguration, user: User): QueryState {
        try {
            query.limitResult = 0

            if (!query.isStatisticQuery && !user.isAllowedToUseCaseQuery && !user.isAdmin)
                return QueryState(
                    queryState = QueryExecutionState.ERROR,
                    errorMessageTranslationKey = "web-padawan.errorDialog.userMayOnlyRunStatisticQueries"
                )

            val exportFolder = PaDaWaNConnector.getResourceFile("/export")
            Files.createDirectories(exportFolder.toPath())
            val exportFile =
                getExportFile(guiExportConfiguration.exportType, exportFolder, guiExportConfiguration.excelFileName)

            val exportConfiguration =
                de.uniwue.dw.query.model.result.export.ExportConfiguration(guiExportConfiguration.exportType)
            exportConfiguration.outputPath = exportFile.toPath()
            val outputStream = Files.newOutputStream(exportFile.toPath(), StandardOpenOption.CREATE_NEW)
            exportConfiguration.outputStream = outputStream

            exportConfiguration.csvDelimiter = guiExportConfiguration.csvDelimiter
            var csvRecordSeparator = guiExportConfiguration.csvRecordSeparator
            if (csvRecordSeparator == "\\n")
                csvRecordSeparator = "\n"
            exportConfiguration.csvRecordSeparator = csvRecordSeparator
            exportConfiguration.csvEscape = guiExportConfiguration.csvEscape
            exportConfiguration.csvQuote = guiExportConfiguration.csvQuote
            exportConfiguration.csvQuoteMode = guiExportConfiguration.csvQuoteMode
            exportConfiguration.isCsvUseUTF8 = guiExportConfiguration.csvUseUTF8
            exportConfiguration.isExcelShortenLongTextContent = guiExportConfiguration.excelShortenLongTextContent
            exportConfiguration.isExcelIncludeTotalAndSumRowsAndColumns =
                guiExportConfiguration.excelIncludeTotalAndSumRowsAndColumns
            if (guiExportConfiguration.excelIncludeTotalAndSumRowsAndColumns)
                query.comment = "Total and sum rows and columns should be included in an excel export"
            else
                query.comment = "Total and sum rows and columns should not be included in an excel export"
            if (guiExportConfiguration.excelDefaultColumnWidth != null) {
                val defaultColumnWidth = guiExportConfiguration.excelDefaultColumnWidth!!
                if (defaultColumnWidth > 0)
                    exportConfiguration.excelDefaultColumnWidth = defaultColumnWidth
            }
            if (!guiExportConfiguration.excelSheetName.isNullOrBlank())
                exportConfiguration.excelSheetName = guiExportConfiguration.excelSheetName

            val queryRunner = PaDaWaNConnector.getGUIClient().queryRunner
            val queryID = queryRunner.createQuery(query, user, exportConfiguration)
            queryRunner.runQuery(queryID)
            if (!userNameToQueryIDToExportedFileMap.containsKey(user.username))
                userNameToQueryIDToExportedFileMap[user.username] = HashMap()

            userNameToQueryIDToExportedFileMap[user.username]!![queryID] = exportFile

            return QueryState(queryID = queryID, nextStatusQueryIntervalInMS = NEXT_STATUS_QUERY_INTERVAL_IN_MS)
        } catch (e: Exception) {
            e.printStackTrace()
            return QueryState(
                queryState = QueryExecutionState.ERROR,
                errorMessage = e.localizedMessage
            )
        }
    }

    private fun getExportFile(exportType: ExportType, exportFolder: File, exportFileName: String? = null): File {
        val fileExtension = when (exportType) {
            ExportType.CSV -> "csv"
            ExportType.EXCEL -> "xlsx"
            else -> throw GUIClientException("A query can only be exported into CSV or EXCEL format")
        }
        return if (!exportFileName.isNullOrBlank()) {
            val exportFile = File(exportFolder, "$exportFileName.$fileExtension")
            if (exportFile.exists())
                if (!exportFile.delete())
                    throw GUIClientException("The File ${exportFile.absolutePath} already exists and could not be deleted")
            exportFile
        } else {
            var exportFile = File(exportFolder, getRandomFileName(fileExtension))
            while (exportFile.exists())
                exportFile = File(exportFolder, getRandomFileName(fileExtension))
            exportFile
        }
    }

    private fun getRandomFileName(fileExtension: String): String {
        return "PaDaWaN-Export_${dateFormat.format(Date())}_${ThreadLocalRandom.current().nextInt()}.$fileExtension"
    }

    fun exportStatus(queryID: Int, user: User): QueryState {
        try {
            val exportFile = userNameToQueryIDToExportedFileMap[user.username]?.get(queryID)
                ?: return QueryState(
                    queryID = queryID,
                    queryState = QueryExecutionState.ERROR,
                    errorMessageTranslationKey = "web-padawan.errorDialog.queryStatusUnavailable"
                )

            val progress = PaDaWaNConnector.getGUIClient().queryRunner.getProgress(queryID)

            val queryState =
                QueryState(queryID = queryID, nextStatusQueryIntervalInMS = NEXT_STATUS_QUERY_INTERVAL_IN_MS)
            when {
                progress.queryStoppedWithErrors() -> {
                    queryState.queryState = QueryExecutionState.ERROR
                    queryState.errorMessage = progress.errorMessage
                }

                progress.isDone -> {
                    if (waitForExportToFinish(exportFile)) {
                        val readableFileSize = FileUtils.byteCountToDisplaySize(FileUtils.sizeOf(exportFile))
                        queryState.queryState = QueryExecutionState.FINISHED_SUCCESSFULLY
                        queryState.statusMessage = readableFileSize
                        queryState.progress = 1.0
                    } else {
                        queryState.queryState = QueryExecutionState.ERROR
                        queryState.errorMessageTranslationKey = "web-padawan.errorDialog.exportCreatedEmptyFile"
                    }
                }

                else -> queryState.progress = progress.getProgressPercentageAsFraction
            }

            return queryState
        } catch (e: Exception) {
            e.printStackTrace()
            return QueryState(
                queryID = queryID,
                queryState = QueryExecutionState.ERROR,
                errorMessage = e.localizedMessage
            )
        }
    }

    private fun waitForExportToFinish(exportFile: File): Boolean {
        // As the query may be shown as done before the resulting file has actually been created,
        // the following loop waits until the file has a size greater than 0
        var numWaits = 0
        while (FileUtils.sizeOf(exportFile) == 0L && numWaits <= 100) {
            Thread.sleep(100)
            numWaits++
        }
        return FileUtils.sizeOf(exportFile) != 0L
    }

    fun cancelExport(queryID: Int, user: User): QueryState {
        try {
            if (!userNameToQueryIDToExportedFileMap.containsKey(user.username) ||
                !userNameToQueryIDToExportedFileMap[user.username]!!.containsKey(queryID)
            )
                return QueryState(
                    queryID = queryID,
                    queryState = QueryExecutionState.ERROR,
                    errorMessageTranslationKey = "web-padawan.errorDialog.queryStatusUnavailable"
                )

            userNameToQueryIDToExportedFileMap[user.username]!!.remove(queryID)

            val cancellationResultStatus = PaDaWaNQueryConnector.doQueryCancellation(queryID)
            return QueryState(
                queryID = queryID,
                queryState = cancellationResultStatus
            )
        } catch (e: Exception) {
            e.printStackTrace()
            return QueryState(
                queryID = queryID,
                queryState = QueryExecutionState.ERROR,
                errorMessage = e.localizedMessage
            )
        }
    }

    fun getExportResult(queryID: Int, user: User): File? {
        val exportFile = userNameToQueryIDToExportedFileMap[user.username]?.get(queryID)
        if (exportFile != null && exportFile.exists())
            return exportFile
        return null
    }
}