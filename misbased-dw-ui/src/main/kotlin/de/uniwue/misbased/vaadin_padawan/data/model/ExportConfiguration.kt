package de.uniwue.misbased.vaadin_padawan.data.model

import de.uniwue.dw.query.model.result.export.ExportType
import org.apache.commons.csv.QuoteMode

data class ExportConfiguration(
    val exportType: ExportType,
    var csvDelimiter: Char = ';',
    var csvRecordSeparator: String = "\\n",
    var csvEscape: Char = '\\',
    var csvQuote: Char = '"',
    var csvQuoteMode: QuoteMode = QuoteMode.NONE,
    var csvUseUTF8: Boolean = true,
    var excelShortenLongTextContent: Boolean = false,
    var excelIncludeTotalAndSumRowsAndColumns: Boolean = true,
    var excelDefaultColumnWidth: Int? = null,
    var excelSheetName: String? = null,
    var excelFileName: String? = null
)
