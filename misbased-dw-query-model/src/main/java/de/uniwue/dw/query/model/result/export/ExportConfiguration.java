package de.uniwue.dw.query.model.result.export;

import org.apache.commons.csv.QuoteMode;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/*
 * This is the configuration for the export of query results. With the type "ResultTable" only the result-object
 * will be created and no file. All other modes create a file. When an output file is not given, the file remains in the
 * resultStream of the queryRunnable. When given, the stream is automatically copied into that file.
 * csvStreaming is a flag used in the queryRunnable's export-method to check if the file has already
 * been created in the createResults-method. This flag has to be set in the respective createResults method so that the
 * result is not written a second time in the writeResultToStream-method.
 */
public class ExportConfiguration {

  private ExportType exportFileType = ExportType.RESULT_TABLE;

  private OutputStream outputStream;

  private Path outputPath;

  /*
   * When set to true, the headers of the output get their special characters replaced by underscores
   * "_". Special characters are all characters other than [a-zA-Z0-9_]
   */
  private boolean cleanHeadersOfSpecialCharacters;

  private char csvDelimiter = ';';

  private String csvRecordSeparator = "\n";

  private char csvEscape = '\\';

  private char csvQuote = '"';

  private QuoteMode csvQuoteMode = QuoteMode.NONE;

  private boolean csvUseUTF8 = true;

  private boolean excelShortenLongTextContent = false;

  private boolean excelIncludeTotalAndSumRowsAndColumns = true;

  private Integer excelDefaultColumnWidth = null;

  private String excelSheetName = "Export";

  private String statisticsAllColumnName = "Alle";

  private String statisticsSumColumnName = "Summe";

  private String statisticsDuplicatesColumnName = "Doppelte";

  private String filterIDTypePatientsName = "Patienten";

  private String filterIDTypeCasesName = "Fälle";

  private String filterIDTypeDocumentsName = "Dokumente";

  private String filterIDTypeStraighteningStepsName = "Richtvorgänge";

  private String filterIDTypeGroupedByName = "gruppiert nach";

  private String filterIDTypeGroupedByPatientsName = "Patienten";

  private String filterIDTypeGroupedByCasesName = "Fällen";

  private String filterIDTypeGroupedByDocumentsName = "Dokumenten";

  public ExportConfiguration() {
  }

  public ExportConfiguration(ExportType exportFileType) {
    this.exportFileType = exportFileType;
  }

  public ExportType getExportFileType() {
    return exportFileType;
  }

  public void setExportFileType(ExportType exportFileType) {
    this.exportFileType = exportFileType;
  }

  public OutputStream getOutputStream() throws IOException {
    if (outputStream == null) {
      Path path = getOutputPath();
      outputStream = Files.newOutputStream(path);
    }
    return outputStream;
  }

  public void setOutputStream(OutputStream outputStream) {
    this.outputStream = outputStream;
  }

  public Path getOutputPath() {
    return outputPath;
  }

  public void setOutputPath(Path outputPath) {
    this.outputPath = outputPath;
  }

  public boolean isCleanHeadersOfSpecialCharacters() {
    return cleanHeadersOfSpecialCharacters;
  }

  public void setCleanHeadersOfSpecialCharacters(boolean cleanHeadersOfSpecialCharacters) {
    this.cleanHeadersOfSpecialCharacters = cleanHeadersOfSpecialCharacters;
  }

  public char getCsvDelimiter() {
    return csvDelimiter;
  }

  public void setCsvDelimiter(char csvDelimiter) {
    this.csvDelimiter = csvDelimiter;
  }

  public String getCsvRecordSeparator() {
    return csvRecordSeparator;
  }

  public void setCsvRecordSeparator(String csvRecordSeparator) {
    this.csvRecordSeparator = csvRecordSeparator;
  }

  public char getCsvEscape() {
    return csvEscape;
  }

  public void setCsvEscape(char csvEscape) {
    this.csvEscape = csvEscape;
  }

  public char getCsvQuote() {
    return csvQuote;
  }

  public void setCsvQuote(char csvQuote) {
    this.csvQuote = csvQuote;
  }

  public QuoteMode getCsvQuoteMode() {
    return csvQuoteMode;
  }

  public void setCsvQuoteMode(QuoteMode csvQuoteMode) {
    this.csvQuoteMode = csvQuoteMode;
  }

  public boolean isCsvUseUTF8() {
    return csvUseUTF8;
  }

  public void setCsvUseUTF8(boolean csvUseUTF8) {
    this.csvUseUTF8 = csvUseUTF8;
  }

  public boolean isExcelShortenLongTextContent() {
    return excelShortenLongTextContent;
  }

  public void setExcelShortenLongTextContent(boolean excelShortenLongTextContent) {
    this.excelShortenLongTextContent = excelShortenLongTextContent;
  }

  public boolean isExcelIncludeTotalAndSumRowsAndColumns() {
    return excelIncludeTotalAndSumRowsAndColumns;
  }

  public void setExcelIncludeTotalAndSumRowsAndColumns(boolean excelIncludeTotalAndSumRowsAndColumns) {
    this.excelIncludeTotalAndSumRowsAndColumns = excelIncludeTotalAndSumRowsAndColumns;
  }

  public Integer getExcelDefaultColumnWidth() {
    return excelDefaultColumnWidth;
  }

  public void setExcelDefaultColumnWidth(Integer excelDefaultColumnWidth) {
    this.excelDefaultColumnWidth = excelDefaultColumnWidth;
  }

  public String getExcelSheetName() {
    return excelSheetName;
  }

  public void setExcelSheetName(String excelSheetName) {
    this.excelSheetName = excelSheetName;
  }

  public String getStatisticsAllColumnName() {
    return statisticsAllColumnName;
  }

  public void setStatisticsAllColumnName(String statisticsAllColumnName) {
    this.statisticsAllColumnName = statisticsAllColumnName;
  }

  public String getStatisticsSumColumnName() {
    return statisticsSumColumnName;
  }

  public void setStatisticsSumColumnName(String statisticsSumColumnName) {
    this.statisticsSumColumnName = statisticsSumColumnName;
  }

  public String getStatisticsDuplicatesColumnName() {
    return statisticsDuplicatesColumnName;
  }

  public void setStatisticsDuplicatesColumnName(String statisticsDuplicatesColumnName) {
    this.statisticsDuplicatesColumnName = statisticsDuplicatesColumnName;
  }

  public String getFilterIDTypePatientsName() {
    return filterIDTypePatientsName;
  }

  public void setFilterIDTypePatientsName(String filterIDTypePatientsName) {
    this.filterIDTypePatientsName = filterIDTypePatientsName;
  }

  public String getFilterIDTypeCasesName() {
    return filterIDTypeCasesName;
  }

  public void setFilterIDTypeCasesName(String filterIDTypeCasesName) {
    this.filterIDTypeCasesName = filterIDTypeCasesName;
  }

  public String getFilterIDTypeDocumentsName() {
    return filterIDTypeDocumentsName;
  }

  public void setFilterIDTypeDocumentsName(String filterIDTypeDocumentsName) {
    this.filterIDTypeDocumentsName = filterIDTypeDocumentsName;
  }

  public String getFilterIDTypeStraighteningStepsName() {
    return filterIDTypeStraighteningStepsName;
  }

  public void setFilterIDTypeStraighteningStepsName(String filterIDTypeStraighteningStepsName) {
    this.filterIDTypeStraighteningStepsName = filterIDTypeStraighteningStepsName;
  }

  public String getFilterIDTypeGroupedByName() {
    return filterIDTypeGroupedByName;
  }

  public void setFilterIDTypeGroupedByName(String filterIDTypeGroupedByName) {
    this.filterIDTypeGroupedByName = filterIDTypeGroupedByName;
  }

  public String getFilterIDTypeGroupedByPatientsName() {
    return filterIDTypeGroupedByPatientsName;
  }

  public void setFilterIDTypeGroupedByPatientsName(String filterIDTypeGroupedByPatientsName) {
    this.filterIDTypeGroupedByPatientsName = filterIDTypeGroupedByPatientsName;
  }

  public String getFilterIDTypeGroupedByCasesName() {
    return filterIDTypeGroupedByCasesName;
  }

  public void setFilterIDTypeGroupedByCasesName(String filterIDTypeGroupedByCasesName) {
    this.filterIDTypeGroupedByCasesName = filterIDTypeGroupedByCasesName;
  }

  public String getFilterIDTypeGroupedByDocumentsName() {
    return filterIDTypeGroupedByDocumentsName;
  }

  public void setFilterIDTypeGroupedByDocumentsName(String filterIDTypeGroupedByDocumentsName) {
    this.filterIDTypeGroupedByDocumentsName = filterIDTypeGroupedByDocumentsName;
  }
}
