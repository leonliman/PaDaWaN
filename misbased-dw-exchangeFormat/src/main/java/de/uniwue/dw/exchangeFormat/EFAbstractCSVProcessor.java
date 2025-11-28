package de.uniwue.dw.exchangeFormat;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class EFAbstractCSVProcessor {

  protected static Logger logger = LogManager.getLogger(EFAbstractCSVProcessor.class);

  private enum CSVParseMode {
    String, File
  }

  private char csvDelimiter = ';';

  private char csvQuote = '"';

  private Character csvEscape = null;

  private String csvRecordSeparator = "\r\n";

  private CSVFormat csvFormat = null;

  private CSVPrinter csvPrinter = null;

  private String[] headers = null;

  private CSVParseMode parseMode;

  private String stringToParse = null;

  private File fileToUse = null;

  private List<List<Object>> recordsToSave = null;

  private StringBuilder outputStringBuilder = null;

  private FileOutputStream fileOutputStream = null;

  private OutputStreamWriter outputStreamWriter = null;

  protected int autoCommitAfterRows = 500;

  private int allSavedRowsCount = 0;

  public EFAbstractCSVProcessor(String csvStringToParse) {
    parseMode = CSVParseMode.String;
    stringToParse = csvStringToParse;
  }

  public EFAbstractCSVProcessor(File csvFileToUse) {
    parseMode = CSVParseMode.File;
    fileToUse = csvFileToUse;
  }

  public void setCsvDelimiterChar(char csvDelimiter) {
    csvFormat = null;
    this.csvDelimiter = csvDelimiter;
  }

  public void setCsvQuoteChar(char csvQuote) {
    csvFormat = null;
    this.csvQuote = csvQuote;
  }

  public void setCsvEscapeCharacter(Character csvEscape) {
    csvFormat = null;
    this.csvEscape = csvEscape;
  }

  public void setCsvRecordSeparatorString(String csvRecordSeparator) {
    csvFormat = null;
    this.csvRecordSeparator = csvRecordSeparator;
  }

  public void setCsvFormat(CSVFormat csvFormat) {
    this.csvFormat = csvFormat;
  }

  public void setCsvPrinter(CSVPrinter csvPrinter) throws IOException {
    if (this.csvPrinter != null) {
      csvPrinter.close();
    }
    this.csvPrinter = csvPrinter;
    allSavedRowsCount = 0;
  }

  public void setHeadersForSaving(String[] headers) throws IOException {
    if (this.csvPrinter != null) {
      csvPrinter.close();
      csvPrinter = null;
    }
    this.headers = headers;
  }

  public Iterable<CSVRecord> getCSVRecords() throws IOException {
    return getCSVRecords(true);
  }

  public Iterable<CSVRecord> getCSVRecords(boolean withFirstRecordAsHeader) throws IOException {
    Reader reader = null;
    switch (parseMode) {
      case File:
        reader = new FileReader(fileToUse);
        break;
      case String:
        reader = new StringReader(stringToParse);
        break;
    }
    if (withFirstRecordAsHeader) {
      return getCSVFormat().withFirstRecordAsHeader().parse(reader);
    } else {
      return getCSVFormat().parse(reader);
    }
  }

  public Map<String, Integer> getCSVHeaderToPositionMap() throws IOException {
    Reader reader = null;
    switch (parseMode) {
      case File:
        reader = new FileReader(fileToUse);
        break;
      case String:
        reader = new StringReader(stringToParse);
        break;
    }
    return getCSVFormat().withFirstRecordAsHeader().parse(reader).getHeaderMap();
  }

  public void saveSingleRow(List<Object> rowToSave) throws IOException {
    List<List<Object>> singleRowList = new ArrayList<List<Object>>();
    singleRowList.add(rowToSave);
    saveMultipleRows(singleRowList);
  }

  public void saveMultipleRows(List<List<Object>> rowsToSave) throws IOException {
    if (recordsToSave == null) {
      recordsToSave = rowsToSave;
    } else {
      recordsToSave.addAll(rowsToSave);
    }
    if (recordsToSave.size() >= autoCommitAfterRows) {
      commit();
    }
  }

  public void commit() throws IOException {
    if (recordsToSave != null && recordsToSave.size() > 0) {
      getCSVPrinter().printRecords(recordsToSave);
      getCSVPrinter().flush();
      allSavedRowsCount += recordsToSave.size();
      logger.info(recordsToSave.size() + " new rows saved; " + allSavedRowsCount
              + " rows saved in total");
      recordsToSave = null;
    }
  }

  public void printCurrentSaveResult() throws IOException {
    commit();
    if (outputStreamWriter == null && outputStringBuilder == null) {
      System.err.println("Nothing had been saved yet");
      return;
    }
    String resultValue = null;
    switch (parseMode) {
      case File:
        resultValue = "The current result can be seen in the file " + fileToUse.getAbsolutePath();
        break;
      case String:
        resultValue = "This is the currently generated CSV:\n" + outputStringBuilder.toString();
        break;
    }
    System.out.println(resultValue);
  }

  private CSVPrinter getCSVPrinter() throws IOException {
    Appendable out = null;
    if (csvPrinter == null) {
      switch (parseMode) {
        case File:
          if (outputStreamWriter != null) {
            fileOutputStream.close();
            outputStreamWriter.close();
          }
          fileOutputStream = new FileOutputStream(fileToUse);
          outputStreamWriter = new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8);
          out = outputStreamWriter;
          break;
        case String:
          outputStringBuilder = new StringBuilder();
          out = outputStringBuilder;
          break;
      }
      if (headers != null && headers.length > 0) {
        csvPrinter = getCSVFormat().withHeader(headers).print(out);
      } else {
        csvPrinter = getCSVFormat().print(out);
      }
      allSavedRowsCount = 0;
    }
    return csvPrinter;
  }

  private CSVFormat getCSVFormat() {
    if (csvFormat == null) {
      csvFormat = CSVFormat.DEFAULT.withDelimiter(csvDelimiter).withQuote(csvQuote)
              .withEscape(csvEscape).withRecordSeparator(csvRecordSeparator).withIgnoreEmptyLines()
              .withTrim();
    }
    return csvFormat;
  }

  public static boolean checkCSVContentIsSet(CSVRecord csvRecord, String columnToCheck) {
    return csvRecord.isSet(columnToCheck) && !csvRecord.get(columnToCheck).trim().isEmpty();
  }

  @Override
  protected void finalize() throws Throwable {
    csvPrinter.close();
    super.finalize();
  }

}
