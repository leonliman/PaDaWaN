package de.uniwue.dw.imports;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;

import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.configured.data.ConfigDataSourceCSVDir;
import de.uniwue.misc.util.StringUtilsUniWue;

/**
 * 
 * Container class for csv-files. Provides useful tools for handling those files.
 *
 */
public class CsvFile extends DataElemFile implements ITableElem {

  private String UTF8_BOM = "\uFEFF";

  private CSVRecord csvRecord;

  private CSVParser csvParser;

  private Iterator<CSVRecord> csvIter;

  private List<String> headerList;

  private boolean isInitialized = false;

  private Long rowCounter = 0L;
  
  public CsvFile(File file, ConfigDataSourceCSVDir dataSourceCSV) throws ImportException {
    super(file, dataSourceCSV);
  }

  protected ConfigDataSourceCSVDir getDataSource() {
    return (ConfigDataSourceCSVDir) dataSource;
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.ITableElem#initialize()
   */
  public void initialize() throws ImportException {
    try {
      if (isInitialized) {
        return;
      }
      isInitialized = true;
      // InputStream inStream = new FileInputStream(file);
      // InputStreamReader inSR = new InputStreamReader(inStream, encoding);
      CSVFormat csvFormat = null;
      if (getDataSource().firstIsHeader) {
        csvFormat = CSVFormat.newFormat(getDataSource().delimiter).withFirstRecordAsHeader()
                .withIgnoreHeaderCase();
      } else {
        csvFormat = CSVFormat.newFormat(getDataSource().delimiter).withHeader(
                getDataSource().manheader);
      }

      csvFormat = csvFormat.withEscape(getDataSource().escapeChar);
      csvFormat = csvFormat.withQuoteMode(getDataSource().quoteMode);
      csvFormat = csvFormat.withQuote(getDataSource().quoteChar);
      if (getDataSource().quoteMode != QuoteMode.NONE) {
        csvFormat = csvFormat.withEscape(getDataSource().escapeChar);
      }
      // hier könnte man das File selbst lesen und das BOM bei Bedarf wegschneiden
      // if (header[0].startsWith(UTF8_BOM)) {
      // header[0] = header[0].substring(1);
      // }

      setCsvParser(CSVParser.parse(file, Charset.forName(getDataSource().encoding), csvFormat));
      setCsvIter(getCsvParser().iterator());
      initHeaderLine();
    } catch (FileNotFoundException e) {
      throw new ImportException(ImportExceptionType.FILE_NOT_FOUND, "file '" + file.getName()
              + "' not found", e);
    } catch (UnsupportedEncodingException e) {
      throw new ImportException(ImportExceptionType.IO_ERROR, "file '" + file.getName()
              + "' has wrong coding", e);
    } catch (IOException e) {
      throw new ImportException(ImportExceptionType.IO_ERROR, e);
    }
    /*
     * started to think about a threaded importer, first writes, second processes, but did not want
     * to spend time yet: readLines = 0L; fileLines= new HashMap<Long, String[]>(); CsvFileReader
     * csvFileReader = new CsvFileReader(readLines, fileLines); csvFileReader.start();
     */
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.ITableElem#getHeaderColumns()
   */
  @Override
  public Set<String> getHeaderColumns() throws ImportException {
    return getCsvParser().getHeaderMap().keySet();
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.ITableElem#getHeaderColumnsAsList()
   */
  @Override
  public List<String> getHeaderColumnsAsList() {
    return headerList;
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.ITableElem#getColumnIndex(java.lang.String)
   */
  @Override
  public Integer getColumnIndex(String key) throws ImportException {
    if (getCsvParser().getHeaderMap().containsKey(key)) {
      return getCsvParser().getHeaderMap().get(key);
    } else {
      throw new ImportException(ImportExceptionType.CSV_COLUMN_NON_EXISTANT, "column '" + key
              + "' non existant ");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.ITableElem#getItem(java.lang.String)
   */
  @Override
  public String getItem(String key) throws ImportException {
    return csvRecord.get(key).trim();
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.ITableElem#moveToNextLine()
   */
  @Override
  public Boolean moveToNextLine() throws ImportException {
    if (!getCsvIter().hasNext()) {
      return false;
    }
    csvRecord = getCsvIter().next();
    rowCounter++;
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.ITableElem#getTokenLength()
   */
  @Override
  public Integer getTokenLength() {
    return csvRecord.size();
  }

  private HashMap<String, Integer> mapCSVHeaderToIndex(String[] header) {
    HashMap<String, Integer> tmp = new HashMap<String, Integer>();
    for (Integer i = 0; i < header.length; i++) {
      tmp.put(header[i].trim(), i);
    }
    return tmp;
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.ITableElem#getHeaderColAmount()
   */
  @Override
  public Integer getHeaderColAmount() throws ImportException {

    return getCsvParser().getHeaderMap().size();

  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.ITableElem#initHeaderLine()
   */
  @Override
  public boolean initHeaderLine() throws ImportException {
    // extract file headers
    String[] header = getCsvParser().getHeaderMap().keySet().toArray(new String[0]);
    headerList = buildHeaderList(getCsvParser().getHeaderMap());

    return true;
  }

  private List<String> buildHeaderList(Map<String, Integer> colName2index) {
    return colName2index.entrySet().stream().sorted(Map.Entry.comparingByValue(/*
                                                                                * Collections.
                                                                                * reverseOrder()
                                                                                */))
            .map(Entry::getKey).map(String::trim).collect(Collectors.toList());
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.ITableElem#checkRequiredHeaders(java.lang.String[])
   */
  @Override
  public void checkRequiredHeaders(String[] necessaryColumnHeaders) throws ImportException {
    if (necessaryColumnHeaders != null) {
      checkRequiredHeaders(Arrays.asList(necessaryColumnHeaders));
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.ITableElem#checkRequiredHeaders(java.util.List)
   */
  @Override
  public void checkRequiredHeaders(List<String> necessaryColumnHeaders) throws ImportException {
    // check if all row column headers exist in file
    if (necessaryColumnHeaders != null) {
      Map<String, Integer> needlist = getCsvParser().getHeaderMap();
      for (String aNecessaryHeader : necessaryColumnHeaders) {
        if (!needlist.containsKey(aNecessaryHeader)) {
          throw new ImportException(ImportExceptionType.CSV_COLUMNS_MISSING,
                  "does not contain all required columns. Required: " + necessaryColumnHeaders
                          + ". Included: "
                          + StringUtilsUniWue.concat(getCsvParser().getHeaderMap().keySet(), ",")
                          + "");
        }
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.ITableElem#close()
   */
  @Override
  public void close() throws ImportException {
    try {
      getCsvParser().close();
    } catch (IOException e) {
      throw new ImportException(ImportExceptionType.IO_ERROR, "error while closing csv file");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.ITableElem#getCurrentLineTokens()
   */
  @Override
  public String[] getCurrentLineTokens() {
    return csvRecord.toMap().values().toArray(new String[0]);
  }

  /*
   * (non-Javadoc)
   * 
   * @see de.uniwue.dw.imports.ITableElem#isCurrentLineWellFormed()
   */
  @Override
  public boolean isCurrentLineWellFormed() throws ImportException {
    if (csvRecord.size() == getHeaderColAmount()) {
      return true;
    }
    return false;
  }

  private CSVParser getCsvParser() throws ImportException {
    initialize();
    return csvParser;
  }

  private void setCsvParser(CSVParser csvParser) {
    this.csvParser = csvParser;
  }

  private Iterator<CSVRecord> getCsvIter() throws ImportException {
    initialize();
    return csvIter;
  }

  private void setCsvIter(Iterator<CSVRecord> csvIter) {
    this.csvIter = csvIter;
  }

  public Long getRowCounter() {
    return rowCounter;
  }

  @Override
  public void logLatestRowNumber() {
    // do nothing for CSVFiles
  }


}
