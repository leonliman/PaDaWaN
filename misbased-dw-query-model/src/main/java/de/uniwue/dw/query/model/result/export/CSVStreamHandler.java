package de.uniwue.dw.query.model.result.export;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.result.Cell;
import de.uniwue.dw.query.model.result.QueryRunnable;
import de.uniwue.dw.query.model.result.Result;
import de.uniwue.dw.query.model.result.Row;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class CSVStreamHandler extends MemoryOutputHandler {

  private CSVPrinter csvPrinter;

  public CSVStreamHandler(QueryRunnable runnable, ExportConfiguration exportConfig,
          int kAnonymity) {
    super(runnable, exportConfig, kAnonymity);
  }

  @Override
  public void setHeader(List<String> header) throws QueryException {
    writeHeader(header);
  }

  @Override
  public void addRow(Row row) throws QueryException {
    writeRow(row);
  }

  @Override
  public void done() throws QueryException {
    close();
  }

  @Override
  public Result getResult() {
    return null;
  }

  private void writeHeader(List<String> header) throws QueryException {
    try {
      OutputStreamWriter writer;
      if (exportConfig.isCsvUseUTF8())
        writer = new OutputStreamWriter(getOutputStream(), StandardCharsets.UTF_8);
      else
        writer = new OutputStreamWriter(getOutputStream());
      CSVFormat format = CSVFormat.newFormat(exportConfig.getCsvDelimiter());
      format = format.withRecordSeparator(exportConfig.getCsvRecordSeparator());
      format = format.withEscape(exportConfig.getCsvEscape());
      format = format.withQuote(exportConfig.getCsvQuote());
      format = format.withQuoteMode(exportConfig.getCsvQuoteMode());
      csvPrinter = format.withHeader(header.toArray(new String[0])).print(writer);
    } catch (IOException | IllegalArgumentException e) {
      throw new QueryException(e);
    }
  }

  private void writeRow(Row row) throws QueryException {
    List<String> stringRow = new ArrayList<>();
    for (Cell cell : row.getCells()) {
      String cellValue = cell.getValueAsString().replaceAll("\\R+[\"]?", "<br>");
      cellValue = cellValue.replaceAll(";", "<semicolon>");
      stringRow.add(cellValue);
    }
    try {
      csvPrinter.printRecord(stringRow.toArray(new String[0]));
    } catch (IOException e) {
      throw new QueryException(e);
    }
  }

  @Override
  public void close() throws QueryException {
    try {
      if (csvPrinter != null)
        csvPrinter.close();
      getOutputStream().close();
    } catch (IOException e) {
      throw new QueryException(e);
    }
  }

  @Override
  public void setResult(Result result) throws QueryException {
    setHeader(result.getHeader());
    for (Row n : result.getRows()) {
      addRow(n);
    }
  }

  @Override
  public void setDocsFound(long numFound) {
    // ignore
  }

  @Override
  public void setQueryTime(long queryTime) {
    // ignore
  }

}
