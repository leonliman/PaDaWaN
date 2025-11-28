package de.uniwue.dw.query.model.result.export;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.result.Cell;
import de.uniwue.dw.query.model.result.QueryRunnable;
import de.uniwue.dw.query.model.result.Row;
import de.uniwue.dw.query.model.result.export.GenericTable.FreezeInformation;
import de.uniwue.misc.util.StringUtilsUniWue;

public class ExcelHandler extends MemoryOutputHandler {

  private static final int NUMBER_OF_EXPORTED_ROWS = 10000;

  private ZipOutputStream zipOutStream;

  public ExcelHandler(QueryRunnable runnable, ExportConfiguration exportConfig, int kAnonymity) {
    super(runnable, exportConfig, kAnonymity);
  }

  @Override
  public void done() throws QueryException {
    export();
  }

  private void createHeader(GenericTable excel) {
    int x = 0;
    for (String columnName : getResult().getHeader()) {
      if (exportConfig.isCleanHeadersOfSpecialCharacters()) {
        columnName = StringUtilsUniWue.cleanStringFromSpecialCharacters(columnName);
      }
      excel.setContent(0, x, columnName);
      x++;
    }
  }

  private boolean createContent(GenericTable excel) throws IOException {
    int y = 1;
    int x;
    for (Row row : getResult().getRows()) {
      if (runnable.getMonitor().isCanceled())
        return false;
      x = 0;
      for (Cell cell : row.getCells()) {
        String formattedValue = getFormattedValue(cell);
        if ((cell.getCellData() != null) && (cell.getCellData().attribute != null)
                && cell.getCellData().attribute.getValueInFile()) {
          if (zipOutStream == null) {
            zipOutStream = new ZipOutputStream(getOutputStream());
          }
          String filename = "Files/" + cell.getCellData().getValue().getCaseID() + ".txt";
          ZipEntry entry = new ZipEntry(filename);
          zipOutStream.putNextEntry(entry);
          zipOutStream.write(formattedValue.getBytes());
          excel.setLink(y, x, filename, filename);
        } else {
          excel.setContent(y, x, formattedValue);
        }
        x++;
      }
      y++;
    }
    return true;
  }

  private void export() throws QueryException {
    try {
      int width = getResult().getHeader().size() + 1;
      int height = NUMBER_OF_EXPORTED_ROWS + 1;
      GenericTable excel = new GenericTable(height, width);
      excel.setShowTitle(false);
      createHeader(excel);
      boolean doContinue = createContent(excel);
      if (!doContinue) {
        return;
      }
      excel.setRotateFirstRow(true);
      excel.shrink();
      FreezeInformation freezeInfo = new FreezeInformation(0, 1);
      excel.addFreezeInformation(freezeInfo);
      excel.addRowHeightInformation(0, 150);

      if (zipOutStream != null) {
        doZipResult(excel);
      } else {
        for (int i = 0; i < width; i++) {
          excel.addColumnWidthInformation(i, true, 20000);
        }
        excel.exportToXLS(getOutputStream(), true, exportConfig);
      }
    } catch (IOException e) {
      throw new QueryException(e);
    }
  }

  @Override
  public void close() throws QueryException {
    try {
      getOutputStream().close();
    } catch (IOException e) {
      throw new QueryException(e);
    }
  }

  private void doZipResult(GenericTable excel) throws IOException {
    ZipEntry entry = new ZipEntry("Export.xlsx");
    zipOutStream.putNextEntry(entry);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    excel.exportToXLS(bos, true, exportConfig);
    bos.writeTo(zipOutStream);
    zipOutStream.close();
  }

}
