package de.uniwue.dw.query.model.result.export;

import de.uniwue.dw.core.client.authentication.DataProtectionAPI;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.core.model.hooks.IDwCatalogHooks;
import de.uniwue.dw.query.model.DWQueryConfig;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.result.*;
import de.uniwue.dw.query.model.result.Cell.CellType;
import de.uniwue.misc.util.RegexUtil;
import de.uniwue.misc.util.StringUtilsUniWue;
import de.uniwue.misc.util.TimeUtil;
import org.htmlcleaner.CleanerProperties;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.SimpleHtmlSerializer;
import org.htmlcleaner.TagNode;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Timestamp;
import java.text.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

/**
 * The MemoryOutputHandler generates a Result-Object that stores the generated result data.
 */
public class MemoryOutputHandler implements IOutputHandler {

  /*
   * After the runnable has been executed (with "work()") this member holds the result-table object
   */
  private Result result = new Result();

  /*
   * This is the configuration file which determines how the export of the result takes place
   */
  protected ExportConfiguration exportConfig;

  protected QueryRunnable runnable;

  private int kAnonymity;

  public static MemoryOutputHandler defaultFormatter = new MemoryOutputHandler(null,
          new ExportConfiguration(ExportType.RESULT_TABLE), 0);

  public MemoryOutputHandler(QueryRunnable runnable, ExportConfiguration exportConfig,
          int kAnonymity) {
    this.runnable = runnable;
    this.exportConfig = exportConfig;
    this.kAnonymity = kAnonymity;
  }

  public OutputStream getOutputStream() throws IOException {
    return exportConfig.getOutputStream();
  }

  @Override
  public void setHeader(List<String> header) throws QueryException {
    result.setHeader(header);
  }

  @Override
  public void addRow(Row row) throws QueryException {
    result.addRow(row);
  }

  @Override
  public Result getResult() {
    return result;
  }

  @Override
  public void setResult(Result result) throws QueryException {
    this.result = result;
  }

  @Override
  public void close() throws QueryException {
    // ignore
  }

  @Override
  public void setDocsFound(long numFound) {
    result.setDocsFound(numFound);
  }

  @Override
  public void setQueryTime(long queryTime) {
    result.setQueryTime(queryTime);
  }

  @Override
  public void done() throws QueryException {
  }

  @Override
  public ExportConfiguration getExportConfiguration() {
    return exportConfig;
  }

  /**
   * Returns the value as a string. Data protection will be applied. Braces will be removed and the
   * exportType formatting rules will be applied.
   *
   * @return formatted value
   */
  public String getFormattedValue(Cell cell) {
    String valueString = applyKAnonymityIfNecessary(cell);
    if (valueString == null) {
      valueString = format(cell);
    }
    return valueString;
  }

  public String getFullText(Cell cell) {
    List<String> resultStrings = new ArrayList<>();
    for (Information anInfo : cell.getCellData().getValues()) {
      String oneInfoString;
      oneInfoString = formatResourceText(anInfo.getValue());
      resultStrings.add(oneInfoString);
    }
    return StringUtilsUniWue.concat(resultStrings, ", ");
  }

  private String format(Cell cell) {
    String result;
    List<String> resultStrings = new ArrayList<>();
    if (cell.cellType == CellType.Statistical) {
      String valueString = cell.value.toString();
      if (RegexUtil.numbersRegex.matcher(valueString).matches()) {
        result = formatNumber(Double.valueOf(cell.value.toString()));
      } else {
        result = valueString;
      }
    } else if (cell.cellType == CellType.Other) {
      if (cell.value == null) {
        cell.value = "";
      }
      result = formatTextForCell(cell.value.toString());
    } else {
      ResultCellData cellData = cell.getCellData();
      if ((cell.cellType == CellType.Value) && (cellData.attribute != null)
              && cellData.attribute.isOnlyDisplayExistence() && (cellData.getValues().size() > 0)) {
        if (cellData.attribute.getCatalogEntry().getDataType() == CatalogEntryType.Text) {
          boolean foundContent = false;
          for (Information anInfo : cellData.getValues()) {
            if (!anInfo.getValue().isBlank()) {
              foundContent = true;
              break;
            }
          }
          if (foundContent) {
            result = "X";
          } else {
            result = "";
          }
        } else {
          result = "X";
        }
      } else {
        for (Information anInfo : cellData.getValues()) {
          String oneInfoString = "";
          if (cell.cellType == CellType.Value) {
            CatalogEntry catalogEntry = cellData.attribute.getCatalogEntry();
            if (catalogEntry.getDataType() == CatalogEntryType.DateTime) {
              Date date = TimeUtil.parseDate(anInfo.getValueShort());
              if (date == null) {
                try {
                  DateFormat formater = DateFormat.getDateTimeInstance(DateFormat.SHORT,
                          DateFormat.MEDIUM, Locale.US);
                  date = formater.parse(anInfo.getValueShort());
                } catch (ParseException ignored) {
                }
              }
              oneInfoString = formatDate(date);
            } else if (cellData.attribute.getCatalogEntry()
                    .getDataType() == CatalogEntryType.Number) {
              String project = catalogEntry.getProject().toLowerCase();
              String extID = catalogEntry.getExtID().toLowerCase();
              if ((project.equals(IDwCatalogHooks.PROJECT_HOOK_PATIENT_ID)
                      && extID.equals(IDwCatalogHooks.EXT_HOOK_PATIENT_ID))
                      || (project.equals(IDwCatalogHooks.PROJECT_HOOK_CASE_ID)
                      && extID.equals(IDwCatalogHooks.EXT_HOOK_CASE_ID))) {
                oneInfoString = anInfo.getValueShort();
              } else {
                oneInfoString = Double.toString(anInfo.getValueDec());
              }
            } else {
              oneInfoString = formatTextForCell(anInfo.getValueShort());
            }
          } else if (cell.cellType == CellType.CaseID) {
            oneInfoString = formatNumber(anInfo.getCaseID());
          } else if (cell.cellType == CellType.PID) {
            oneInfoString = formatNumber(anInfo.getPid());
          } else if (cell.cellType == CellType.DocID) {
            oneInfoString = formatNumber(anInfo.getDocID());
          } else if (cell.cellType == CellType.MeasureTime) {
            Timestamp measureTime = anInfo.getMeasureTime();
            if (measureTime != null) {
              oneInfoString = formatDate(anInfo.getMeasureTime());
            }
          } else {
            throw new RuntimeException("Unknown Celltype");
          }
          resultStrings.add(oneInfoString);
        }
        result = StringUtilsUniWue.concat(resultStrings, " | ");
      }
    }
    return result;
  }

  private String applyKAnonymityIfNecessary(Cell aCell) {
    if ((getKAnonymity() > 0) && (aCell.cellType == CellType.Statistical)) {
      try {
        if (Double.parseDouble(aCell.value.toString()) <= getKAnonymity()) {
          return DataProtectionAPI.ANONYMISE_STRING;
        }
      } catch (NumberFormatException e) {
        // no numeric value
      }
    }
    return null;
  }

  private ExportType getExportType() {
    return exportConfig.getExportFileType();
  }

  private int getKAnonymity() {
    return kAnonymity;
  }

  public String formatNumber(Number value) {
    DecimalFormat formatter = (DecimalFormat) NumberFormat.getInstance(Locale.GERMANY);
    DecimalFormatSymbols symbols = formatter.getDecimalFormatSymbols();
    if (getThousandsSeparator() == null) {
      formatter.setGroupingUsed(false);
    } else {
      symbols.setGroupingSeparator(getThousandsSeparator());
    }
    symbols.setDecimalSeparator(getDecimalSeperator());
    formatter.setMaximumFractionDigits(getMaximumFractionDigits());
    formatter.setDecimalFormatSymbols(symbols);
    return formatter.format(value);
  }

  public String formatDate(Date o) {
    return getDateFormat().format(o);
  }

  private String formatTextForCell(String value) {
    return formatText(value, getLineBreakForCell());
  }

  public String formatResourceText(String value) {
    return formatText(value, getLineBreakForResource());
  }

  private String formatText(String value, String formateLineBreak) {
    if (value == null) {
      return null;
    }
    value = value.trim();
    value = value.replace(DWQueryConfig.queryHighlightPre(), getHitHighlightPre());
    value = value.replace(DWQueryConfig.queryHighlightPost(), getHitHighlightPost());
//    if (getExportType() == ExportType.CSV) {
//      value = value.replaceAll("(\n|\r)", formateLineBreak);
//    }
    if (getExportType() == ExportType.GUI) {
      value = cleanHTML(value);
      value = value.replace("\r\n", formateLineBreak).replace("\n", formateLineBreak).replace("\r", "");
    } else {
      value = value.replaceAll("<b>|</b>|<u>|</u>|<i>|</i>", "");
    }
    if (value.startsWith("@")) {
      // somehow Excel has a problem with long texts that start with a @, so they are removed in the export
      value = value.replaceAll("^@*", "");
    }
    return value;
  }

  private String cleanHTML(String dirtyHTML) {
    CleanerProperties props = new CleanerProperties();
    props.setOmitComments(true);
    props.setOmitXmlDeclaration(true);
    props.setOmitHtmlEnvelope(true);
    final SimpleHtmlSerializer htmlSerializer = new SimpleHtmlSerializer(props);
    TagNode tagNode = new HtmlCleaner(props).clean(dirtyHTML);
    return htmlSerializer.getAsString(tagNode);
  }

  private SimpleDateFormat getDateFormat() {
    return new SimpleDateFormat(TimeUtil.sdf_withTimeString);
  }

  private char getDecimalSeperator() {
    return ',';
  }

  private Character getThousandsSeparator() {
    if (getExportType() == ExportType.GUI) {
      return ' ';
    } else {
      return null;
    }
  }

  private int getMaximumFractionDigits() {
    if (getExportType() == ExportType.GUI) {
      return 2;
    } else {
      return 3;
    }
  }

  private String getHitHighlightPre() {
    if (getExportType() == ExportType.GUI) {
      return "<span style=\"" + DWQueryConfig.queryHighlightStyle() + "\">";
    } else {
      return ">>";
    }
  }

  private String getHitHighlightPost() {
    if (getExportType() == ExportType.GUI) {
      return "</span>";
    } else {
      return "<<";
    }
  }

  private String getLineBreakForCell() {
    return " ";
  }

  private String getLineBreakForResource() {
    return "<br>";
  }

}
