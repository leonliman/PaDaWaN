package de.uniwue.dw.query.model.result.export;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import org.apache.poi.common.usermodel.HyperlinkType;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellRangeAddressList;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.awt.Color;
import java.io.*;
import java.util.List;
import java.util.*;

/**
 * @author fried
 */
public class GenericTable {

  public static final String FORMULA_INDICATOR = "**FORMULA**:";

  public static final double MAX_NUMERIC_VALUE_IN_EXCEL = Math.pow(10, 14);

  // how much difference is between the internal array index and the
  // "official" Excel index
  public static final int HEADER_OFFSET = 2;

  public static final short EXCEL_COLUMN_WIDTH_FACTOR = 256;

  public static final int UNIT_OFFSET_LENGTH = 7;

  public static final int[] UNIT_OFFSET_MAP = new int[] { 0, 36, 73, 109, 146, 182, 219 };

  private static final String CELL_COMMENT_AUTHOR = "";

  private static final String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  private final Hashtable<String, CellStyle> cellStyles = new Hashtable<String, CellStyle>();

  String[][] content;

  Map<Integer, HashMap<Integer, String>> links = new HashMap<Integer, HashMap<Integer, String>>();

  boolean showTitle = true;

  int height;

  int width;

  String title;

  boolean rotateFirstRow;

  boolean rotateSecondRow;

  boolean querformat;

  List<GroupInformation> groupInformations;

  List<MergeInformation> mergeInformations;

  List<ColumnWidthInformation> columnWidthInformations;

  List<FreezeInformation> freezedAreaInformations;

  List<CellInformation> cellInformations;

  List<String> commentLines;

  Map<Integer, Integer> rowHeights;

  boolean disableMinimumSize;

  boolean useMergedCellsOnAutoSizeColumn;

  List<File> images;

  boolean fitWidthToPage = false;

  private int actualLine;

  private int fontSize = 10;

  public GenericTable(int height, int width) {
    super();
    this.width = width;
    this.height = height;

    rotateFirstRow = false;
    title = "no title";
    content = new String[height][width];
    mergeInformations = new ArrayList<MergeInformation>();
    columnWidthInformations = new ArrayList<ColumnWidthInformation>();
    groupInformations = new ArrayList<GroupInformation>();
    freezedAreaInformations = new ArrayList<FreezeInformation>();
    cellInformations = new ArrayList<CellInformation>();
    rowHeights = new HashMap<Integer, Integer>();
    commentLines = new ArrayList<String>();
    images = new ArrayList<File>();
  }

  /**
   * @param i
   * @return column name in excel 0-indexed (0 -> A)
   */
  public static String c2s(int i) {
    // the original solution was 1-indexed, we are 0-indexed
    i++;

    int x = 0;
    int y = 0;
    int z = i;
    while (z > 26) {
      y++;
      z -= 26;
    }
    while (y > 26) {
      x++;
      y -= 26;
    }
    return "" + (x > 0 ? chars.charAt(x - 1) : "") + (y > 0 ? chars.charAt(y - 1) : "") + chars.charAt(z - 1);
  }

  public static void exportToXls(List<GenericTable> tables, OutputStream outputStream, boolean xlsx,
          ExportConfiguration exportConfiguration) throws IOException {
    Workbook wb;
    if (xlsx) {
      wb = new XSSFWorkbook();
    } else {
      wb = new HSSFWorkbook();
    }

    for (GenericTable gt : tables) {
      gt.generateSheet(wb, xlsx, exportConfiguration);
    }

    wb.write(outputStream);
  }

  public static void exportToXls(List<GenericTable> tables, String filename, boolean xlsx,
          ExportConfiguration exportConfiguration) throws IOException {
    File f = new File(filename);
    f.getParentFile().mkdirs();
    FileOutputStream fileOut;
    fileOut = new FileOutputStream(filename);
    exportToXls(tables, fileOut, xlsx, exportConfiguration);
    fileOut.close();
  }

  private static String getCellStyleKey(CellStyle style) {
    StringBuilder sb = new StringBuilder();
    String separator = "|||";
    sb.append(style.getFontIndex());
    sb.append(separator);
    sb.append(style.getAlignment().toString());
    sb.append(separator);
    sb.append(style.getDataFormat());
    sb.append(separator);
    sb.append(style.getFillBackgroundColor());
    sb.append(separator);
    sb.append(style.getFillForegroundColor());
    sb.append(separator);
    sb.append(style.getFillPattern().toString());
    sb.append(separator);
    sb.append(style.getFontIndex());
    sb.append(separator);
    sb.append(style.getIndention());
    sb.append(separator);
    sb.append(style.getRotation());
    sb.append(separator);
    sb.append(style.getVerticalAlignment().toString());
    sb.append(separator);
    sb.append(style.getWrapText());
    sb.append(separator);
    sb.append(style.getHidden());
    sb.append(separator);
    sb.append(style.getLocked());
    sb.append(separator);
    sb.append(style.getBorderBottom().toString());
    if (style instanceof XSSFCellStyle) {
      sb.append(separator);
      XSSFCellStyle xStyle = (XSSFCellStyle) style;
      sb.append(getXSSFColorIdentificatorString(xStyle.getFillBackgroundXSSFColor()));
      sb.append(separator);
      sb.append(getXSSFColorIdentificatorString(xStyle.getFillForegroundXSSFColor()));
    }
    return sb.toString();
  }

  private static String getXSSFColorIdentificatorString(XSSFColor color) {
    if (color == null)
      return "null";
    StringBuilder sb = new StringBuilder();
    String separator = "|||";
    byte[] b = color.getRGB();
    sb.append(b[0]);
    sb.append(separator);
    sb.append(b[1]);
    sb.append(separator);
    sb.append(b[2]);
    return sb.toString();
  }

  public static void main(String[] args) throws IOException {
    String filename = "E:\\test.xlsx";
    GenericTable table = new GenericTable(5, 5);
    // table.setContent(0, 0, "Hallo");
    // table.addCellComment(1, 1, "This is my Test\nComment!");
    // table.addGroupInformation(1, 3, true);
    // table.setContent(2, 2, "Jetzt neu in Farbe!");
    // Color color = new Color(1.0f, 0f, 0f);
    // table.addBackgroundColorInformation(1, 1, color);
    //
    // table.setContent(3, 3, "Grenze");
    // table.setContent(3, 1, "2");
    // table.setContent(4, 0, "Formel:");
    // table.setContent(4, 1, FORMULA_INDICATOR + "B5+2");
    // table.addRowHeightInformation(1, 100);
    // // table.addImage(new File("C:/test/som.png"));
    // table.setContent(5, 0, "5");
    // table.setContent(5, 1, "JA");
    // table.setContent(5, 2, "NEIN");
    // table.setContent(6, 0, FORMULA_INDICATOR + "IF(A7>3,B7,C7)");
    // table.addWrapInformation(0, 5, true);
    // table.setContent(0, 5, "Das ist ein \n Zeilenumbruch");
    // table.addWrapInformation(1, 5, true);
    table.setContent(0, 0, "111092");

    table.shrink();
    table.exportToXLS(filename, true, new ExportConfiguration());
  }

  /**
   * Background Colors are only generated for XLSX documents
   */
  public void addBackgroundColorInformation(int row, int column, Color color) {
    cellInformations.add(new BackgroundColorInformation(row, column, color));
  }

  public void addCellComment(int row, int column, String text) {
    cellInformations.add(new CellComment(row, column, text));
  }

  public void addColumnWidthInformation(int col, boolean max, int width) {
    columnWidthInformations.add(new ColumnWidthInformation(col, max, width));
  }

  public void addCommentLine(String line) {
    commentLines.add(line);
  }

  public void addDataFormatInformation(int row, int col, boolean inPercent) {
    cellInformations.add(new DataFormatInformation(inPercent, row, col));
  }

  public void addDecorationInformation(DecorationInformation di) {
    cellInformations.add(di);
  }

  public void addFontColorInformation(int row, int column, IndexedColors color) {
    cellInformations.add(new FontColorInformation(row, column, color));
  }

  public void addDropDownInformation(int row, int column, String[] values) {
    cellInformations.add(new DropDownInformation(row, column, values));
  }

  public void addFreezeInformation(FreezeInformation fi) {
    freezedAreaInformations.add(fi);
  }

  public void addGroupInformation(int from, int to, boolean isRow) {
    if (to >= from) {
      groupInformations.add(new GroupInformation(from, to, isRow));
    }
  }

  public void addImage(File imageFile) {
    images.add(imageFile);
  }

  public void addMergeInformation(int rowFrom, int rowTo, int columnFrom, int columnTo) {
    mergeInformations.add(new MergeInformation(rowFrom, rowTo, columnFrom, columnTo));
  }

  public void addRowHeightInformation(Integer row, Integer height) {
    rowHeights.put(row, height);
  }

  public void addWrapInformation(int row, int column, boolean wrap) {
    cellInformations.add(new WrapInformation(row, column, wrap));
  }

  public void colorColumn(int column, Color color) {
    for (int i = 0; i < height; i++) {
      addBackgroundColorInformation(i, column, color);
    }
  }

  public void colorRow(int row, Color color) {
    for (int i = 0; i < width; i++) {
      addBackgroundColorInformation(row, i, color);
    }
  }

  private CellStyle createStdCellStyle(Workbook wb) {
    CellStyle stdStyle = wb.createCellStyle();
    Font font = wb.createFont();
    font.setFontHeightInPoints((short) fontSize);
    stdStyle.setFont(font);
    DataFormat fmt = wb.createDataFormat();
    stdStyle.setDataFormat(fmt.getFormat("@"));
    // borders
    stdStyle.setBorderBottom(BorderStyle.THIN);
    stdStyle.setBottomBorderColor(IndexedColors.BLACK.getIndex());
    stdStyle.setBorderLeft(BorderStyle.THIN);
    stdStyle.setLeftBorderColor(IndexedColors.BLACK.getIndex());
    stdStyle.setBorderRight(BorderStyle.THIN);
    stdStyle.setRightBorderColor(IndexedColors.BLACK.getIndex());
    stdStyle.setBorderTop(BorderStyle.THIN);
    stdStyle.setTopBorderColor(IndexedColors.BLACK.getIndex());
    return stdStyle;
  }

  public void expand(int newHeight, int newWidth) {

    // copy contents to new array
    String[][] newContent = new String[newHeight][newWidth];
    for (int i = 0; i < height; i++) {
      for (int j = 0; j < width; j++) {
        newContent[i][j] = content[i][j];
      }
    }
    this.width = newWidth;
    this.height = newHeight;
    this.content = newContent;
  }

  public void exportToCSV(String filename) {
    try {
      File f = new File(filename);
      f.getParentFile().mkdirs();

      FileWriter fstream = new FileWriter(filename);
      BufferedWriter out = new BufferedWriter(fstream);
      for (int i = 0; i < height; i++) {
        for (int j = 0; j < width; j++) {
          String cell = content[i][j];
          if (cell == null) {
            cell = "";
          }
          out.write(cell);
          if (j < width - 1) {
            out.write(";");
          } else {
            out.write("\n");
          }
        }
      }

      // Close the output stream
      out.close();
    } catch (Exception e) {// Catch exception if any
      e.printStackTrace();
    }
  }

  public void exportToXLS(OutputStream outSteam, boolean xlsx, ExportConfiguration exportConfiguration)
          throws IOException {
    GenericTable.exportToXls(Collections.singletonList(this), outSteam, xlsx, exportConfiguration);
  }

  public void exportToXLS(String filename, boolean xlsx, ExportConfiguration exportConfiguration) throws IOException {
    GenericTable.exportToXls(Collections.singletonList(this), filename, xlsx, exportConfiguration);
  }

  private Sheet generateSheet(Workbook wb, boolean xlsx, ExportConfiguration exportConfiguration) throws IOException {
    String sheetTitle = title.substring(0, Math.min(title.length(), 30)).replaceAll(" ", "");
    sheetTitle = sheetTitle.replaceAll(":", "-");
    if (exportConfiguration.getExcelSheetName() != null) {
      sheetTitle = exportConfiguration.getExcelSheetName();
    }
    Sheet sheet = wb.createSheet(sheetTitle);
    CreationHelper factory = wb.getCreationHelper();

    int y_offset = isShowTitle() ? 1 : 0;

    // create the style for all standard textCells:
    CellStyle stdStyle = createStdCellStyle(wb);

    CellStyle rotatedStyle = wb.createCellStyle();
    rotatedStyle.cloneStyleFrom(stdStyle);
    rotatedStyle.setRotation((short) 30);

    for (int i = 0; i < height; i++) {
      Row row = sheet.createRow(i + y_offset);

      if (rowHeights != null && rowHeights.containsKey(i)) {
        row.setHeightInPoints(rowHeights.get(i));
      }
      // System.out.println("height: " + height + "; width: " + width);
      for (int j = 0; j < width; j++) {
        String cellValue = content[i][j];
        // if (cellValue != null) {
        Cell cell = row.createCell(j);

        if (cellValue != null && cellValue.startsWith(FORMULA_INDICATOR)) {
          cell.setCellType(CellType.FORMULA);
          String cellFormulaString = cellValue.substring(FORMULA_INDICATOR.length());
          cell.setCellFormula(cellFormulaString);
          setCellStyle(cell, stdStyle);
        } else {

          // if (isNumber(cellValue)) {
          // cell.setCellValue(getNumber(cellValue));
          // cell.setCellType(Cell.CELL_TYPE_NUMERIC);
          // } else {
          cell.setCellType(CellType.STRING);
          if (cellValue != null && cellValue.length() > wb.getSpreadsheetVersion().getMaxTextLength() - 100) {
            if (exportConfiguration.isExcelShortenLongTextContent()) {
              cellValue = cellValue.substring(0, wb.getSpreadsheetVersion().getMaxTextLength() - 100);
            } else {
              throw new IOException(
                      "This export exceeds the maximum length (" + wb.getSpreadsheetVersion().getMaxTextLength()
                              + " characters) of content  for a single Excel-cell. "
                              + "Please change your export settings or use another export format.");
            }
          }
          cell.setCellValue(cellValue);
          // }

          setCellStyle(cell, stdStyle);

          if (links.containsKey(i) && links.get(i).containsKey(j)) {
            Hyperlink link = factory.createHyperlink(HyperlinkType.FILE);
            link.setAddress(links.get(i).get(j));
            cell.setHyperlink(link);
          }
        }
        if (rotateFirstRow && (i == 0)) {
          setCellStyle(row.getCell(j), rotatedStyle);
        }
        if (rotateSecondRow && (i == 1)) {
          setCellStyle(row.getCell(j), rotatedStyle);
        }
        // }
      }
    }

    // merged cells
    for (MergeInformation m : mergeInformations) {
      sheet.addMergedRegion(new CellRangeAddress(m.rowFrom + y_offset, m.rowTo + y_offset, m.columnFrom, m.columnTo));
    }

    // decoration

    // this is for cellComments!
    Drawing drawing = sheet.createDrawingPatriarch();
    Font commentFont = wb.createFont();
    commentFont.setFontHeight((short) 160);

    for (CellInformation cellInfo : cellInformations) {
      // illegal cell information
      if (cellInfo.row >= height || cellInfo.col >= width) {
        continue;
      }

      if (cellInfo instanceof DropDownInformation) {
        DataValidationHelper dvHelper = sheet.getDataValidationHelper();
        CellRangeAddressList addressList = new CellRangeAddressList(cellInfo.row, cellInfo.row, cellInfo.col,
                cellInfo.col);
        DataValidationConstraint constraint = dvHelper
                .createExplicitListConstraint(((DropDownInformation) cellInfo).values);
        DataValidation dataValidation = dvHelper.createValidation(constraint, addressList);
        dataValidation.setSuppressDropDownArrow(true);
        sheet.addValidationData(dataValidation);
        continue;
      }

      Row row = sheet.getRow(cellInfo.row + y_offset);
      if (cellInfo instanceof CellComment) {
        Cell cell = row.getCell(cellInfo.col);
        ClientAnchor anchor = factory.createClientAnchor();
        Comment comment = drawing.createCellComment(anchor);
        RichTextString str = factory.createRichTextString(((CellComment) cellInfo).text);
        str.applyFont(commentFont);
        comment.setString(str);
        comment.setAuthor(CELL_COMMENT_AUTHOR);
        cell.setCellComment(comment);
      }

      CellStyle style = wb.createCellStyle();
      style.cloneStyleFrom(row.getCell(cellInfo.col).getCellStyle());

      if (cellInfo instanceof DataFormatInformation) {
        if (((DataFormatInformation) cellInfo).isPercent)
          style.setDataFormat(wb.createDataFormat().getFormat("0%"));
      }
      if (cellInfo instanceof WrapInformation) {
        style.setWrapText(((WrapInformation) cellInfo).wrap);
      }

      Font prevFont = wb.getFontAt(style.getFontIndex());
      boolean bold = prevFont.getBold();
      short color = prevFont.getColor();
      short fontheight = prevFont.getFontHeight();
      String fontName = prevFont.getFontName();
      boolean italic = prevFont.getItalic();
      boolean strikeout = prevFont.getStrikeout();
      short typeOffset = prevFont.getTypeOffset();
      byte underline = prevFont.getUnderline();

      if (cellInfo instanceof DecorationInformation) {
        DecorationInformation decInfo = (DecorationInformation) cellInfo;
        bold = decInfo.bold;
        italic = decInfo.italic;
        underline = decInfo.underlined ? Font.U_SINGLE : Font.U_NONE;
      }
      if (cellInfo instanceof FontColorInformation) {
        FontColorInformation coloInfo = (FontColorInformation) cellInfo;
        color = coloInfo.color.getIndex();
      }

      Font reqFont = wb.findFont(bold, color, fontheight, fontName, italic, strikeout, typeOffset, underline);
      if (reqFont == null) {
        reqFont = wb.createFont();
        reqFont.setBold(bold);
        reqFont.setColor(color);
        reqFont.setFontHeight(fontheight);
        reqFont.setFontName(fontName);
        reqFont.setItalic(italic);
        reqFont.setStrikeout(strikeout);
        reqFont.setTypeOffset(typeOffset);
        reqFont.setUnderline(underline);
      }

      style.setFont(reqFont);
      if (xlsx && cellInfo instanceof BackgroundColorInformation) {
        BackgroundColorInformation coloInfo = (BackgroundColorInformation) cellInfo;
        ((XSSFCellStyle) style).setFillForegroundColor(new XSSFColor(coloInfo.color, null));
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
      }
      setCellStyle(row.getCell(cellInfo.col), style);
    }

    // group cells
    sheet.setRowSumsBelow(false);
    sheet.setRowSumsRight(false);
    // sort in a way excel can handle:
    // outer groups before innergroups.
    Collections.sort(groupInformations);
    for (GroupInformation gi : groupInformations) {
      if (gi.isRow) {
        sheet.groupRow(gi.from + y_offset, gi.to + y_offset);
      } else {
        sheet.groupColumn(gi.from, gi.to);
      }
    }

    if (exportConfiguration.getExcelDefaultColumnWidth() != null &&
            exportConfiguration.getExcelDefaultColumnWidth() > 0) {
      int defaultColumnWidthToUse = exportConfiguration.getExcelDefaultColumnWidth() * 256 + 213;
      for (int j = 0; j < width; j++) {
        sheet.setColumnWidth(j, defaultColumnWidthToUse);
      }
    } else {
      // auto resize columns
      for (int j = 0; j < width; j++) {
        sheet.autoSizeColumn(j, useMergedCellsOnAutoSizeColumn);
      }
    }
    if (!disableMinimumSize) {
      for (int j = 0; j < width; j++) {
        if (sheet.getColumnWidth(j) < 900) {
          sheet.setColumnWidth(j, 900);
        }
      }
    }

    // set maximum/minimum Width for columns according to the specific
    // informations
    for (ColumnWidthInformation info : columnWidthInformations) {
      if (info.max) {
        if (sheet.getColumnWidth(info.col) > info.width) {
          sheet.setColumnWidth(info.col, info.width);
        }
      } else {
        if (sheet.getColumnWidth(info.col) < info.width) {
          sheet.setColumnWidth(info.col, info.width);
        }
      }
    }

    // set cellType for numeric cells
    for (Row row : sheet) {
      for (Cell cell : row) {
        if (cell.getCellType() != CellType.FORMULA) {
          String cellValue = cell.getStringCellValue();
          if (isNumber(cellValue)) {
            // das würde LONG-Zahlen runden was nicht erwünscht ist.
            double number = getNumber(cellValue);
            if (number <= MAX_NUMERIC_VALUE_IN_EXCEL) {
              cell.setCellValue(number);
              cell.setCellType(CellType.NUMERIC);
            }
          }
        }
      }
    }

    // crate commentLines
    for (int i = 0; i < commentLines.size(); i++) {
      int rowNo = i + height + 1 + y_offset;
      Row row = sheet.createRow(rowNo);
      Cell cell = row.createCell(0);
      cell.setCellValue(commentLines.get(i));
      cell.setCellType(CellType.STRING);
    }

    // page Setup
    PrintSetup ps = sheet.getPrintSetup();
    if (isQuerformat()) {
      ps.setLandscape(true);
    }

    if (isFitWidthToPage()) {
      ps.setFitWidth((short) 1);
    }

    // add title last, because of autoSizeColumns
    if (isShowTitle()) {
      CellStyle titleStyle = wb.createCellStyle();
      Font titleFont = wb.createFont();
      titleFont.setBold(true);
      titleFont.setFontHeightInPoints((short) 14);
      titleStyle.setFont(titleFont);
      sheet.addMergedRegion(new CellRangeAddress(0, 0, 0, width - 1));
      Row titleRow = sheet.createRow(0);
      Cell titleCell = titleRow.createCell(0);
      titleCell.setCellValue(title);
      setCellStyle(titleCell, titleStyle);
    }

    for (FreezeInformation fi : freezedAreaInformations) {
      sheet.createFreezePane(fi.colSplit, fi.rowSplit + y_offset);
    }

    // add images
    CreationHelper helper = wb.getCreationHelper();
    for (File imageFile : images) {
      // add picture data to this workbook.
      InputStream is = new FileInputStream(imageFile);
      byte[] bytes = IOUtils.toByteArray(is);
      int pictureIdx = wb.addPicture(bytes, Workbook.PICTURE_TYPE_JPEG);
      is.close();

      // Create the drawing patriarch. This is the top level container for
      // all shapes.
      // add a picture shape
      ClientAnchor anchor = helper.createClientAnchor();
      // set top-left corner of the picture,
      // subsequent call of Picture#resize() will operate relative to it
      anchor.setCol1(1);
      anchor.setRow1(height + 1);
      Picture pict = drawing.createPicture(anchor, pictureIdx);

      // auto-size picture relative to its top-left corner
      pict.resize();
    }
    return sheet;
  }

  public String[][] getContent() {
    return content;
  }

  public String getContent(int x, int y) {
    return content[x][y];
  }

  public int getFontSize() {
    return fontSize;
  }

  public void setFontSize(int fontSize) {
    this.fontSize = fontSize;
  }

  public List<GroupInformation> getGroupInformations() {
    return groupInformations;
  }

  public int getHeight() {
    return height;
  }

  private int[] getMaxUsedDimensions() {
    int maxHeight = 1;
    int maxWidth = 1;
    for (int i = 0; i < height; i++) {
      for (int j = 0; j < width; j++) {
        if (content[i][j] != null && !content[i][j].isEmpty()) {
          maxHeight = i + 1;
          maxWidth = Math.max(j + 1, maxWidth);
        }
      }
    }
    return new int[] { maxHeight, maxWidth };
  }

  // public void expandToSize(int y, int x) {
  // if (y < this.height) {
  // throw new IllegalArgumentException();
  // }
  // if (x < this.width) {
  // throw new IllegalArgumentException();
  // }
  // String[][] newContent = new String[y][x];
  // }

  public List<MergeInformation> getMergeInformations() {
    return mergeInformations;
  }

  private double getNumber(String cellValue) {
    return Double.parseDouble(cellValue.replaceAll(",", "."));
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title.replace("?", "-");
  }

  public int getWidth() {
    return width;
  }

  public boolean isDisableMinimumSize() {
    return disableMinimumSize;
  }

  public void setDisableMinimumSize(boolean disableMinimumSize) {
    this.disableMinimumSize = disableMinimumSize;
  }

  public boolean isFitWidthToPage() {
    return fitWidthToPage;
  }

  public void setFitWidthToPage(boolean fitWidthToPage) {
    this.fitWidthToPage = fitWidthToPage;
  }

  private boolean isNumber(String cellValue) {
    if (cellValue == null) {
      return false;
    }
    try {
      Double.parseDouble(cellValue.replaceAll(",", "."));
      return true;
    } catch (NumberFormatException nfe) {
      return false;
    }
  }

  public boolean isQuerformat() {
    return querformat;
  }

  public void setQuerformat(boolean querformat) {
    this.querformat = querformat;
  }

  public boolean isRotateFirstRow() {
    return rotateFirstRow;
  }

  public void setRotateFirstRow(boolean rotateFirstRow) {
    this.rotateFirstRow = rotateFirstRow;
  }

  public boolean isRotateSecondRow() {
    return rotateSecondRow;
  }

  public void setRotateSecondRow(boolean rotateSecondRow) {
    this.rotateSecondRow = rotateSecondRow;
  }

  public boolean isShowTitle() {
    return showTitle;
  }

  public void setShowTitle(boolean showTitle) {
    this.showTitle = showTitle;
  }

  public void removeRowHeightInformation(Integer row) {
    rowHeights.remove(row);
  }

  /**
   * Each _exact_occurence of the first String in a cell is replaced by the second String
   */
  public void replace(String toReplace, String replaceBy) {
    for (int i = 0; i < height; i++) {
      for (int j = 0; j < width; j++) {
        if (content[i][j] != null && content[i][j].equals(toReplace)) {
          content[i][j] = replaceBy;
        }
      }
    }
  }

  private void setCellStyle(Cell c, CellStyle style) {
    String key = getCellStyleKey(style);
    CellStyle knownStyle = cellStyles.get(key);
    if (knownStyle == null) {
      cellStyles.put(key, style);
      knownStyle = style;
    }
    c.setCellStyle(knownStyle);
  }

  public void setLink(int y, int x, String displayString, String url) {
    setContent(y, x, displayString);
    if (!links.containsKey(y)) {
      links.put(y, new HashMap<Integer, String>());
    }
    links.get(y).put(x, url);
    addFontColorInformation(y, x, IndexedColors.BLUE);
  }

  public void setContent(int y, int x, String cellValue) {
    if (y >= height) {
      expand(getHeight() * 2, getWidth());
      // throw new ArrayIndexOutOfBoundsException(
      // " y-value too high: " + y + ", maxHeight was: " + (height - 1));
    }
    if (x >= width) {
      expand(getHeight(), getWidth() * 2);
      // throw new ArrayIndexOutOfBoundsException(
      // " x-value too high: " + x + ", max-Width was: " + (width - 1));
    }
    content[y][x] = cellValue;
  }

  /**
   * Generate Excel file with all children of CatalogEntry.
   *
   * @param catalogEntry
   * @param childCount   Count Children to indent
   */
  public void setCatalogEntryToContent(CatalogEntry catalogEntry, int childCount) {
    resetActualLine();
    HashMap<Integer, GroupInformation> groupList = new HashMap<Integer, GroupInformation>();
    this.setContent(0, 0, "Catalog Entry Format: Name | Project | ExtId | DataType | AttrID (Don't Change this!)");
    this.addFontColorInformation(0, 0, IndexedColors.RED);

    setCatalogEntryToContentRec(catalogEntry, childCount, groupList);

    for (Map.Entry<Integer, GroupInformation> entry : groupList.entrySet()) {
      if (entry.getValue().to == -1)
        continue;

      addGroupInformation(entry.getValue().from, entry.getValue().to, entry.getValue().isRow);
    }
  }

  private void setCatalogEntryToContentRec(CatalogEntry catalogEntry, int childCount,
          HashMap<Integer, GroupInformation> groupList) {
    if (!catalogEntry.isRoot()) {
      if (catalogEntry.getParent().getChildren().get(0) == catalogEntry) {
        groupList.put(catalogEntry.getParentID(), new GroupInformation(actualLine, -1, true));
      }
      if (catalogEntry.getAttrId() == 51969)
        System.out.println();

      if (catalogEntry.getParent().getLastChild() == catalogEntry) {
        int from = groupList.get(catalogEntry.getParentID()).from;
        groupList.put(catalogEntry.getParentID(), new GroupInformation(from, actualLine, true));
      }

      this.setContent(actualLine, childCount, catalogEntry.getName());
      this.setContent(actualLine, childCount + 1, catalogEntry.getProject());
      this.setContent(actualLine, childCount + 2, catalogEntry.getExtID());
      this.setContent(actualLine, childCount + 3, catalogEntry.getDataType().name());
      this.setContent(actualLine, childCount + 4, catalogEntry.getAttrId() + "");
      this.addFontColorInformation(actualLine, childCount + 4, IndexedColors.DARK_RED);

      List<String> values = new ArrayList<String>();
      for (CatalogEntryType entryType : CatalogEntryType.values()) {
        values.add(entryType.name());
      }
      this.addDropDownInformation(actualLine + 1, childCount + 3, values.toArray(new String[values.size()]));
    } else {
      actualLine--;
      childCount--;
    }

    for (CatalogEntry childEntry : catalogEntry.getChildren()) {
      actualLine++;
      setCatalogEntryToContentRec(childEntry, childCount + 1, groupList);
    }
  }

  private void resetActualLine() {
    this.actualLine = 1;
  }

  public void shrink() {
    int[] maxUsedDimensions = getMaxUsedDimensions();
    shrink(maxUsedDimensions[0], maxUsedDimensions[1]);
  }

  /**
   * Shrinks the dimensions of this table to the new given dimension. These must be equal or smaller
   * than the original ones.
   *
   * @param newHeight
   * @param newWidth
   */
  public void shrink(int newHeight, int newWidth) {

    // check sizes
    if (newWidth > width || newHeight > height) {
      throw new IllegalArgumentException("Cannot shrink to larger dimensions");
    }
    this.width = newWidth;
    this.height = newHeight;

    // copy contents to new array
    String[][] newContent = new String[newHeight][newWidth];
    for (int i = 0; i < newHeight; i++) {
      for (int j = 0; j < newWidth; j++) {
        newContent[i][j] = content[i][j];
      }
    }

    // set the new array as content
    this.content = newContent;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < height; i++) {
      for (int j = 0; j < width; j++) {
        String cell = content[i][j];
        if (cell == null) {
          cell = "";
        }
        sb.append(cell);
        if (j < width - 1) {
          sb.append(";");
        } else {
          sb.append("\n");
        }
      }
    }
    return sb.toString();
  }

  /**
   * Background Colors are only generated for XLSX documents
   */
  public static class BackgroundColorInformation extends CellInformation {
    Color color;

    public BackgroundColorInformation(int row, int col, Color color) {
      super(row, col);
      this.color = color;
    }
  }

  public static class CellComment extends CellInformation {
    String text;

    public CellComment(int row, int col, String text) {
      super(row, col);
      this.text = text;
    }
  }

  public static class CellInformation {
    int row;

    int col;

    public CellInformation(int row, int col) {
      super();
      this.col = col;
      this.row = row;
    }
  }

  public static class ColumnWidthInformation {
    int col;

    boolean max;

    int width;

    /**
     * Welche Spalte, maximum oderr minimum, wie breit?
     */
    private ColumnWidthInformation(int col, boolean max, int width) {
      super();
      this.col = col;
      this.max = max;
      this.width = width;
    }
  }

  public static class DataFormatInformation extends CellInformation {
    boolean isPercent;

    public DataFormatInformation(boolean inPercent, int row, int col) {
      super(row, col);
      isPercent = inPercent;
    }
  }

  public static class DecorationInformation extends CellInformation {
    boolean bold;

    boolean italic;

    boolean underlined;

    public DecorationInformation(int row, int col, boolean bold, boolean italic, boolean underlined) {
      super(row, col);
      this.bold = bold;
      this.italic = italic;
      this.underlined = underlined;
    }
  }

  public static class FontColorInformation extends CellInformation {
    IndexedColors color;

    public FontColorInformation(int row, int col, IndexedColors color) {
      super(row, col);
      this.color = color;
    }
  }

  public static class DropDownInformation extends CellInformation {
    String[] values;

    public DropDownInformation(int row, int col, String[] values) {
      super(row, col);
      this.values = values;
    }
  }

  public static class FreezeInformation {
    int colSplit;

    int rowSplit;

    public FreezeInformation(int colSplit, int rowSplit) {
      super();
      this.colSplit = colSplit;
      this.rowSplit = rowSplit;
    }
  }

  public static class GroupInformation implements Comparable<GroupInformation> {
    int from;

    int to;

    boolean isRow;

    public GroupInformation(int from, int to, boolean isRow) {
      this.from = from;
      this.to = to;
      this.isRow = isRow;
    }

    @Override
    public int compareTo(GroupInformation o) {
      if (from == o.from) {
        return o.to - to;
      }
      return from - o.from;
    }
  }

  public static class MergeInformation {
    int rowFrom;

    int rowTo;

    int columnFrom;

    int columnTo;

    public MergeInformation(int rowFrom, int rowTo, int columnFrom, int columnTo) {
      this.rowFrom = rowFrom;
      this.rowTo = rowTo;
      this.columnFrom = columnFrom;
      this.columnTo = columnTo;
    }
  }

  public static class WrapInformation extends CellInformation {
    boolean wrap;

    public WrapInformation(int row, int col, boolean wrap) {
      super(row, col);
      this.wrap = wrap;
    }
  }

}
