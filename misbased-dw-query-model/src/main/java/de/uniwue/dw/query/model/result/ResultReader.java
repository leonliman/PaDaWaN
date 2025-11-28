package de.uniwue.dw.query.model.result;

import java.util.Arrays;

public class ResultReader {

  public static Result readCSV(String content, String delimiter) {
    content = content.replaceAll("\r", "");
    String[] lines = content.split("\n");
    Result result = new Result();
    boolean first = true;
    for (String line : lines) {
      String[] cells = line.split(delimiter, -1);
      if (first) {
        result.setHeader(Arrays.asList(cells));
        first = false;
      } else {
        Row row = result.createNewRow();
        for (String cellString : cells) {
          Cell cell = row.createNewCell();
          cell.setValue(cellString);
        }
      }
    }
    return result;
  }

}
