package de.uniwue.dw.query.model.result;

import java.util.HashMap;

import de.uniwue.dw.query.model.result.export.MemoryOutputHandler;

public class Cell {

  private Row row;

  private int columnNumber;

  private ResultCellData cellData;

  // class fields should be private. :-/
  public String toolTip;

  public CellType cellType;

  public Object value;

  private HashMap<String, Object> key2value = new HashMap<String, Object>();

  public enum CellType {
    Value, CaseID, PID, MeasureTime, DocID, Statistical, Other
  }

  public Cell(Row row, int columnNumber) {
    this(row, columnNumber, null, CellType.Other);
  }

  public Cell(Row row, int columnNumber, ResultCellData aCellData, CellType aCellType) {
    this.row = row;
    this.columnNumber = columnNumber;
    cellData = aCellData;
    cellType = aCellType;
  }

  public ResultCellData getCellData() {
    return cellData;
  }

  public Row getRow() {
    return row;
  }

  public Object get(String key) {
    return key2value.get(key);
  }

  public void put(String key, Object value) {
    if (value != null) {
      this.key2value.put(key, value);
    }
  }

  public int getColumnNumber() {
    return columnNumber;
  }

  public String getColumnName() {
    return row.getResult().getHeader().get(columnNumber);
  }

  public String getValueAsString() {
    return MemoryOutputHandler.defaultFormatter.getFormattedValue(this);
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  @Override
  public String toString() {
    if (cellData != null) {
      return cellData.toString();
    }
    if (getValue() == null) {
      return "";
    } else {
      return getValue().toString();
    }
  }

  // @Override
  // public int hashCode() {
  // final int prime = 31;
  // int result = 1;
  // result = prime * result + columnNumber;
  // result = prime * result + ((key2value == null) ? 0 : key2value.hashCode());
  // result = prime * result + ((row == null) ? 0 : row.hashCode());
  // return result;
  // }
  //
  // @Override
  // public boolean equals(Object obj) {
  // if (this == obj)
  // return true;
  // if (obj == null)
  // return false;
  // if (getClass() != obj.getClass())
  // return false;
  // Cell other = (Cell) obj;
  // if (columnNumber != other.columnNumber)
  // return false;
  // if (key2value == null) {
  // if (other.key2value != null)
  // return false;
  // } else if (!key2value.equals(other.key2value))
  // return false;
  // if (row == null) {
  // if (other.row != null)
  // return false;
  // } else if (!row.equals(other.row))
  // return false;
  // return true;
  // }

}
