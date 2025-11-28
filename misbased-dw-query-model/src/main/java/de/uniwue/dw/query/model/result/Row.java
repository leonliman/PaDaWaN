package de.uniwue.dw.query.model.result;

import java.util.ArrayList;

import de.uniwue.dw.query.model.result.Cell.CellType;

public class Row {

  private ArrayList<Cell> cells = new ArrayList<>();

  private long pid;

  private Result result;

  private int rowNumber;

  public Row() {
  }

  Row(Result result, int rowNumber) {
    this.result = result;
  }

  public ArrayList<Cell> getCells() {
    return cells;
  }

  public void setCells(ArrayList<Cell> cells) {
    this.cells = cells;
  }

  public long getPid() {
    return pid;
  }

  public void setPid(long pid) {
    this.pid = pid;
  }

  public Cell getCell(int i) {
    return cells.get(i);
  }

  public void setPid(Object patientID) {
    if (patientID == null)
      setPid(-1);
    else {
      String id = patientID.toString();
      long idLong = Long.parseLong(id);
      setPid(idLong);
    }
  }

  public StatisticalCell createNewStatisticalCell() {
    StatisticalCell c = new StatisticalCell(this, cells.size());
    cells.add(c);
    return c;
  }

  public Cell createNewCell(ResultCellData aCellData, CellType aCellType) {
    Cell c = new Cell(this, cells.size(), aCellData, aCellType);
    cells.add(c);
    return c;
  }

  public Cell createNewCell() {
    Cell c = new Cell(this, cells.size());
    cells.add(c);
    return c;
  }

  public Result getResult() {
    return result;
  }

  public void setResult(Result result) {
    this.result = result;
  }

  /**
   * First Row has the number 0.
   * 
   * @return
   */
  public int getRowNumber() {
    return rowNumber;
  }

  /**
   * First Row has the number 0.
   * 
   */
  public void setRowNumber(int rowNumber) {
    if (rowNumber < 0)
      throw new IllegalArgumentException();
    this.rowNumber = rowNumber;
  }

  public void remove() {
    getResult().removeRow(this);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((cells == null) ? 0 : cells.hashCode());
    result = prime * result + (int) (pid ^ (pid >>> 32));
    result = prime * result + ((this.result == null) ? 0 : this.result.hashCode());
    result = prime * result + rowNumber;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Row other = (Row) obj;
    if (cells == null) {
      if (other.cells != null)
        return false;
    } else if (!cells.equals(other.cells))
      return false;
    if (pid != other.pid)
      return false;
    if (result == null) {
      if (other.result != null)
        return false;
    } else if (!result.equals(other.result))
      return false;
    if (rowNumber != other.rowNumber)
      return false;
    return true;
  }

  public String toString() {
    return this.cells.toString();
  }

}
