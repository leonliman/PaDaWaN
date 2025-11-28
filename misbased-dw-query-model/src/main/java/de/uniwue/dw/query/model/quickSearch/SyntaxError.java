package de.uniwue.dw.query.model.quickSearch;

public class SyntaxError {
  /**
   * Fehler die sich auf die ganze Zeile beziehen haben einen Positiionswert von -1.
   */
  private int position;

  private String error;

  public SyntaxError(int position, String error) {
    this.position = position;
    this.error = error;
  }

  public int getPosition() {
    return position;
  }

  public void setPosition(int position) {
    this.position = position;
  }

  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }

}
