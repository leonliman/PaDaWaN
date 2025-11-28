package de.uniwue.dw.query.model.quickSearch;

import java.util.ArrayList;
import java.util.List;

public class FormattingResult {

  private String formattedLine;

  private List<SyntaxError> errors;

  public FormattingResult(String formattedLine, List<SyntaxError> errors) {
    this.formattedLine = formattedLine;
    this.errors = errors;
  }

  public FormattingResult(String formattedLine) {
    this(formattedLine, new ArrayList<>());
  }

  public String getFormattedLine() {
    return formattedLine;
  }

  public void setFormattedLine(String formattedLine) {
    this.formattedLine = formattedLine;
  }

  public List<SyntaxError> getErrors() {
    return errors;
  }

  public void setErrors(List<SyntaxError> errors) {
    this.errors = errors;
  }

}
