package de.uniwue.dw.query.model.client;

import de.uniwue.dw.query.model.exception.QueryException;

public class QueryStatus {

  private QueryException exception;

  private double progressPercentageAsFraction;

  private boolean isDone = false;

  public int getProgressPercentageAsNumber() {
    int result = (int) (progressPercentageAsFraction * 100);
    return result;
  }

  public double getGetProgressPercentageAsFraction() {
    return progressPercentageAsFraction;
  }

  public void setWorkedPercentageAsFraction(double workedPercentage) {
    this.progressPercentageAsFraction = workedPercentage;
  }

  public void setException(QueryException exception) {
    this.exception = exception;
  }

  public String getErrorMessage() {
    if (exception == null)
      return null;
    else {
      return exception.getMessage();
    }
  }

  public boolean queryStoppedWithErrors() {
    return exception != null;
  }

  public void setDone(boolean done) {
    isDone = done;
  }

  public boolean isDone() {
    return isDone;
  }

}
