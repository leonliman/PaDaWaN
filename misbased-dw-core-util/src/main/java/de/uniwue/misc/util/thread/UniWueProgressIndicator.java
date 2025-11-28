package de.uniwue.misc.util.thread;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class UniWueProgressIndicator implements IUniWueProgressMonitorAdapter {

  private static Logger logger = LogManager.getLogger(UniWueProgressIndicator.class);

  private double totalWork;

  private double sumWorked;

  @Override
  public void beginTask(String name, int totalWork) {
    this.totalWork = totalWork;
    this.sumWorked = 0;
    beginTaskInternal(name, totalWork);
  }

  protected abstract void beginTaskInternal(String name, int totalWork2);

  @Override
  public void done() {
    sumWorked = totalWork;
    doneInternal();
  }

  protected abstract void doneInternal();

  @Override
  public void worked(int work) {
    sumWorked += work;
    if (sumWorked > totalWork) {
      sumWorked = totalWork;
    }
    if (sumWorked < 0) {
      sumWorked = 0;
    }
    workedInternal(work);
    logger.trace("{}/{} {}% isDone: {}", sumWorked, totalWork, getWorkedPercentage(), isDone());
  }

  protected abstract void workedInternal(int work);

  @Override
  public double getTotalWork() {
    return totalWork;
  }

  @Override
  public double getSumWorked() {
    return sumWorked;
  }

  @Override
  public double getWorkedPercentage() {
    return sumWorked / totalWork;
  }

  @Override
  public boolean isDone() {
    return sumWorked >= totalWork;
  }

}
