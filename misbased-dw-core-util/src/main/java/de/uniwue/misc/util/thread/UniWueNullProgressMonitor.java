package de.uniwue.misc.util.thread;

public class UniWueNullProgressMonitor extends UniWueProgressIndicator {

  private boolean canceled = false;

  private boolean done = false;
  
  @Override
  public void subTask(String name) {
  }

  @Override
  public boolean isCanceled() {
    return canceled;
  }

  @Override
  public IUniWueProgressMonitorAdapter subMonitor(int fraction) {
    return null;
  }

  @Override
  public void setCanceled() {
    this.canceled = true;
  }

  @Override
  public void setTaskName(String name) {
  }

  @Override
  protected void beginTaskInternal(String name, int totalWork) {
  }

  @Override
  protected void doneInternal() {
    done = true;
  }

  @Override
  protected void workedInternal(int work) {
  }

  @Override
  public boolean isDone() {
    return done;
  }  
}
