package de.uniwue.misc.util.thread;

import java.lang.reflect.InvocationTargetException;

public abstract class CancelableRunnableWithProgress {

  private CancelWatchdog watchdog;

  private IUniWueProgressMonitorAdapter monitor = new UniWueNullProgressMonitor();

  protected boolean done = false;

  protected boolean canceled = false;

  public int workTotal;

  public CancelableRunnableWithProgress(int workTotal) {
    this.workTotal = workTotal;
  }

  public void run() throws InvocationTargetException, InterruptedException {
    run(new UniWueNullProgressMonitor());
  }

  protected void doCancel() {
    getMonitor().setCanceled();
    canceled = true;
  }

  /*
   * Override this method when needed
   */
  public CancelWatchdog createWatchDog() {
    return new CancelWatchdog(this) {
      @Override
      public void cancelJob() {
        doCancel();
      }
    };
  }

  public void run(IUniWueProgressMonitorAdapter monitor)
          throws InvocationTargetException, InterruptedException {
    this.monitor = monitor;
    Thread.currentThread().setName("CancalbleRunnableWithProgress");
    watchdog = createWatchDog();
    watchdog.start();
    getMonitor().beginTask("Starting operation", workTotal);
    Thread thread = new Thread() {
      @Override
      public void run() {
        work();
        getMonitor().done();
        done = true;
      }
    };
    thread.start();    
  }

  /**
   * This is the method in which the actual work is done. It is a blocking operation
   */
  protected abstract void work();

  public IUniWueProgressMonitorAdapter getMonitor() {
    return monitor;
  }

  public boolean isDone() {
    return done;
  }

}
