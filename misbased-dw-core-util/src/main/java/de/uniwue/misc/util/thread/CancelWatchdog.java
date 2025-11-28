package de.uniwue.misc.util.thread;

import java.util.concurrent.TimeUnit;

public abstract class CancelWatchdog extends Thread {

  protected CancelableRunnableWithProgress myRunnableWithProgress;

  public CancelWatchdog(CancelableRunnableWithProgress myRunnableWithProgress) {
    this.myRunnableWithProgress = myRunnableWithProgress;
  }

  public void run() {
    Thread.currentThread().setName("Watchdog");
    while (!myRunnableWithProgress.isDone() && !myRunnableWithProgress.getMonitor().isCanceled()) {
      try {
        TimeUnit.MILLISECONDS.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    if (myRunnableWithProgress.getMonitor().isCanceled())
      try {
        cancelJob();
      } catch (Exception e) {
        e.printStackTrace();
      }
  }

  public abstract void cancelJob();

}
