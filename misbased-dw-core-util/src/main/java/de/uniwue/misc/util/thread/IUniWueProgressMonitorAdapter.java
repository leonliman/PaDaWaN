package de.uniwue.misc.util.thread;

/**
 * This interface has the purpose to wrap an eclipse ProgressMonitor and to use the wrapped instance
 * in another project without having the surrounding project to reference the Eclipse packages
 * 
 * @author Georg Fette
 */
public interface IUniWueProgressMonitorAdapter {

  void beginTask(String name, int totalWork);

  void done();

  void subTask(String name);

  void worked(int work);

  boolean isCanceled();

  IUniWueProgressMonitorAdapter subMonitor(int fraction);

  void setCanceled();

  void setTaskName(String name);
  
  double getTotalWork();
  
  double getSumWorked();
  
  double getWorkedPercentage();

  boolean isDone();

}
