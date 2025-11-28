package de.uniwue.misc.sql;

import java.sql.SQLException;
import java.sql.Statement;

import de.uniwue.misc.util.thread.CancelWatchdog;
import de.uniwue.misc.util.thread.CancelableRunnableWithProgress;

public class SQLWatchDog extends CancelWatchdog {

  private Statement stmt;

  public SQLWatchDog(CancelableRunnableWithProgress myRunnableWithProgress, Statement stmt) {
    super(myRunnableWithProgress);
    this.stmt = stmt;
  }

  @Override
  public void cancelJob() {
    try {
      stmt.cancel();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

}
