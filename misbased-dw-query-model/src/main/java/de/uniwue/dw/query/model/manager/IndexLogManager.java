package de.uniwue.dw.query.model.manager;

import de.uniwue.dw.query.model.DWQueryConfig;
import de.uniwue.dw.query.model.data.IndexLogEntry;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.dw.query.model.manager.adapter.IIndexLogAdapter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public class IndexLogManager {

  private static IIndexLogAdapter logAdapter;

  private static Logger log = LogManager.getLogger("DWIndex");

  public static void init() throws IndexException {
    try {
      logAdapter = DWQueryConfig.getInstance().getQueryAdapterFactory().createIndexLogAdapter();
    } catch (SQLException e) {
      throw new IndexException(e);
    }
  }

  public static void error(IndexException e, String serverID) throws IndexException {
    if (logAdapter != null) {
      logAdapter.error(e, serverID);
    }
    log.error(e);
    throw e;
  }

  public static void error(String message, String serverID) throws IndexException {
    if (logAdapter != null) {
      logAdapter.error(message, serverID);
    }
    log.error(message);
    throw new IndexException(message);
  }

  public static void info(String message, String serverID) {
    if (logAdapter != null) {
      logAdapter.insert(message, serverID);
    }
    log.info(message);
  }

  public static void info(String message, long pid, long caseID, String serverID) throws IndexException {
    if (logAdapter != null) {
      logAdapter.insert(message, pid, caseID, serverID);
    }
    log.info(message);
  }

  public static List<IndexLogEntry> getLogEntriesByServerIDSinceTime(String serverID, Timestamp time) {
    if (logAdapter == null) {
      try {
        init();
      } catch (IndexException e) {
        e.printStackTrace();
        return null;
      }
    }
    return logAdapter.getLogEntriesByServerIDSinceTime(serverID, time);
  }

}
