package de.uniwue.dw.query.model.manager.adapter;

import de.uniwue.dw.query.model.data.IndexLogEntry;
import de.uniwue.dw.query.model.index.IndexException;

import java.sql.Timestamp;
import java.util.List;

public interface IIndexLogAdapter {

  void error(String message, String serverID);

  void error(IndexException e, String serverID);

  void error(String message, IndexException e, String serverID);

  void insert(String message, String serverID);

  void insert(String message, long pid, long caseID, String serverID);

  void dispose();

  List<IndexLogEntry> getLogEntriesByServerIDSinceTime(String serverID, Timestamp time);

}