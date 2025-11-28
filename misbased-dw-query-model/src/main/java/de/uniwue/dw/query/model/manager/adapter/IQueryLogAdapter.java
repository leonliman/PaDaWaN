package de.uniwue.dw.query.model.manager.adapter;

import java.sql.SQLException;
import java.util.List;

import de.uniwue.dw.query.model.data.LoggedQuery;

public interface IQueryLogAdapter {

  void insert(String queryXML, String user, int queryID, String exportType, String engineType,
          String engineVersion) throws SQLException;

  void updateEntry(int queryID, long resultCount, long duration) throws SQLException;

  List<LoggedQuery> getLatestLoggedQueries(String user, int numberOfQueries) throws SQLException;

  LoggedQuery getLoggedQueryByID(int logID) throws SQLException;

  boolean dropTable() throws SQLException;

  void dispose();

}