package de.uniwue.dw.core.model.manager.adapter;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.core.model.manager.InfoIterator;

public interface IDeleteAdapter {

  void markInfosDeleted(InfoIterator anIter) throws SQLException;

  boolean dropTable() throws SQLException;

  List<Information> getIDsAfterTimeForDeletedInfos(Timestamp timestamp) throws SQLException;

  List<Integer> getAttrIDsOfDeletedInfosAfterTime(Timestamp timestamp) throws SQLException;

  boolean truncateTable() throws SQLException;

}
