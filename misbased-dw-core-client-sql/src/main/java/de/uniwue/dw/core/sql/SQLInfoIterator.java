package de.uniwue.dw.core.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.core.model.manager.DWIterException;
import de.uniwue.dw.core.model.manager.InfoIterator;

public class SQLInfoIterator extends InfoIterator {

  protected ResultSet resultSet;

  protected Statement statement;

  protected boolean getValue;

  protected Information cachedNextValidInfo;

  protected boolean isBeforeFirst = true;

  public SQLInfoIterator(ResultSet rs, Statement st) {
    this(rs, st, true);
  }

  public SQLInfoIterator(ResultSet rs) {
    this(rs, true);
  }

  public SQLInfoIterator(ResultSet rs, boolean getValue) {
    this(rs, null, getValue);
  }

  public SQLInfoIterator(ResultSet rs, Statement st, boolean getValue) {
    resultSet = rs;
    statement = st;
    this.getValue = getValue;
  }

  @Override
  public void dispose() throws DWIterException {
    try {
      if (resultSet != null) {
        resultSet.close();
      }
      if (statement != null) {
        statement.close();
      }
    } catch (SQLException e) {
      throw new DWIterException(e);
    }
    super.dispose();
  }

  @Override
  public boolean hasNext() {
    if (cachedNextValidInfo != null) {
      return true;
    }
    if (resultSet == null) {
      return false;
    }
    try {
      boolean result = true;
      // result = !resultSet.isLast() && !resultSet.isAfterLast();
      if (isBeforeFirst && !resultSet.isBeforeFirst()) {
        result = false;
      }
      nextInternal();
      if (cachedNextValidInfo == null) {
        // there could not be a next valid information be retrieved from the underlying data source
        result = false;
      }
      return result;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected Information nextInternal() throws SQLException {
    isBeforeFirst = false;
    cachedNextValidInfo = null;
    if (resultSet == null) {
      throw new RuntimeException("non next");
    }
    boolean hasNext = resultSet.next();
    if (hasNext) {
      cachedNextValidInfo = SQLInfoAdapter.getInfo(resultSet, getValue);
    }
    return cachedNextValidInfo;
  }

  @Override
  public Information next() {
    try {
      if (cachedNextValidInfo == null) {
        nextInternal();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    if (cachedNextValidInfo != null) {
      Information result = cachedNextValidInfo;
      cachedNextValidInfo = null;
      return result;
    } else {
      throw new RuntimeException("no next");
    }
  }

}
