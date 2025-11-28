package de.uniwue.dw.core.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import de.uniwue.dw.core.model.manager.DWIterException;
import de.uniwue.dw.core.model.manager.ValueIterator;

public class SQLValueIterator extends ValueIterator {

  protected ResultSet resultSet;

  protected Statement statement;

  public SQLValueIterator(ResultSet rs) {
    this(rs, null);
  }

  public SQLValueIterator(ResultSet rs, Statement st) {
    resultSet = rs;
    statement = st;
  }

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

  protected boolean isBeforeFirst = true;

  @Override
  public boolean hasNext() {
    if (resultSet == null) {
      return false;
    }
    try {
      boolean result = !resultSet.isLast() && !resultSet.isAfterLast();
      if (isBeforeFirst && !resultSet.isBeforeFirst()) {
        result = false;
      }
      return result;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String next() {
    try {
      if (resultSet == null) {
        return null;
      }
      boolean hasNext = resultSet.next();
      isBeforeFirst = false;
      String value = resultSet.getString("value");
      return value;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

}
