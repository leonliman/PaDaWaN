package de.uniwue.dw.query.sql.util;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.query.model.client.IGUIClient;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.result.Cell;
import de.uniwue.dw.query.model.result.PatientQueryRunnable;
import de.uniwue.dw.query.model.result.Row;
import de.uniwue.dw.query.model.result.export.ExportConfiguration;
import de.uniwue.dw.query.sql.SQLQueryRoot;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SQLQueryRunnable extends PatientQueryRunnable {

  public SQLQueryRunnable(int workTotal, QueryRoot queryRoot, User user,
          ExportConfiguration exportConfig, IGUIClient guiClient)
          throws QueryException {
    super(workTotal, queryRoot, user, exportConfig, guiClient);
  }

  private SQLQueryRoot sqlQuery;

  @Override
  public void createOutput() throws QueryException {
    try {
      // TODO: creation of result does not work yet !!!! creation of header should be made by
      // patientQueryRunnable
      // TODO: linking between outputs and QueryAttributes should be created !!!
      sqlQuery = new SQLQueryRoot(SQLPropertiesConfiguration.getInstance().getSQLManager(),
              getQueryRoot());
      List<List<Object>> resultTable = sqlQuery.getResultTable();
      outputHandler.setDocsFound(resultTable.size() - 1);
      List<Object> firstRow = resultTable.get(0);
      resultTable.remove(0);
      List<String> header = new ArrayList<>();
      for (Object aHeader : firstRow) {
        header.add(aHeader.toString());
      }
      outputHandler.setHeader(header);
      for (List<Object> anSQLRow : resultTable) {
        Row aRow = new Row();
        for (Object anEntry : anSQLRow) {
          Cell aCell = aRow.createNewCell();
          aCell.value = anEntry;
        }
        outputHandler.addRow(aRow);
      }
    } catch (SQLException e) {
      getMonitor().setCanceled();
      System.out.println(e.getMessage());
    }
  }

  @Override
  public void doCancel() {
    super.doCancel();
    if (sqlQuery != null) {
      sqlQuery.cancel();
    }
  }

  @Override
  public long getOnlyCountNumber() throws QueryException {
    try {
      sqlQuery = new SQLQueryRoot(SQLPropertiesConfiguration.getInstance().getSQLManager(),
              getQueryRoot());
      List<List<Object>> resultTable = sqlQuery.getResultTable();
      long result = (long) resultTable.get(1).get(0);
      return result;
    } catch (SQLException e) {
      throw new QueryException(e);
    }
  }

}
