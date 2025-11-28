package de.uniwue.dw.query.model.sql;

import java.sql.SQLException;

import de.uniwue.dw.core.sql.IDwSqlSchemaConstant;
import de.uniwue.dw.query.model.IQueryAdapterFactory;
import de.uniwue.dw.query.model.manager.IQueryIOManager;
import de.uniwue.dw.query.model.manager.adapter.IIndexLogAdapter;
import de.uniwue.dw.query.model.manager.adapter.IQueryAdapter;
import de.uniwue.dw.query.model.manager.adapter.IQueryLogAdapter;
import de.uniwue.misc.sql.IParamsAdapter;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLParamsAdapter;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;

public abstract class SQLQueryAdapterFactory implements IQueryAdapterFactory {

  protected String getIndexLogStorageName() {
    return IDwSqlSchemaConstant.T_INDEX_LOG;
  }

  @Override
  public IQueryAdapter createQueryAdapter(IQueryIOManager aManager) throws SQLException {
    return new SQLQueryAdapter(aManager, SQLPropertiesConfiguration.getInstance().getSQLManager());
  }

  @Override
  public IQueryLogAdapter createQueryLogAdapter() throws SQLException {
    return new SQLQueryLogAdapter(SQLPropertiesConfiguration.getInstance().getSQLManager());
  }

  @Override
  public IIndexLogAdapter createIndexLogAdapter() throws SQLException {
    return new SQLIndexLogAdapter(SQLPropertiesConfiguration.getInstance().getSQLManager(),
            IDwSqlSchemaConstant.T_INDEX_LOG);
  }

  @Override
  public IParamsAdapter createParamsAdapter(String storageName) throws SQLException {
    return new SQLParamsAdapter(SQLPropertiesConfiguration.getInstance().getSQLManager(),
            storageName);
  }

  @Override
  public void dropDataTables() throws SQLException {
    SQLManager sqlManager = SQLPropertiesConfiguration.getInstance().getSQLManager();
    sqlManager.dropTable(IDwSqlSchemaConstant.T_QUERY);
    sqlManager.dropTable(IDwSqlSchemaConstant.T_INDEX_PARAMS);
    sqlManager.dropTable(IDwSqlSchemaConstant.T_INDEX_LOG);
    sqlManager.dropTable(IDwSqlSchemaConstant.T_QUERY_LOG);
  }

}
