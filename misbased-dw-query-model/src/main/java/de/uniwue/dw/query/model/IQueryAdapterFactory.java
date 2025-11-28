package de.uniwue.dw.query.model;

import java.sql.SQLException;

import de.uniwue.dw.query.model.manager.IQueryIOManager;
import de.uniwue.dw.query.model.manager.adapter.IIndexLogAdapter;
import de.uniwue.dw.query.model.manager.adapter.IQueryAdapter;
import de.uniwue.dw.query.model.manager.adapter.IQueryLogAdapter;
import de.uniwue.dw.query.model.mapper.ICatalogMapper;
import de.uniwue.misc.sql.IParamsAdapter;

public interface IQueryAdapterFactory {

  IQueryAdapter createQueryAdapter(IQueryIOManager aManager) throws SQLException;

  IQueryLogAdapter createQueryLogAdapter() throws SQLException;

  IIndexLogAdapter createIndexLogAdapter() throws SQLException;

  IParamsAdapter createParamsAdapter(String storageName) throws SQLException;
  
  void dropDataTables() throws SQLException;
  
  public ICatalogMapper createCatalogMapper() throws SQLException;
  
}
