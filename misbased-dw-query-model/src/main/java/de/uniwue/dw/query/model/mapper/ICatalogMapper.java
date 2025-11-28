package de.uniwue.dw.query.model.mapper;

import java.sql.SQLException;

import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.lang.QueryRoot;

public interface ICatalogMapper {

  public void queryRootMapper(QueryRoot queryRoot) throws GUIClientException, SQLException;
}
