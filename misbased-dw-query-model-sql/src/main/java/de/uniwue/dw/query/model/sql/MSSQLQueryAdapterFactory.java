package de.uniwue.dw.query.model.sql;

import java.sql.SQLException;

import de.uniwue.dw.query.model.mapper.ICatalogMapper;
import de.uniwue.dw.query.model.mapper.sql.MSSQLCatalogMapper;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;

public class MSSQLQueryAdapterFactory extends SQLQueryAdapterFactory {

  @Override
  public ICatalogMapper createCatalogMapper() throws SQLException {
    return new MSSQLCatalogMapper(SQLPropertiesConfiguration.getInstance().getSQLManager());
  }

}
