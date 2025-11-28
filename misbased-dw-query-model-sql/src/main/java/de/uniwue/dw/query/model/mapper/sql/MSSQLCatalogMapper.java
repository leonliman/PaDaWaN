package de.uniwue.dw.query.model.mapper.sql;

import java.sql.SQLException;

import de.uniwue.misc.sql.SQLManager;

public class MSSQLCatalogMapper extends SQLCatalogMapper {

  public MSSQLCatalogMapper(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
  }

  @Override
  protected String getTableIsEmptySQL() {
    String sql = "select top 1 * from " + getTableName();
    return sql;
  }

  @Override
  protected String getCreateTableString() {
    //@formatter:off
    String sql = "CREATE TABLE " + getTableName() + " ( "
            + "[id] [int] IDENTITY(1,1) NOT NULL, "
            + "[masterExtId] [varchar](255) NOT NULL, "
            + "[masterProject] [varchar](255) NOT NULL, "
            + "[localExtId] [varchar](255) NOT NULL, "
            + "[localProject] [varchar](255) NOT NULL, "
            + "CONSTRAINT masterEntry UNIQUE ([masterExtId],[masterProject]) "
            + ")";
    //@formatter:on
    return sql;
  }

}