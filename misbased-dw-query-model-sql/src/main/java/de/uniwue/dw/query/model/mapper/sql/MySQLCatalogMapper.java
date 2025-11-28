package de.uniwue.dw.query.model.mapper.sql;

import java.sql.SQLException;

import de.uniwue.misc.sql.SQLManager;

public class MySQLCatalogMapper extends SQLCatalogMapper {

  public MySQLCatalogMapper(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
  }

  @Override
  protected String getTableIsEmptySQL() {
    String sql = "select * from " + getTableName() + " limit 1";
    return sql;
  }

  @Override
  protected String getCreateTableString() {
    //@formatter:off
    String sql = "CREATE TABLE " + getTableName() + " ( " 
            + "id INT(11) NOT NULL AUTO_INCREMENT, "
            + "masterExtId VARCHAR(255) NOT NULL, " 
            + "masterProject VARCHAR(255) NOT NULL, "
            + "localExtId VARCHAR(255) NOT NULL, " 
            + "localProject VARCHAR(255) NOT NULL, "
            + "PRIMARY KEY (id), " 
            + "UNIQUE INDEX masterEntry (masterExtId, masterProject) " 
            + ")";
    //@formatter:on
    return sql;
  }

}
