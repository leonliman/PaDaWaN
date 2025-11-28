package de.uniwue.misc.sql.MSSQL;

import java.sql.SQLException;

import de.uniwue.misc.sql.SQLConfig;
import de.uniwue.misc.sql.SQLFactory;
import de.uniwue.misc.sql.SQLManager;

public class MSSQLFactory extends SQLFactory {

  public SQLManager getSQLManagerInternal(SQLConfig aConfig) throws SQLException {
    SQLManager result = new MSSQLManager(aConfig);
    return result;
  }

}
