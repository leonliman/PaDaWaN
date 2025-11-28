package de.uniwue.misc.sql.MySQL;

import java.sql.SQLException;

import de.uniwue.misc.sql.SQLConfig;
import de.uniwue.misc.sql.SQLFactory;
import de.uniwue.misc.sql.SQLManager;

public class MySQLFactory extends SQLFactory {

  public SQLManager getSQLManagerInternal(SQLConfig aConfig) throws SQLException {
    SQLManager result = new MySQLManager(aConfig);
    return result;
  }

}
