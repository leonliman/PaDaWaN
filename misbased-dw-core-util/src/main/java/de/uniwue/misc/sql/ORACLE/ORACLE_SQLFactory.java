package de.uniwue.misc.sql.ORACLE;

import java.sql.SQLException;

import de.uniwue.misc.sql.SQLConfig;
import de.uniwue.misc.sql.SQLFactory;
import de.uniwue.misc.sql.SQLManager;

public class ORACLE_SQLFactory extends SQLFactory {

  public SQLManager getSQLManagerInternal(SQLConfig aConfig) throws SQLException {
    SQLManager result = new ORACLE_SQLManager(aConfig);
    return result;
  }

}
