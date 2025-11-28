package de.uniwue.misc.sql.MAXDB;

import java.sql.SQLException;

import de.uniwue.misc.sql.SQLConfig;
import de.uniwue.misc.sql.SQLFactory;
import de.uniwue.misc.sql.SQLManager;

public class MAXDBFactory extends SQLFactory {

  public SQLManager getSQLManagerInternal(SQLConfig aConfig) throws SQLException {
    SQLManager result = new MAXDBManager(aConfig);
    return result;
  }

}
