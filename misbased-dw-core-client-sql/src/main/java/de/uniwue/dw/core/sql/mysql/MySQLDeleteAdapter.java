package de.uniwue.dw.core.sql.mysql;

import java.sql.SQLException;

import de.uniwue.dw.core.sql.SQLDeleteAdapter;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;

public class MySQLDeleteAdapter extends SQLDeleteAdapter {

  public MySQLDeleteAdapter(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
  }

  @Override
  // @formatter:off
  protected String getCreateTableString() {
    String command = "";

    command = getCreateTableStub();
    command += "InfoID BIGINT NOT NULL PRIMARY KEY, \n" 
            + "DeleteTime " + SQLTypes.timestampType(sqlManager.config) + ", \n" 
            + "AttrID INT NOT NULL" + ", \n" 
            + "CaseID BIGINT NOT NULL" + ", \n" 
            + "Ref BIGINT NOT NULL" + ", \n" 
            + "MeasureTime " + SQLTypes.timestampType(sqlManager.config) + ", \n" 
            + "PID BIGINT NOT NULL" 
            + ")"
            ;
    return command;
  } // @formatter:on

}
