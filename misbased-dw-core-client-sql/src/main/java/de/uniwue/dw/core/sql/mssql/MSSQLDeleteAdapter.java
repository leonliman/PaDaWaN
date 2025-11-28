package de.uniwue.dw.core.sql.mssql;

import java.sql.SQLException;

import de.uniwue.dw.core.sql.SQLDeleteAdapter;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;

public class MSSQLDeleteAdapter extends SQLDeleteAdapter {

  public MSSQLDeleteAdapter(SQLManager aSqlManager) throws SQLException {
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
            + "PID BIGINT NOT NULL" + ")"
            + "ALTER TABLE " + getTableName() + " "
            + "ADD CONSTRAINT " + getTableName() + "_infoID UNIQUE (infoID) WITH ( IGNORE_DUP_KEY = ON ) \n"            
            ;
    return command;
  } // @formatter:on



}
