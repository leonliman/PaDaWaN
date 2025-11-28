package de.uniwue.dw.imports.sql.mysql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import de.uniwue.dw.core.model.manager.InfoManager;
import de.uniwue.dw.imports.manager.adapter.IRefIDAdapter;
import de.uniwue.dw.imports.sql.SQLRefIDAdapter;
import de.uniwue.misc.sql.SQLManager;

public class MySQLRefIDAdapter extends SQLRefIDAdapter implements IRefIDAdapter {


  public MySQLRefIDAdapter(SQLManager aSqlManager, InfoManager ainfoManager) throws SQLException {
    super(aSqlManager, ainfoManager);
  }

  @Override
  protected String getCreateTableString() {
    String command = "";

    command = getCreateTableStub();
    command += "" + "Ref BIGINT AUTO_INCREMENT PRIMARY KEY, \n"
            + "addRefID TINYINT"
            + ",\n" + "INDEX " + getTableName() + "_Ref (Ref)\n" + "); \n" 
            + "ALTER TABLE " + getTableName() + " AUTO_INCREMENT=1000000000000000000";
    return command;
  }

  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public long getNonUsedRefID() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Long getUsedRefID(int attrId, long pid, long caseid, Timestamp measureDate,
          boolean ignoreNanoseconds) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Long getUsedOrNewRefID(int attrId, long pid, long caseID, Timestamp startTime, boolean ignoreNanoseconds) {
    // TODO Auto-generated method stub
    return null;
  }

}
