package de.uniwue.dw.core.sql.mysql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.core.sql.SQLUserInGroupAdapter;
import de.uniwue.dw.core.sql.mssql.MSSQLUserInGroupAdapter;
import de.uniwue.misc.sql.SQLManager;

public class MySQLUserInGroupAdapter extends SQLUserInGroupAdapter {
  
  private static final Logger logger = LogManager.getLogger(MySQLUserInGroupAdapter.class);

  public MySQLUserInGroupAdapter(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
  }

  @Override
  public void insertUser2Group(int groupID, String userName) throws SQLException {
    String command = "INSERT INTO " + getTableName() + " (groupID, username) VALUES "
            + "(?, ?) ON DUPLICATE KEY UPDATE groupID=?, username=?";
    PreparedStatement st = sqlManager.createPreparedStatement(command);
    int paramOffset = 1;
    st.setInt(paramOffset++, groupID);
    st.setString(paramOffset++, userName);
    st.setInt(paramOffset++, groupID);
    st.setString(paramOffset++, userName);
    st.execute();
    st.close();
    commit();
    logger.info("inserted/updated user in group. groupID: "+groupID+" user: "+userName);
  }
}
