package de.uniwue.dw.core.sql.mssql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.core.client.authentication.group.Group;
import de.uniwue.dw.core.sql.SQLUserInGroupAdapter;
import de.uniwue.misc.sql.SQLManager;

public class MSSQLUserInGroupAdapter extends SQLUserInGroupAdapter {

  private static final Logger logger = LogManager.getLogger(MSSQLUserInGroupAdapter.class);

  public MSSQLUserInGroupAdapter(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
  }

  @Override
  public void insertUser2Group(int groupID, String userName) throws SQLException {
    Group result = null;
    String command = "IF NOT EXISTS (SELECT * FROM " + getTableName()
            + " WHERE groupID=? AND username=?) INSERT INTO " + getTableName() + "\n"
            + "(groupID, username) \n" + "VALUES (?, ?)";
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
