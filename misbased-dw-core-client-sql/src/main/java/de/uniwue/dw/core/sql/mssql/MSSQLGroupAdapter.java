package de.uniwue.dw.core.sql.mssql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.core.client.authentication.group.Group;
import de.uniwue.dw.core.sql.SQLGroupAdapter;
import de.uniwue.misc.sql.SQLManager;

public class MSSQLGroupAdapter extends SQLGroupAdapter {

  private static final Logger logger = LogManager.getLogger(MSSQLGroupAdapter.class);

  public MSSQLGroupAdapter(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
  }

  @Override
  protected String getCreateTableString() {
    //@formatter:off
    String sql = "CREATE TABLE [" + getTableName() + "]( " 
            + "[id] [int] IDENTITY(1,1) PRIMARY KEY NOT NULL, "
            + "[name] [varchar](500) UNIQUE NOT NULL, " 
            + "[kAnonymity] [varchar](200) DEFAULT 10, "
            + "[case_query] [bit] DEFAULT 0, " 
            + "[admin] [bit] DEFAULT 0 " 
            + ");";
    //@formatter:on
    return sql;
  }

  @Override
  public Group insertGroup(String name, int kAnonymity, boolean caseQuery, boolean admin)
          throws SQLException {
    Group result = null;
    String command = "IF NOT EXISTS (SELECT * FROM " + getTableName()
            + " WHERE name=?) INSERT INTO " + getTableName() + "\n"
            + "(name, kAnonymity, case_query, admin) \n" + "VALUES (?, ?, ?, ?)";
    PreparedStatement st = sqlManager.createPreparedStatementReturnGeneratedKey(command);
    int paramOffset = 1;
    st.setString(paramOffset++, name);
    st.setString(paramOffset++, name);
    st.setInt(paramOffset++, kAnonymity);
    st.setBoolean(paramOffset++, caseQuery);
    st.setBoolean(paramOffset++, admin);
    st.execute();
    int genID = getGeneratedIntKey(st);
    st.close();
    result = new Group(genID, name, kAnonymity, caseQuery, admin);
    commit();
    logger.info("Inserted group " + name);
    return result;
  }

}
