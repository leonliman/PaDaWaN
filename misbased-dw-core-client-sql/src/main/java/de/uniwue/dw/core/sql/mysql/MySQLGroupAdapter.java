package de.uniwue.dw.core.sql.mysql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.core.client.authentication.group.Group;
import de.uniwue.dw.core.sql.SQLGroupAdapter;
import de.uniwue.dw.core.sql.mssql.MSSQLGroupAdapter;
import de.uniwue.misc.sql.SQLManager;

public class MySQLGroupAdapter extends SQLGroupAdapter {

  private static final Logger logger = LogManager.getLogger(MySQLGroupAdapter.class);

  
  public MySQLGroupAdapter(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
  }

  @Override
  protected String getCreateTableString() {
    // @formatter:off
    String sql = "CREATE TABLE " + getTableName() + " ( " + "`id` INT(11) NOT NULL AUTO_INCREMENT, "
            + "`name` VARCHAR(255) NOT NULL DEFAULT '0', "
            + "`kAnonymity` INT(11) NOT NULL DEFAULT '10', "
            + "`case_query` BIT(1) NOT NULL DEFAULT b'0', "
            + "`admin` BIT(1) NOT NULL DEFAULT b'0', " + "PRIMARY KEY (`id`), "
            + "UNIQUE INDEX `unique_name` (`name`) " + ");";
    // @formatter:on
    return sql;
  }

  @Override
  public Group insertGroup(String name, int kAnonymity, boolean caseQuery, boolean admin)
          throws SQLException {
    String command = "INSERT INTO " + getTableName()
            + " (name, kAnonymity, case_query, admin) VALUES "
            + "(?, ?, ?, ?) ON DUPLICATE KEY UPDATE kAnonymity=?, case_query=?, admin=?";
    PreparedStatement st = sqlManager.createPreparedStatementReturnGeneratedKey(command);
    int paramOffset = 1;
    st.setString(paramOffset++, name);
    st.setInt(paramOffset++, kAnonymity);
    st.setBoolean(paramOffset++, caseQuery);
    st.setBoolean(paramOffset++, admin);
    st.setInt(paramOffset++, kAnonymity);
    st.setBoolean(paramOffset++, caseQuery);
    st.setBoolean(paramOffset++, admin);
    st.execute();
    int genID = getGeneratedIntKey(st);
    st.close();
    Group result = new Group(genID, name, kAnonymity, caseQuery, admin);
    commit();
    logger.info("Inserted group " + name);
    return result;
  }

}
