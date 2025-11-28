package de.uniwue.dw.core.sql.mysql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.sql.SQLCatalogNumDataAdapter;
import de.uniwue.misc.sql.SQLManager;

public class MySQLCatalogNumDataAdapter extends SQLCatalogNumDataAdapter {

  public MySQLCatalogNumDataAdapter(SQLManager aSqlManager, CatalogManager aCatalogManager)
          throws SQLException {
    super(aSqlManager, aCatalogManager);
  }

  public void insert(Integer attrID, String unit, double lowBound, double highBound)
          throws SQLException {
    PreparedStatement st;
    String command = "";

    command += "INSERT IGNORE INTO " + getTableName() + " "
            + "(AttrID, unit, lowBound, highBound) values (?, ?, ?, ?)";
    st = sqlManager.createPreparedStatementReturnGeneratedKey(command);
    int paramOffset = 1;
    st.setInt(paramOffset++, attrID);
    st.setString(paramOffset++, unit);
    st.setDouble(paramOffset++, lowBound);
    st.setDouble(paramOffset++, highBound);
    st.execute();
    st.close();
  }

}
