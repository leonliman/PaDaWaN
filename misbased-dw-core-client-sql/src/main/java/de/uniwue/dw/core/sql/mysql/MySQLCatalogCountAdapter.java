package de.uniwue.dw.core.sql.mysql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.sql.SQLCatalogCountAdapter;
import de.uniwue.misc.sql.SQLManager;

public class MySQLCatalogCountAdapter extends SQLCatalogCountAdapter {

  public MySQLCatalogCountAdapter(SQLManager aSqlManager, CatalogManager aCatalogManager)
          throws SQLException {
    super(aSqlManager, aCatalogManager);
  }

  @Override
  public void insertOrUpdateCounts(int attrId, long pidCount, long caseIDCount, long absoluteCount)
          throws SQLException {
    String sql = "INSERT INTO " + getTableName()
            + " (AttrID, quantity, quantityPID, quantityAbsolute) VALUES (?, ?, ?, ?) "
            + "ON DUPLICATE KEY UPDATE quantity = ?, quantityPID = ?, quantityAbsolute = ?";
    PreparedStatement st = sqlManager.createPreparedStatement(sql);
    int paramOffset = 1;
    st.setInt(paramOffset++, attrId);
    st.setLong(paramOffset++, caseIDCount);
    st.setLong(paramOffset++, pidCount);
    st.setLong(paramOffset++, absoluteCount);
    st.setLong(paramOffset++, caseIDCount);
    st.setLong(paramOffset++, pidCount);
    st.setLong(paramOffset++, absoluteCount);
    st.execute();
    st.close();
  }

}
