package de.uniwue.dw.core.sql.mssql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.sql.SQLCatalogCountAdapter;
import de.uniwue.misc.sql.SQLManager;

public class MSSQLCatalogCountAdapter extends SQLCatalogCountAdapter {

  public MSSQLCatalogCountAdapter(SQLManager aSqlManager, CatalogManager aCatalogManager)
          throws SQLException {
    super(aSqlManager, aCatalogManager);
  }

  @Override
  public void insertOrUpdateCounts(int attrId, long pidCount, long caseIDCount, long absoluteCount)
          throws SQLException {
    String sql = "if exists (select * from " + getTableName() + "  where AttrID = ?) "
            + "begin update " + getTableName()
            + " set quantity = ?, quantityPID = ?, quantityAbsolute = ? "
            + "where AttrID = ? end else begin insert " + getTableName()
            + " (AttrID, quantity, quantityPID, quantityAbsolute) values (?, ?, ?, ?) end";
    PreparedStatement st = sqlManager.createPreparedStatement(sql);
    int paramOffset = 1;
    st.setInt(paramOffset++, attrId);
    st.setLong(paramOffset++, caseIDCount);
    st.setLong(paramOffset++, pidCount);
    st.setLong(paramOffset++, absoluteCount);
    st.setInt(paramOffset++, attrId);
    st.setInt(paramOffset++, attrId);
    st.setLong(paramOffset++, caseIDCount);
    st.setLong(paramOffset++, pidCount);
    st.setLong(paramOffset++, absoluteCount);
    st.execute();
    st.close();
  }

}
