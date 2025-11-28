package de.uniwue.dw.imports.sql;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;

import de.uniwue.dw.core.model.manager.InfoManager;
import de.uniwue.dw.core.sql.IDwSqlSchemaConstant;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.manager.adapter.IRefIDAdapter;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;

public abstract class SQLRefIDAdapter extends DatabaseManager implements IDwSqlSchemaConstant,
        IRefIDAdapter {

  protected HashMap<String, Long> refsOfCurrentImport = null;

  protected SQLManager sqlManagerForRef = null;

  protected InfoManager infoManager = null;

  public SQLRefIDAdapter(SQLManager aSqlManager, InfoManager ainfoManager) throws SQLException {
    super(aSqlManager);
    createSQLTables();
    infoManager = ainfoManager;
    sqlManagerForRef = SQLPropertiesConfiguration.getInstance().getSQLManager();
  }

  public Long addUsedRefID(int attrId, long pid, long caseid, Timestamp measureDate, Long refid)
          throws ImportException {
    Long toRet = null;
    if (refsOfCurrentImport == null) {
      refsOfCurrentImport = new HashMap<String, Long>();
    }
    String key = Integer.toString(attrId) + Long.toString(pid) + Long.toString(caseid)
            + measureDate.toString();

    refsOfCurrentImport.put(key, refid);

    return toRet;
  }

  abstract public long getNonUsedRefID() throws SQLException;

  abstract public Long getUsedRefID(int attrId, long pid, long caseid, Timestamp measureDate,
          boolean ignoreNanoseconds) throws SQLException;

  @Override
  public String getTableName() {
    return T_REF_ID;
  }

}
