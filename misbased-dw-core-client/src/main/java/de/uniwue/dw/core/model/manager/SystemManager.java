package de.uniwue.dw.core.model.manager;

import java.net.InetAddress;
import java.sql.SQLException;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.misc.sql.IParamsAdapter;
import de.uniwue.misc.util.TimeUtil;

/**
 * The SystemManager stores parameters in an EAV model about states of the SQL database. It can e.g.
 * store if the DW is locked or not
 */
public class SystemManager {

  private IParamsAdapter paramsAdapter;

  private double version = -1;

  private static String versionParam = "DB_Schema_Version";

  public static String db_schema_version = "2.0";

  public SystemManager() throws SQLException {
    paramsAdapter = DwClientConfiguration.getInstance().getClientAdapterFactory()
            .createParamsAdapter();
  }

  public String getParam(String paramName, String defaultValue) throws SQLException {
    return paramsAdapter.getParam(paramName, defaultValue);
  }

  public String getParam(String paramName) throws SQLException {
    return getParam(paramName, null);
  }

  public void setParam(String paramName, String value) throws SQLException {
    paramsAdapter.setParam(paramName, value);
  }

  public void deleteParam(String paramName) throws SQLException {
    paramsAdapter.deleteParam(paramName);
  }

  public double getVersion() {
    if (version == -1) {
      try {
        String paramString = getParam(versionParam);
        if (paramString == null) {
          setParam(versionParam, db_schema_version);
          paramString = db_schema_version;
        }
        version = Double.parseDouble(paramString);
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    return version;
  }

  public void lock(String paramName) throws Exception {
    String message = paramName + " started by user: " + System.getProperty("user.name")
            + " on host: " + InetAddress.getLocalHost().getHostName();
    lockTable();
    String param = getParam(paramName);
    if (param != null) {
      paramsAdapter.commit(); // to unlock
      throw new Exception("There is currently another process running doing the same thing");
    } else {
      setParam(paramName, TimeUtil.currentTime() + " " + message);
      paramsAdapter.commit(); // to unlock
    }
  }

  public void unlock(String paramName) throws SQLException {
    deleteParam(paramName);
  }

  private void lockTable() throws SQLException {
    paramsAdapter.lock();
  }

}
