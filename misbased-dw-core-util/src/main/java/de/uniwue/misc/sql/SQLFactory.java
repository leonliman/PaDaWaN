package de.uniwue.misc.sql;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public abstract class SQLFactory {

  public static Map<SQLConfig, SQLManager> singeltons = new HashMap<SQLConfig, SQLManager>();

  public abstract SQLManager getSQLManagerInternal(SQLConfig aConfig) throws SQLException;

  public SQLManager getSQLManager(SQLConfig aConfig) throws SQLException {
    SQLManager result = null;
    if (singeltons.containsKey(aConfig)) {
      result = singeltons.get(aConfig);
    } else {
      if (!aConfig.isValid()) {
        throw new SQLException(
                "Invalid SQLConfiguration: \n" + "Server: '" + aConfig.sqlServer + "'\n" + "User: '"
                        + aConfig.user + "'\n" + "Database: '" + aConfig.database + "'\n");
      }
      try {
        result = getSQLManagerInternal(aConfig);
        singeltons.put(aConfig, result);
      } catch (SQLException e) {
        throw new SQLException("Could not create SQLManager with parameters: \n" + "Server: '"
                + aConfig.sqlServer + "'\n" + "User: '" + aConfig.user + "'\n" + "Database: '"
                + aConfig.database + "' UseTrustedConnection: " + aConfig.useTrustedConnection
                + "\n" + e.getMessage());
      }
    }
    return result;
  }

  public static SQLFactory getSQLFactory(SQLConfig config) throws SQLException {
    return getSQLFactory(config, null);
  }

  public static SQLFactory getSQLFactory(SQLConfig config, String factoryClassNameDefaultValue)
          throws SQLException {
    String sqlFactoryClass = config.getSQLFactoryClassName(factoryClassNameDefaultValue);
    try {
      SQLFactory sqlFactory = (SQLFactory) SQLFactory.class.getClassLoader()
              .loadClass(sqlFactoryClass).getConstructor().newInstance();
      return sqlFactory;
    } catch (InstantiationException e) {
      throw new SQLException(e);
    } catch (IllegalAccessException e) {
      throw new SQLException(e);
    } catch (IllegalArgumentException e) {
      throw new SQLException(e);
    } catch (InvocationTargetException e) {
      throw new SQLException(e);
    } catch (NoSuchMethodException e) {
      throw new SQLException(e);
    } catch (SecurityException e) {
      throw new SQLException(e);
    } catch (ClassNotFoundException e) {
      throw new SQLException(e);
    }
  }

}
