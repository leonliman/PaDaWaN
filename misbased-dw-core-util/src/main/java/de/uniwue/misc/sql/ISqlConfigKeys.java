package de.uniwue.misc.sql;

public interface ISqlConfigKeys {

  public static final String PARAM_SQL_SERVER = "sql.server";

  public static final String PARAM_SQL_DB_TYPE = "sql.db_type";

  public static final String PARAM_SQL_DB_NAME = "sql.db_name";

  public static final String PARAM_SQL_USER_NAME = "sql.user";

  public static final String PARAM_SQL_PASSWORD = "sql.password";

  public static final String PARAM_SQL_FACTORY_CLASS = "sql.factoryClassName";

  /**
   * String parameter that specifies the folder used for bulk imports. Beware that this folder has
   * to be relative to the machine the SQL server is running on as bulk inserts are processed on the
   * machine the sql-server service is running on, not the machine on which the code ordering the
   * bulk insert is running on. So make this path an absolute path on the sql server machine on
   * which the java code is also running on, or a network path which is accessible from both the sql
   * server machine and the java machine.
   */
  public static final String PARAM_SQL_BULK_IMPORT_DIR = "sql.bulk_import_dir";

  public static final String PARAM_SQL_BULK_IMPORT_DIR_DESTINATION_HOST_PERSPECTIVE = "sql.bulk_import_dir.destination_host_perspective";

  public static final String PARAM_SQL_BULK_INSERT_MERGE = "sql.bulk_import_merge";

  // the login to the database can be the login of the currently logged in user. In this mode no
  // further user or password are required. This is a feature which has not yet been thoroughly
  // tested
  public static final String PARAM_SQL_TRUSTED_CONNECTION = "sql.trusted_connection";

}
