package de.uniwue.misc.sql;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.stream.Collectors;

public class SQLTypes {

  public static String decimalType() {
    return "DECIMAL(19,4)";
  }

  public static Double getDecimalMax() {
    return 999999999999.9999;
  }

  public static Double getDecimalMin() {
    return -getDecimalMax();
  }

  /*
   * Some SQL engines do not accept timestamps earlier than the 1.1.1763, so here we do restrict the
   * earliest used timestamp even to the 1.1.1970. 00:00:00
   */
  public static Timestamp getMinTimestamp(Timestamp aTimestamp) {
    if (aTimestamp.getTime() < 0) {
      aTimestamp = new Timestamp(0);
    }
    return aTimestamp;
  }

  // ATTENTION: the nanosec precision should be high, since it is used to calculate different dates
  // on another key variable, e.g. caseid or extid
  public final static int fractionalSecondsPrecision = 6;

  public static String timestampType(SQLConfig aConfig) {
    if (aConfig.dbType == DBType.MSSQL) {
      return "DATETIME2(" + fractionalSecondsPrecision + ")";
    } else {
      return "DATETIME";
    }
  }

  public static String getCurrentTimestamp(SQLConfig aConfig) {
    if (aConfig.dbType == DBType.MSSQL) {
      return "(GETDATE())";
    } else {
      return "CURRENT_TIMESTAMP";
    }
  }

  public static String bigTextType(SQLConfig aConfig) {
    if (aConfig.dbType == DBType.MSSQL) {
      return "VARCHAR(MAX)";
    } else {
      return "MEDIUMTEXT";
    }
  }

  public static String incrementFlagStartingWith1(SQLConfig aConfig) {
    if (aConfig.dbType == DBType.MSSQL) {
      return "IDENTITY(1, 1)";
    } else {
      return "AUTO_INCREMENT";
    }
  }

  public static String getNullChecker(SQLConfig aConfig) {
    if (aConfig.dbType == DBType.MSSQL) {
      return "ISNULL";
    } else {
      return "IFNULL";
    }
  }

  public static String createUniqueConstraint(SQLConfig aConfig, String constraintName,
          String... columns) {
    String cols = Arrays.stream(columns).collect(Collectors.joining(", "));
    cols = " (" + cols + ")";
    if (aConfig.dbType == DBType.MSSQL) {
      return "CONSTRAINT " + constraintName + " UNIQUE " + cols;
    } else {
      return "UNIQUE INDEX " + constraintName + cols;
    }
  }

}
