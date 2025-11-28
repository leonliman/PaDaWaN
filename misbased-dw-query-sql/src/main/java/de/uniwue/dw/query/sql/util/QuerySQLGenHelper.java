package de.uniwue.dw.query.sql.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.sql.SQLInfoAdapter;
import de.uniwue.dw.query.model.lang.QueryStructureElem;
import de.uniwue.dw.query.sql.SQLQueryAttribute;
import de.uniwue.dw.query.sql.SQLQueryStructureElem;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;
import de.uniwue.misc.util.EnvironmentUniWue;

public class QuerySQLGenHelper {

  public SQLManager sqlManager;

  private int resultTableCounter = 0;

  private static int maxColumnNameLength = 64;

  private long queryID = Math.abs(EnvironmentUniWue.random.nextLong());

  // private long queryID = 0L;

  // which column in the result table belongs to which element ?
  public Map<String, QueryStructureElem> column2Elem = new HashMap<String, QueryStructureElem>();

  public Set<String> timeColumns = new HashSet<String>();

  public Set<String> yearColumns = new HashSet<String>();

  public Set<String> docIDColumns = new HashSet<String>();

  public Set<String> caseIDColumns = new HashSet<String>();

  // the table an attribute created
  public Map<QueryStructureElem, String> attr2tableNameMap = new HashMap<QueryStructureElem, String>();

  // the value column (not the time column) an attribute created in the result table
  public Map<QueryStructureElem, String> attr2columnNameMap = new HashMap<QueryStructureElem, String>();

  // the builder that contains the final sql query
  public StringBuilder sqlQuery = new StringBuilder();

  // the temporary tables for different QueryElements
  public Map<QueryStructureElem, String> tmpTableNames = new HashMap<QueryStructureElem, String>();

  public QuerySQLGenHelper(SQLManager anSQLManager) {
    sqlManager = anSQLManager;
  }

  public String getColumnDataType(SQLQueryAttribute anAttr) {
    if (anAttr.getMyQueryElem().getCatalogEntry() == null) {
      return "VARCHAR(" + SQLInfoAdapter.VALUE_SHORT_LENGTH + ")";
    } else if (anAttr.getMyQueryElem().getCatalogEntry().getDataType() == CatalogEntryType.Number) {
      return SQLTypes.decimalType();
    } else {
      if (anAttr.getMyQueryElem().getValueInFile()) {
        if (sqlManager.getDBType() == DBType.MSSQL) {
          return "VARCHAR(MAX)";
        } else {
          return "MEDIUMTEXT";
        }
      } else {
        return "VARCHAR(" + SQLInfoAdapter.VALUE_SHORT_LENGTH + ")";
      }
    }
  }

  public String getColumnTimeDataType() {
    if (sqlManager.getDBType() == DBType.MSSQL) {
      return "DATETIME2(0)";
    } else {
      return "DATETIME";
    }
  }

  public String getTmpTableName(SQLQueryStructureElem anElem) {
    if (tmpTableNames.containsKey(anElem.getMyQueryElem())) {
      return tmpTableNames.get(anElem.getMyQueryElem());
    } else {
      String result = "queryTmp_" + anElem.getXMLName() + "_" + getTableName(anElem) + "_"
              + queryID;
      tmpTableNames.put(anElem.getMyQueryElem(), result);
      return result;
    }
  }

  public void dropTmpTables() {
    for (String aTableName : tmpTableNames.values()) {
      sqlQuery.append("DROP TABLE " + aTableName + ";\n");
    }
  }

  public String getTableName(SQLQueryStructureElem anAttr) {
    if (attr2tableNameMap.containsKey(anAttr.getMyQueryElem())) {
      return attr2tableNameMap.get(anAttr.getMyQueryElem());
    }
    String tableName = "a" + resultTableCounter;
    resultTableCounter++;
    attr2tableNameMap.put(anAttr.getMyQueryElem(), tableName);
    return tableName;
  }

  public String getPIDColumnName(SQLQueryStructureElem anElem) {
    String result = "";
    if (anElem instanceof SQLQueryAttribute) {
      result = getValueColumnName((SQLQueryAttribute) anElem);
    } else {
      result = getTableName(anElem) + "_" + anElem.getXMLName();
    }
    result += "_pid";
    column2Elem.put(result, anElem.getMyQueryElem());
    return result;
  }

  public String getMeasureTimeColumnName(SQLQueryStructureElem anElem) {
    String columnName;
    if (anElem instanceof SQLQueryAttribute) {
      columnName = getValueColumnName((SQLQueryAttribute) anElem);
    } else {
      columnName = getTableName(anElem) + "_" + anElem.getXMLName();
    }
    String result = columnName + "_time";
    column2Elem.put(result, anElem.getMyQueryElem());
    timeColumns.add(result);
    return result;
  }

  public String getCaseIDColumnName(SQLQueryStructureElem anElem) {
    String result = "";
    if (anElem instanceof SQLQueryAttribute) {
      result = getValueColumnName((SQLQueryAttribute) anElem);
    } else {
      result = getTableName(anElem) + "_" + anElem.getXMLName();
    }
    result += "_caseID";
    column2Elem.put(result, anElem.getMyQueryElem());
    caseIDColumns.add(result);
    return result;
  }

  public String getDocIDColumnName(SQLQueryStructureElem anElem) {
    String result = "";
    if (anElem instanceof SQLQueryAttribute) {
      result = getValueColumnName((SQLQueryAttribute) anElem);
    } else {
      result = getTableName(anElem) + "_" + anElem.getXMLName();
    }
    result += "_docID";
    column2Elem.put(result, anElem.getMyQueryElem());
    docIDColumns.add(result);
    return result;
  }

  public String getYearColumnName(SQLQueryStructureElem anElem) {
    String result = "";
    if (anElem instanceof SQLQueryAttribute) {
      result = getValueColumnName((SQLQueryAttribute) anElem);
    } else {
      result = getTableName(anElem) + "_" + anElem.getXMLName();
    }
    result += "_year";
    column2Elem.put(result, anElem.getMyQueryElem());
    yearColumns.add(result);
    return result;
  }

  // returns the name of the sql-column which should be generated for this attribute
  public String getValueColumnName(SQLQueryAttribute anAttr) {
    if (attr2columnNameMap.containsKey(anAttr.getMyQueryElem())) {
      return attr2columnNameMap.get(anAttr.getMyQueryElem());
    }
    String tableName = getTableName(anAttr);
    String columnName = tableName;
    columnName += "_" + (anAttr.getMyQueryElem().getCatalogEntry().getExtID() + "_"
            + anAttr.getMyQueryElem().getCatalogEntry().getProject());
    columnName = columnName.replaceAll("\\s+", "_");
    columnName = columnName.replaceAll("[^a-zA-Z0-9öüäÖÜÄ]", "_");
    if (columnName.length() > maxColumnNameLength) {
      columnName = columnName.substring(0, maxColumnNameLength - 1);
    }
    String uniqueName = columnName;
    int i = 1;
    while (column2Elem.containsKey(uniqueName)
            && (column2Elem.get(uniqueName) != anAttr.getMyQueryElem())) {
      i++;
      uniqueName = columnName + i;
    }
    column2Elem.put(columnName, anAttr.getMyQueryElem());
    attr2columnNameMap.put(anAttr.getMyQueryElem(), uniqueName);
    return columnName;
  }

}
