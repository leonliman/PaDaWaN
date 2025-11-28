package de.uniwue.dw.query.sql;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import de.uniwue.dw.query.model.DWQueryConfig;
import de.uniwue.dw.query.model.QueryReader;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryIDFilter;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.lang.QueryStructureElem;
import de.uniwue.dw.query.model.manager.QueryManipulationManager;
import de.uniwue.dw.query.sql.util.QuerySQLGenHelper;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;
import de.uniwue.misc.util.FileUtilsUniWue;

public class SQLQueryRoot extends SQLQueryAnd {

  // a SQLManager which is used for reading the catalog and doing the actual query.
  // It is created in the constructor and reused everywhere
  private SQLManager sqlManager;

  // A helper class which is used for the creation of the SQL code.
  public QuerySQLGenHelper genHelper;

  // the statement which may currently be executed. Is null if no statment is executed at the
  // moment.
  private Statement currentStatement;

  private String bigJoinTableName;

  // the result count of the query after the results have been requested with "getResultTable()"
  public long resultCount;

  public boolean canceled = false;

  public SQLQueryRoot(SQLManager aSqlManager, QueryRoot aRoot) {
    super(aRoot);
    sqlManager = aSqlManager;
  }

  public static List<List<Object>> doQuery(SQLManager sqlManager, File queryFile)
          throws QueryException, SQLException, IOException {
    String queryXML = FileUtilsUniWue.file2String(queryFile);
    return doQuery(sqlManager, queryXML);
  }

  public static List<List<Object>> doQuery(SQLManager sqlManager, String queryXML)
          throws QueryException, SQLException {
    QueryRoot query = QueryReader.read(DWQueryConfig.getInstance().getCatalogClientManager(),
            queryXML);
    SQLQueryRoot sqlQuery = new SQLQueryRoot(sqlManager, query);
    List<List<Object>> resultTable = sqlQuery.getResultTable();
    return resultTable;
  }

  @Override
  public QueryRoot getMyQueryElem() {
    return (QueryRoot) queryElem;
  }

  private void createTmpTables(QuerySQLGenHelper genHelper) {
    for (QueryIDFilter aFilter : getMyQueryElem().getExistingPIDsFilters()) {
      SQLQueryIDFilter sqlFilter = (SQLQueryIDFilter) SQLQueryElemFactory.generateSQLElem(aFilter);
      sqlFilter.createIDFilterTmpTable(genHelper);
    }
    // List<QueryStructureElem> orContents = getOrContents();
    // for (QueryStructureElem anOrPart : orContents) {
    // createTmpTables(genHelper, anOrPart);
    // }
    createBigJoinTmpTable(genHelper);
  }

  private void createBigJoinTmpTable(QuerySQLGenHelper genHelper) {
    StringBuilder builder = genHelper.sqlQuery;
    bigJoinTableName = genHelper.getTmpTableName(this);
    builder.append("CREATE TABLE " + bigJoinTableName + " (\n");
    builder.append(
            "  InfoID BIGINT " + SQLTypes.incrementFlagStartingWith1(genHelper.sqlManager.config)
                    + " NOT NULL PRIMARY KEY,\n");
    builder.append("  PID BIGINT, ");
    createColumnsForOuterSelect(genHelper, true, false);
    if (genHelper.sqlManager.getDBType() == DBType.MSSQL) {
      builder.append(");\n");
      builder.append(
              "CREATE INDEX " + bigJoinTableName + "_pid ON " + bigJoinTableName + " (PID);\n");
      builder.append("CREATE INDEX " + bigJoinTableName + "_infoID ON " + bigJoinTableName
              + " (InfoID);\n\n");
    } else {
      builder.append(",\n" + "INDEX " + bigJoinTableName + "_pid (PID)");
      builder.append(",\n" + "INDEX " + bigJoinTableName + "_infoID (infoID) \n" + ");\n");
    }
    builder.append("\n");
  }

  @Override
  public void createColumnsForOuterSelect(QuerySQLGenHelper genHelper, boolean createTable,
          boolean finalSelect) {
    super.createColumnsForOuterSelect(genHelper, createTable, finalSelect);
    StringBuilder builder = genHelper.sqlQuery;
    builder.replace(builder.length() - 2, builder.length(), "");
  }

  public void generateSQLPreparation(QuerySQLGenHelper genHelper) throws QueryException {
    createTmpTables(genHelper);
    StringBuilder builder = genHelper.sqlQuery;
    builder.append("INSERT INTO " + bigJoinTableName + " (PID, ");
    createColumnsForOuterSelect(genHelper, false, false);
    builder.append(")");
    super.generateSQL(genHelper);
  }

  public void generateSQLPostStatement(QuerySQLGenHelper genHelper) {
    genHelper.dropTmpTables();
  }

  @Override
  public void generateSQL(QuerySQLGenHelper genHelper) {
    StringBuilder builder = genHelper.sqlQuery;

    builder.append("SELECT ");

    // for MSSQL the limit has to be defined at the beginning of the statement
    if (!getMyQueryElem().isOnlyCount() && (getMyQueryElem().getLimitResult() > 0)
            && genHelper.sqlManager.getDBType() == DBType.MSSQL) {
      builder.append("TOP " + getMyQueryElem().getLimitResult() + " ");
    }

    List<QueryStructureElem> siblings = getMyQueryElem().getSiblings();
    siblings.add(getMyQueryElem());

    if (getMyQueryElem().isOnlyCount()) {
      if (getMyQueryElem().isDistinct() || (getMyQueryElem().getIDFilterRecursive().size() == 0)) {
        builder.append("COUNT(DISTINCT PID) ");
      } else {
        builder.append("COUNT(*) ");
      }
    } else {
      if (getMyQueryElem().isDisplayPID()) {
        builder.append("PID, ");
      }
      createColumnsForOuterSelect(genHelper, false, true);
    }

    builder.append("\nFROM " + bigJoinTableName);

    List<QueryStructureElem> reducingElems = getMyQueryElem().getReducingElemsRecursive();
    for (QueryStructureElem anElem : reducingElems.toArray(new QueryStructureElem[0])) {
      if ((anElem instanceof QueryAttribute)
              && !((QueryAttribute) anElem).hasRestrictionsWithOtherAttributes()) {
        reducingElems.remove(anElem);
      }
    }

    if (!getMyQueryElem().isDistinct() && !reducingElems.isEmpty()) {
      builder.append(",\n");
      builder.append(" (SELECT max(InfoID) AS InfoID FROM " + bigJoinTableName + "\n");

      String groupColumn = "PID";
      for (QueryStructureElem anElem : reducingElems) {
        if (anElem instanceof QueryAttribute) {
          QueryAttribute anAttr = (QueryAttribute) anElem;
          SQLQueryAttribute sqlAttr = (SQLQueryAttribute) SQLQueryElemFactory
                  .generateSQLElem(anElem);
          String tableName = genHelper.getTableName(sqlAttr);
          String maxColumnName = "mV_" + tableName;
          String valueColumn = genHelper.getValueColumnName(sqlAttr);

          List<QueryIDFilter> ancestorIDFilters = anAttr.getAncestorIDFilters();
          groupColumn = "PID";
          for (QueryIDFilter aFilter : ancestorIDFilters) {
            SQLQueryIDFilter sqlFilter = (SQLQueryIDFilter) SQLQueryElemFactory
                    .generateSQLElem(aFilter);
            if (!sqlFilter.hasIDRestrictions()) {
              groupColumn = sqlFilter.getIDColumn(genHelper);
            }
          }

          builder.append(" JOIN \n (SELECT " + groupColumn + ", max(" + valueColumn + ") "
                  + maxColumnName + " FROM " + bigJoinTableName + " GROUP BY " + groupColumn + ") "
                  + tableName + "\n  ON " + bigJoinTableName + "." + groupColumn + " = " + tableName
                  + "." + groupColumn + " AND (" + bigJoinTableName + "." + valueColumn + " = "
                  + maxColumnName + " OR \n(" + bigJoinTableName + "." + valueColumn
                  + " IS NULL AND " + maxColumnName + " IS NULL))\n");
        }
      }

      builder.append(" GROUP BY " + bigJoinTableName + "." + groupColumn + ", ");

      for (QueryStructureElem anElem : reducingElems) {
        if (anElem instanceof QueryIDFilter) {
          QueryIDFilter filter = (QueryIDFilter) anElem;
          SQLQueryIDFilter sqlFilter = (SQLQueryIDFilter) SQLQueryElemFactory
                  .generateSQLElem(filter);
          if (filter.somePIDsAppearMultipleTimesInFilter()) {
            String valueColumn = sqlFilter.getFilterIDColumnForGivenIDs(genHelper);
            builder.append(bigJoinTableName + "." + valueColumn + ", ");
          } else if (filter.isDistinct()) {
            String valueColumn = sqlFilter.getIDColumn(genHelper);
            builder.append(bigJoinTableName + "." + valueColumn + ", ");
          }
        }
      }
      builder.replace(builder.length() - 2, builder.length(), "");

      builder.append(") maxTable WHERE maxTable.infoID = " + bigJoinTableName + ".infoID\n");
    }
    // for MySQL the limit has to be defined at the end of the statement
    if (!getMyQueryElem().isOnlyCount() && (getMyQueryElem().getLimitResult() > 0)
            && (genHelper.sqlManager.getDBType() != DBType.MSSQL)) {
      builder.append(" LIMIT 10");
    }
    builder.append(";\n");
  }

  public String generateSQL() throws QueryException {
    genHelper = new QuerySQLGenHelper(sqlManager);
    try {

      QueryRoot rootBackup = getMyQueryElem()
              .copyForQuery(DWQueryConfig.getInstance().getCatalogClientManager());

      rootBackup.sortChildrenForQuery();
      rootBackup.removeInactiveChildren();

      generateSQLPreparation(genHelper);
      genHelper.sqlQuery.append("\n");
      generateSQL(genHelper);
      genHelper.sqlQuery.append("\n");
      generateSQLPostStatement(genHelper);
      String sql = genHelper.sqlQuery.toString();
      queryElem = rootBackup;
      return sql;
    } catch (SQLException e) {
      e.printStackTrace();
      throw new QueryException(e);
    }
  }

  public void cancel() {
    canceled = true;
    if (currentStatement != null) {
      try {
        currentStatement.cancel();
      } catch (SQLException e) {
      }
      try {
        currentStatement.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      currentStatement = null;
    }
  }

  // The preparation consists of the creation of the temporary tables
  private void doSQLPreparation(QuerySQLGenHelper genHelper) throws SQLException, QueryException {
    genHelper.sqlQuery = new StringBuilder();
    generateSQLPreparation(genHelper);
    String sql = genHelper.sqlQuery.toString();
    if (!sql.isEmpty()) {
      currentStatement = sqlManager.createStatement();
      currentStatement.execute(sql);
      currentStatement.close();
      currentStatement = null;
    }
  }

  // The post process consists of the deletion of the temporary tables
  private void doSQLPost(QuerySQLGenHelper genHelper) throws SQLException {
    Statement st = sqlManager.createStatement();
    genHelper.sqlQuery = new StringBuilder();
    generateSQLPostStatement(genHelper);
    String sql = genHelper.sqlQuery.toString();
    if (!sql.isEmpty()) {
      st.execute(sql);
      st.close();
      st = null;
    }
  }

  private List<Object> readResultRow(ResultSet resultSet, ResultSetMetaData metaData,
          QuerySQLGenHelper genHelper) throws SQLException {
    List<Object> row = new ArrayList<>();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      Object aValue = null;
      String columnType = metaData.getColumnTypeName(i);
      if (columnType.toLowerCase().equals("decimal")) {
        if (resultSet.getBigDecimal(i) != null) {
          aValue = resultSet.getBigDecimal(i).doubleValue();
        }
      } else if (columnType.toLowerCase().equals("int")) {
        aValue = (long) resultSet.getInt(i);
      } else if (columnType.toLowerCase().equals("bigint")) {
        aValue = resultSet.getLong(i);
      } else {
        aValue = resultSet.getString(i);
      }
      row.add(aValue);
    }
    return row;
  }

  private List<List<Object>> doSQLMainQuery(QuerySQLGenHelper genHelper) throws SQLException {
    List<List<Object>> result = new ArrayList<>();
    genHelper.sqlQuery = new StringBuilder();
    generateSQL(genHelper);
    String sql = genHelper.sqlQuery.toString();
    currentStatement = sqlManager.createStatement();
    ResultSet resultSet = currentStatement.executeQuery(sql);
    ResultSetMetaData metaData = resultSet.getMetaData();
    List<Object> names = new ArrayList<>();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      String aName = metaData.getColumnLabel(i);
      names.add(aName);
    }
    result.add(names);
    while (resultSet.next()) {
      List<Object> row = readResultRow(resultSet, metaData, genHelper);
      result.add(row);
      if (canceled) {
        break;
      }
    }
    resultSet.close();
    currentStatement.close();
    currentStatement = null;
    return result;
  }

  private long getResultCountWithoutLimit(QuerySQLGenHelper genHelper) throws SQLException {
    List<List<Object>> result = new ArrayList<>();
    genHelper.sqlQuery = new StringBuilder();
    boolean formerOnlyCount = getMyQueryElem().isOnlyCount();
    getMyQueryElem().setOnlyCount(true);
    doSQLMainQuery(genHelper);
    generateSQL(genHelper);
    String sql = genHelper.sqlQuery.toString();
    Statement st = sqlManager.createStatement();
    ResultSet resultSet = st.executeQuery(sql);
    ResultSetMetaData metaData = resultSet.getMetaData();
    List<Object> names = new ArrayList<>();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      String aName = metaData.getColumnLabel(i);
      names.add(aName);
    }
    result.add(names);
    while (resultSet.next()) {
      List<Object> row = readResultRow(resultSet, metaData, genHelper);
      result.add(row);
    }
    resultSet.close();
    st.close();
    getMyQueryElem().setOnlyCount(formerOnlyCount);
    // because sometimes it's a long sometimes an integer
    long count = Long.valueOf(result.get(1).get(0).toString());
    return count;
  }

  // Creates a "result table". The returned List is a list of result rows.
  // The first row contains the column header names.
  public List<List<Object>> getResultTable() throws QueryException {
    List<List<Object>> result = new ArrayList<>();
    // do a backup because there may be done some optimization and reconfiguration
    QueryRoot rootBackup = getMyQueryElem();
    try {
      queryElem = getMyQueryElem()
              .copyForQuery(DWQueryConfig.getInstance().getCatalogClientManager());

      QueryManipulationManager manipManager = new QueryManipulationManager();
      manipManager.setQuery(getMyQueryElem());
      getMyQueryElem().shrink(manipManager);
      getMyQueryElem().sortChildrenForQuery();
      getMyQueryElem().removeInactiveChildren();

      // use this line to get the complete query including prep- and post-sql-code
      @SuppressWarnings("unused")
      String completeQuery = generateSQL();
      @SuppressWarnings("unused")
      String xml = getMyQueryElem().generateXML();

      // System.out.println(xml);
      // System.out.println(completeQuery);
      // FileUtilsUniWue.saveString2File(completeQuery, new File("D:\\Daten\\query.sql"));
      genHelper = new QuerySQLGenHelper(sqlManager);
      doSQLPreparation(genHelper); // create temp. tables
      resultCount = getResultCountWithoutLimit(genHelper);

      result = doSQLMainQuery(genHelper);
    } catch (Exception e) {
      throw new QueryException(e);
    } finally {
      try {
        doSQLPost(genHelper); // delete temp. tables
      } catch (SQLException e) {
        throw new QueryException(e);
      } finally {
        queryElem = rootBackup;
      }
    }
    return result;
  }

}
