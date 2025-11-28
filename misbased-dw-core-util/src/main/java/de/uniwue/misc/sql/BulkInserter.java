package de.uniwue.misc.sql;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.misc.util.StringUtilsUniWue;

public class BulkInserter {

  private static Logger logger = LogManager.getLogger(BulkInserter.class);

  public static final String DEFAULT_FIELD_TERMINATOR = "|~|";

  public static final String DEFAULT_ROW_TERMINATOR = "$~$";

  public static final int FLUSH_AFTER_ROWS = 1000000;

  private DecimalFormat df;

  private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  private String fieldTerminator = DEFAULT_FIELD_TERMINATOR;

  private String rowTerminator = DEFAULT_ROW_TERMINATOR;

  /*
   * The csvFile and writer exist and are used as instance members for inserting. When the actual
   * bulk insert is performed the references to the file are passed as method arguments because the
   * parallelization mechanism has perhaps already cleared the instance members for use of the next
   * thread.
   */
  private Path csvFile;

  private BufferedWriter writer;

  private String bulkFolder;

  private String bulkFolderDestinationHostPerspective;

  private int unflushedRows = 0;

  private boolean keepIdentity = false;

  private DatabaseManager databaseManager;

  private Random rnd = new Random();

  private boolean parallelizeInsert;

  public BulkInserter() {
  }


  /**
   * @param aBulkFolder
   *          folder, in which the csv files are buffered before they are inserted into the db. The
   *          folder must be on the same machine as the MSSQL instance and the MSSQL instance must
   *          have the right to access this folder.
   * @throws IOException
   */
  public BulkInserter(DatabaseManager aDatabaseManager, String aBulkFolder) throws IOException {
    databaseManager = aDatabaseManager;
    bulkFolder = aBulkFolder;
    bulkFolderDestinationHostPerspective = SQLPropertiesConfiguration.getSQLBulkImportDirDestinationHostPerspective();
    df = getDecimalFormat();
  }


  /**
   * @param bulkImportDir
   *          folder, in which the csv files are buffered before they are inserted into the db. The
   *          folder must be on the same machine as the MSSQL instance and the MSSQL instance must
   *          have the right to access this folder.
   * @throws IOException
   */
  public BulkInserter(DatabaseManager aDatabaseManager, File bulkImportDir) throws IOException {
    this(aDatabaseManager, bulkImportDir == null ? null : bulkImportDir.getAbsolutePath());
  }


  public static DecimalFormat getDecimalFormat() {
    DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(Locale.GERMAN);
    otherSymbols.setDecimalSeparator('.');
    return new DecimalFormat("#.####", otherSymbols);
  }


  private SQLManager getSQLManager() {
    return databaseManager.getSQLManager();
  }


  private Charset getCharset() {
    if (getSQLManager().config.dbType == DBType.MSSQL) {
      return StandardCharsets.UTF_16;
    } else {
      return StandardCharsets.UTF_8;
    }
  }


  public void createWriter() throws IOException {
    Path folderPath = FileSystems.getDefault().getPath(bulkFolder);
    if (!Files.exists(folderPath)) {
      Files.createDirectories(folderPath);
    }
    String filename = rnd.nextLong() + ".csv";
    csvFile = folderPath.resolve(filename);
    // System.out.println(csvFile.toFile().getAbsolutePath());
    writer = Files.newBufferedWriter(csvFile, getCharset());
  }


  /**
   * Adds a data row to the table. The size of the object list must match the column size of the
   * table. By default the given IDs will be ignored and the db will generate new ones. Use
   * setKeepIdentity() to change this behavior. After every FLUSH_AFTER_ROWS added rows, the
   * inserter will automatically insert these rows.
   */
  public void addRow(Object... objects) throws IOException, SQLException {
    if (writer == null) {
      createWriter();
    }
    String row = getRow(fieldTerminator, rowTerminator, df, getSQLManager().config.dbType, objects);
    writer.write(row);
    writer.write(rowTerminator);
    unflushedRows++;
    if (unflushedRows >= FLUSH_AFTER_ROWS) {
      writer.flush();
      executeInsert();
    }
  }

  private Pattern fieldTermPattern = Pattern.compile(fieldTerminator);

  private Pattern rowTermPattern = Pattern.compile(rowTerminator);

  public String getRow(String fieldTerminator, String rowTerminator, DecimalFormat df, DBType dbType,
          Object... objects) {
    boolean first = true;
    StringBuilder result = new StringBuilder();
    for (Object o : objects) {
      if (o == null) {
        if (dbType == DBType.MySQL)
          o = "\\N";
        else
          o = "";
      }
      if (o instanceof Double) {
        o = df.format(o);
      } else if (o instanceof Timestamp) {
        o = dateFormat.format(new Date(((Timestamp) o).getTime()));
      } else if (o instanceof Boolean) {
        o = Integer.valueOf(DatabaseManager.getBooleanInt((Boolean) o));
      }
      if (first) {
        first = false;
      } else {
        result.append(fieldTerminator);
      }
      String bulkCellValue = o.toString();
      if (fieldTermPattern.matcher(bulkCellValue).find()) {
        bulkCellValue = bulkCellValue.replaceAll(fieldTerminator, "");
      }
      if (rowTermPattern.matcher(bulkCellValue).find()) {
        bulkCellValue = bulkCellValue.replaceAll(rowTerminator, "");
      }
      result.append(bulkCellValue);
    }
    return result.toString();
  }


  public void importFile(File aBulkFile) throws SQLException, IOException {
    executeBulkInsertStatement(aBulkFile);
  }


  /**
   * The added rows will be inserted in the table. Note: check the setKeepIdentity()-method for the
   * ID handling.
   */
  public void executeInsert() throws SQLException, IOException {
    if (writer == null) {
      return;
    }
    try {
      writer.close();
    } catch (IOException e) {
      cleanup();
      throw e;
    }
    Path oldCSVFile = csvFile;
    if (isParallelizeInsert()) {
      Thread thread = new Thread() {
        @Override
        public void run() {
          try {
            executeBulkInsertStatement(oldCSVFile.toFile());
          } catch (SQLException e) {
            e.printStackTrace();
          } finally {
            if (oldCSVFile != null) {
              try {
                Files.deleteIfExists(oldCSVFile);
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
          }
        }
      };
      thread.start();
      try {
        if (writer != null) {
          writer.close();
        }
      } finally {
        writer = null;
        csvFile = null;
        unflushedRows = 0;
      }
    } else {
      executeBulkInsertStatement(csvFile.toFile());
      cleanup();
    }
  }


  /**
   * The oldCSVFile is passed because of reasons explained above in the comment above the csvFile
   * instance member
   */
  private DatabaseManager createTmpTable(File oldCSVFile) throws SQLException {
    DatabaseManager tmpManager = new DatabaseManager(getSQLManager()) {

      @Override
      public String getTableName() {
        String name = "tmp" + FilenameUtils.removeExtension(oldCSVFile.getName()).replaceAll("-", "_");
        return name;
      }


      @Override
      protected String getCreateTableString() {
        String command = getCreateTableStub();
        try {
          boolean isFirst = true;
          Set<String> keyColumns = databaseManager.getKeyColumns();
          Set<String> columnNames = databaseManager.getColumnNames();
          for (String aColumn : columnNames) {
            if (isFirst) {
              isFirst = false;
            } else {
              command += ", \n";
            }
            command += aColumn + " " + databaseManager.getColumnType(aColumn);
            if (keyColumns.contains(aColumn)) {
              command += " NOT NULL";
            }
          }
          command += ", \n";
          command += "PRIMARY KEY (";
          isFirst = true;
          for (String aKeyColumn : keyColumns) {
            if (isFirst) {
              isFirst = false;
            } else {
              command += ", ";
            }
            command += aKeyColumn;
          }
          command += ")";
          if (getSQLManager().config.dbType == DBType.MSSQL) {
            command += " WITH ( IGNORE_DUP_KEY = ON )";
          }
          command += ")";
        } catch (SQLException e) {
          e.printStackTrace();
          command = "";
        }
        return command;
      }


      @Override
      protected void readResult(ResultSet resultSet) throws SQLException {
      }

    };
    tmpManager.createSQLTables();
    return tmpManager;
  }


  private void mergeMySQL(DatabaseManager tmpTable) throws SQLException {
    Set<String> keyColumns = new HashSet<String>();
    for (String aKeyColumn : databaseManager.getKeyColumns()) {
      keyColumns.add(aKeyColumn.toLowerCase());
    }
    Set<String> columnNames = databaseManager.getColumnNames();
    Set<String> noUpdateColumns = databaseManager.getNoUpdateColumns();
    for (String aColumn : columnNames.toArray(new String[0])) {
      if (databaseManager.isAutoincrementColumn(aColumn) && !databaseManager.isIdentityInsert()) {
        columnNames.remove(aColumn);
      }
    }
    // if (keepIdentity) {
    // getSQLManager().executeSQL("SET IDENTITY_INSERT " + getTableName() + " ON");
    // }
    String command = "INSERT INTO " + getTableName() + " SELECT * FROM " + tmpTable.getTableName() + "\n"
            + "ON DUPLICATE KEY UPDATE ";
    boolean isFirst = true;
    for (String aColumn : columnNames) {
      if (!keyColumns.contains(aColumn) && !noUpdateColumns.contains(aColumn)
              && !databaseManager.isAutoincrementColumn(aColumn)) {
        if (isFirst) {
          isFirst = false;
        } else {
          command += ", \n";
        }
        command += getTableName() + "." + aColumn + " = " + tmpTable.getTableName() + "." + aColumn;
      }
    }
    getSQLManager().executeSQL(command);
    // if (keepIdentity) {
    // getSQLManager().executeSQL("SET IDENTITY_INSERT " + getTableName() + " OFF");
    // }
  }


  private void mergeMSSQL(DatabaseManager tmpTable) throws SQLException {
    Set<String> keyColumns = new HashSet<String>();
    for (String aKeyColumn : databaseManager.getKeyColumns()) {
      keyColumns.add(aKeyColumn.toLowerCase());
    }
    Set<String> columnNames = databaseManager.getColumnNames();
    Set<String> noUpdateColumns = databaseManager.getNoUpdateColumns();
    for (String aColumn : columnNames.toArray(new String[0])) {
      if (databaseManager.isAutoincrementColumn(aColumn) && !databaseManager.isIdentityInsert()) {
        columnNames.remove(aColumn);
      }
    }
    if (keepIdentity) {
      getSQLManager().executeSQL("SET IDENTITY_INSERT " + getTableName() + " ON");
    }
    String command = "MERGE " + getTableName() + " AS target \n" + "USING (SELECT * FROM " + tmpTable.getTableName()
            + ") AS source \n" + "ON (";
    boolean isFirst = true;
    for (String keyColumn : keyColumns) {
      if (isFirst) {
        isFirst = false;
      } else {
        command += " AND \n";
      }
      command += "target." + keyColumn + " = source." + keyColumn;
    }
    command += ") \n" + "WHEN MATCHED THEN UPDATE SET ";
    isFirst = true;
    for (String aColumn : columnNames) {
      if (!keyColumns.contains(aColumn) && !noUpdateColumns.contains(aColumn)
              && !databaseManager.isAutoincrementColumn(aColumn)) {
        if (isFirst) {
          isFirst = false;
        } else {
          command += ", \n";
        }
        command += "target." + aColumn + " = source." + aColumn;
      }
    }
    command += "\n" + "WHEN NOT MATCHED BY TARGET THEN INSERT (" + StringUtilsUniWue.concat(columnNames, ", ")
            + ") VALUES (";
    isFirst = true;
    for (String aColumn : columnNames) {
      if (isFirst) {
        isFirst = false;
      } else {
        command += ", \n";
      }
      command += "source." + aColumn;
    }
    command += ");";
    getSQLManager().executeSQL(command);
    if (keepIdentity) {
      getSQLManager().executeSQL("SET IDENTITY_INSERT " + getTableName() + " OFF");
    }
  }


  /**
   * The oldCSVFile is passed because of reasons explained above in the comment above the csvFile
   * instance member
   */
  private void executeBulkInsertViaTmpTableAndMerge(File oldCSVFile) throws SQLException {
    DatabaseManager tmpTable = createTmpTable(oldCSVFile);
    executeBulkInsertInTable(tmpTable.getTableName(), oldCSVFile);
    if (getSQLManager().config.dbType == DBType.MSSQL) {
      mergeMSSQL(tmpTable);
    } else {
      mergeMySQL(tmpTable);
    }
    tmpTable.dropTable();
    tmpTable.dispose();
  }


  /**
   * The oldCSVFile is passed because of reasons explained above in the comment above the csvFile
   * instance member
   */
  private void executeBulkInsertInTable(String tableName, File oldCSVFile) throws SQLException {
    String sql = "";
    if (bulkFolderDestinationHostPerspective != null) {
      String filePath = oldCSVFile.getAbsolutePath();
      filePath = filePath.replace(bulkFolder, bulkFolderDestinationHostPerspective);
      oldCSVFile = new File(filePath);
    }
    if (getSQLManager().config.dbType == DBType.MSSQL) {
      String keepIdentityArg = keepIdentity ? "KEEPIDENTITY, " : "";
      sql += "BULK INSERT " + tableName + " FROM '" + oldCSVFile.toString() + "'\n" + "WITH (" + keepIdentityArg
              + "FIELDTERMINATOR = '" + fieldTerminator + "', ROWTERMINATOR = '" + rowTerminator + "', KEEPNULLS )";
    } else if (getSQLManager().config.dbType == DBType.MySQL) {
      String path = oldCSVFile.getPath();
      path = path.replace("\\", "/");
      String charsetName = getCharset().toString().replace("-", "") + "MB4";
      // to enable this on a MySQL 8 server add the following line to the server's my.ini and
      // restart the server:
      // 'local_infile=1'
      sql += "LOAD DATA LOCAL INFILE '" + path + "'" + " INTO TABLE " + tableName + " CHARACTER SET " + charsetName
              + " FIELDS TERMINATED BY '" + fieldTerminator + "' LINES TERMINATED BY '" + rowTerminator + "'";
    } else {
      throw new SQLException("unsupported SQL type");
    }
    PreparedStatement stmt = getSQLManager().createPreparedStatement(sql);
    stmt.execute();
    stmt.close();
  }


  /**
   * The oldCSVFile is passed because of reasons explained above in the comment above the csvFile
   * instance member
   */
  private void executeBulkInsertStatement(File oldCSVFile) throws SQLException {
    if (oldCSVFile.length() > 0) {
      if (databaseManager.getInsertMode() == SQLInsertMode.bulkInsert) {
        executeBulkInsertInTable(databaseManager.getTableName(), oldCSVFile);
      } else if (databaseManager.getInsertMode() == SQLInsertMode.bulkInsertTmpTableMerge) {
        executeBulkInsertViaTmpTableAndMerge(oldCSVFile);
      } else {
        throw new SQLException("unknown bulk insert mode");
      }
    }
  }


  public void dispose() {
    try {
      cleanup();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  public void cleanup() throws IOException {
    unflushedRows = 0;
    try {
      if (writer != null) {
        writer.close();
      }
      if (csvFile != null) {
        Files.deleteIfExists(csvFile);
      }
    } finally {
      writer = null;
      csvFile = null;
    }
  }


  /**
   * Retrieves whether the given IDs are used or new ones will be generated by the db.
   *
   * @return
   */
  public boolean isKeepIdentity() {
    return keepIdentity;
  }


  /**
   * Enables the identity insert. The given IDs will be used. If keepIdentity is set to false, the
   * db will generated IDs and ignore the given IDs. The default value is false.
   *
   * @param keepIdentity
   *          true enable identity insert, false uses generated IDs
   */
  public void setKeepIdentity(boolean keepIdentity) {
    this.keepIdentity = keepIdentity;
  }


  public String getTableName() {
    return databaseManager.getTableName();
  }


  public String getFieldTerminator() {
    return fieldTerminator;
  }


  public void setFieldTerminator(String fieldTerminator) {
    this.fieldTerminator = fieldTerminator;
  }


  public String getRowTerminator() {
    return rowTerminator;
  }


  public void setRowTerminator(String rowTerminator) {
    this.rowTerminator = rowTerminator;
  }


  public boolean isParallelizeInsert() {
    return parallelizeInsert;
  }


  public void setParallelizeInsert(boolean parallelizeInsert) {
    this.parallelizeInsert = parallelizeInsert;
  }

}
