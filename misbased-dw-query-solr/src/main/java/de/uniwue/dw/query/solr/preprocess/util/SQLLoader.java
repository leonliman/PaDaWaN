package de.uniwue.dw.query.solr.preprocess.util;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;

public class SQLLoader {

  private static final String SQL_COMMENT_IDENTIFIER = "--";

  private HashMap<String, String> name2stmt = new HashMap<>();

  public SQLLoader(String filename) throws IOException {
    this(new File(TextIndexerHelper.class.getResource("/" + filename).getPath()));
  }

  public SQLLoader(File file) throws IOException {
    loadSatements(file);
  }

  private void loadSatements(File file) throws IOException {
    Scanner sc = new Scanner(file);
    StringBuilder stmt = new StringBuilder();
    String stmtName = null;
    while (sc.hasNext()) {
      String line = sc.nextLine();
      if (line.trim().startsWith(SQL_COMMENT_IDENTIFIER)) {
        line = line.trim().substring(SQL_COMMENT_IDENTIFIER.length()).trim();
        if (stmtName != null) {
          name2stmt.put(stmtName, stmt.toString());
        }
        stmtName = line;
        stmt.setLength(0);
      } else
        stmt.append(line + "\n");
    }
    if (stmtName != null) {
      name2stmt.put(stmtName, stmt.toString());
    }
    sc.close();
  }

  public String getStmt(String key) {
    return name2stmt.get(key);
  }

  public static String getStmt(String filename, String key) throws IOException {
    return new SQLLoader(filename).getStmt(key);
  }

  public static void main(String[] args) throws IOException {
    String stmt = getStmt("sql/Statements.sql", "update.lowerbound");
    System.out.println(stmt);
  }
}
