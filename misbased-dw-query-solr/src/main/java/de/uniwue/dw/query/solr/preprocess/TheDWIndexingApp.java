package de.uniwue.dw.query.solr.preprocess;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;

import java.io.File;

public class TheDWIndexingApp {

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println("you must give a properties file to run the indexer");
      return;
    }
    DwClientConfiguration.loadProperties(new File(args[0]));
    try {
      String sqlType = DwClientConfiguration.getInstance().getParameter("sql.db_type");
      if (sqlType.equalsIgnoreCase("MySQL"))
        Class.forName("com.mysql.cj.jdbc.Driver").getDeclaredConstructor().newInstance();
      else if (sqlType.equalsIgnoreCase("MSSQL"))
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      e.printStackTrace();
    }
    NestedDocumentIndexer indexer = new NestedDocumentIndexer();
    indexer.update();
  }

}
