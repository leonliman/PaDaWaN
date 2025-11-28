package de.uniwue.dw.exchangeFormat;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.misc.util.ConfigException;
import net.lingala.zip4j.ZipFile;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

public class EFConsoleStarter {

  public static void main(String[] args)
          throws IOException, ConfigException, SQLException, ParseException, GUIClientException,
          IndexException {
    if (args.length <= 2) {
      System.out.println("Possible parameters: <Path-to-Config-File> <Mode> <ModeParameters>");
      System.out.println("Modes:");
      System.out.println(" - import (Parameters: <Optional-Path-To-Catalog-Export> <Path-To-Facts-Export>)");
      System.out.println(" - export (Parameters: <Path-To-Export-Catalog-To> <Path-To-Export-Facts-To>)");
      System.out.println(" - exportByQuery (Parameters: <Path-To-MXQL-Query-File> <Path-To-Generate-Zip-File-To>)");
      System.out.println(" - importZip (Parameters: <Path-To-Ziped-Export> " +
              "<Optional-boolean-for-withAuthManagerReset> <Optional-boolean-for-withSolrIndexRebuild)");
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

    switch (args[1]) {
      case "import":
        EFImport.doImport(args.length == 3 ? null : args[2], args.length == 3 ? args[2] : args[3], null, null, true);
        break;
      case "export":
        EFExport.doExport(args[2], args[3], true);
        break;
      case "exportByQuery":
        EFExportByQuery.doExport(EFExportByQuery.loadQuery(new File(args[2])), new File(args[3]));
        break;
      case "importZip":
        EFImport.doImport(new ZipFile(new File(args[2])), args.length >= 4 && Boolean.parseBoolean(args[3]),
                args.length == 5 && Boolean.parseBoolean(args[4]));
        break;
    }
  }

}