package de.uniwue.dw.core.client.authentication;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.group.AuthManager;
import de.uniwue.misc.util.ConfigException;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

public class AddPerm {

  public static void main(String[] args) throws IOException, NoSuchAlgorithmException,
          ConfigException, SQLException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
          InvocationTargetException, InstantiationException {
    if (args.length < 1) {
      System.out.println("Usage: \n"
              + "AddPerm <propFile> -u <userName> <password> <firstName> <lastName> <email> \n"
              + "AddPerm <propFile> -g <groupName> <kAnonymity:INT> <caseQuery:0|1> <admin:0|1> \n"
              + "AddPerm <propFile> -ug <userName> <groupName> \n");
      return;
    }
    if (args[1].equals("-u") && (args.length != 7)) {
      System.out
              .println("Usage: AddPerm <propFile> -u <userName> <password> <firstName> <lastName> <email>\n"
                      + "Not right amound of parameters");
    }
    if (args[1].equals("-g") && (args.length != 6)) {
      System.out
              .println("Usage: AddPerm <propFile> -g <groupName> <kAnonymity:INT> <caseQuery:0|1> <admin:0|1>\n"
                      + "Not right amound of parameters");
    }
    if (args[1].equals("-ug") && (args.length != 4)) {
      System.out.println("Usage: AddPerm <propFile> -ug <userName> <groupName>\n"
              + "Not right amound of parameters");
    }
    File config = new File(args[0]);
    DwClientConfiguration.getInstance().loadProperties(config);
    String sqlType = DwClientConfiguration.getInstance().getParameter("sql.db_type");
    if (sqlType.equalsIgnoreCase("MySQL"))
      Class.forName("com.mysql.cj.jdbc.Driver").getDeclaredConstructor().newInstance();
    else if (sqlType.equalsIgnoreCase("MSSQL"))
      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").getDeclaredConstructor().newInstance();
    AuthManager authManager = DwClientConfiguration.getInstance().getAuthManager();
    if (args[1].equals("-u")) {
      authManager.addUser(args[2], args[3], args[4], args[5], args[6]);
    }
    if (args[1].equals("-g")) {
      int kAnonymity = Integer.valueOf(args[3]);
      if (!args[4].matches("0|1")) {
        System.out
                .println("Usage: AddPerm <propFile> -g <groupName> <kAnonymity:INT> <caseQuery:0|1> <admin:0|1>\n"
                        + "parameter 'caseQuery' is not right");
        return;
      }
      boolean caseQuery = true;
      if (args[4].equals("0")) {
        caseQuery = false;
      }
      if (!args[5].matches("0|1")) {
        System.out
                .println("Usage: AddPerm <propFile> -g <groupName> <kAnonymity:INT> <caseQuery:0|1> <admin:0|1>\n"
                        + "parameter 'admin' is not right");
        return;
      }
      boolean admin = false;
      if (args[5].equals("1")) {
        admin = true;
      }
      authManager.addGroup(args[2], kAnonymity, caseQuery, admin);
    }
    if (args[1].equals("-ug")) {
      authManager.addUser2Group(args[2], args[3]);
    }
  }

}
