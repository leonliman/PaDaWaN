package de.uniwue.dw.core.client.authentication;

import de.uniwue.misc.util.ConfigException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

public class AddStdUserCredentials {

  public static void main(String[] args) throws NoSuchAlgorithmException, IOException,
          ConfigException, SQLException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
          InstantiationException, IllegalAccessException {
    AddPerm.main(new String[] { args[0], "-u", "testUser", "testPassword", "testFirstname",
            "testLastname", "testEmail" });
    AddPerm.main(new String[] { args[0], "-g", "testGroup", "10", "1", "1" });
    AddPerm.main(new String[] { args[0], "-ug", "testUser", "testGroup" });
  }

}
