package de.uniwue.dw.core.client.authentication;

import javax.security.auth.login.AccountException;

import de.uniwue.misc.util.ConfigException;

public abstract class AuthenticationService {

  public static final String INVALID_CREDENTIALS = "The credentials are invalid.";

  public static final String INVALID_CONFIG = "Authentification config is incorrect.";

  /**
   * 
   * @param username
   * @param password
   * @return the authenticated user
   * @throws AccountException
   *           if the user credentials were incorrect
   * @throws ConfigException
   *           technical errors during the authentication
   */
  public abstract User authenticate(String username, String password)
          throws AccountException, ConfigException;

  /**
   * 
   * @return a boolean that indicates of the authentication system is online
   */
  public abstract boolean isOnline() throws AccountException;

  public AuthenticationService getSystem() {
    return this;
  }

  /**
   * @return a string containing the name of the authentication service
   */
  public abstract String getServiceName();

  /**
   * 
   * @param username
   * @return a array with account informations like first/last name, email, etc.
   * @throws AccountException
   */
  public abstract String[] requestUserData(String username)
          throws AccountException, ConfigException;

//  public abstract Collection<String> getUsers();
//  
//  public abstract Collection<Group> getGroups();
  
  public String toString() {
    return getServiceName();
  }

}
