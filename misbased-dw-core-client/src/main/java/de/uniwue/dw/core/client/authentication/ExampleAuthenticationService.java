package de.uniwue.dw.core.client.authentication;

import javax.security.auth.login.AccountException;

public class ExampleAuthenticationService extends AuthenticationService {

  @Override
  public User authenticate(String username, String password) throws AccountException {
    if (username.equals("test") && password.equals("test")) {
      return new User("username", "firstname", "lastname", "email");
    } else {
      throw new AccountException(INVALID_CREDENTIALS);
    }
  }

  @Override
  public boolean isOnline() {
    return true;
  }

  @Override
  public String getServiceName() {
    return "Example Authentication Service";
  }

  @Override
  public String[] requestUserData(String username) throws AccountException {
    return new String[] { "Max", "Mustermann", "test@test.test" };
  }

}
