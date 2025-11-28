package de.uniwue.dw.core.client.authentication;

import java.sql.SQLException;
import java.util.Optional;

import javax.security.auth.login.AccountException;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.group.AuthManager;
import de.uniwue.misc.util.ConfigException;

public class SQLAuthenticationService extends AuthenticationService {

  private AuthManager authManager;

  public SQLAuthenticationService() throws SQLException {
    super();
    authManager = DwClientConfiguration.getInstance().getAuthManager();
  }

  @Override
  public boolean isOnline() {
    try {
      authManager.getGroups();
    } catch (SQLException e) {
      return false;
    }
    return true;
  }

  @Override
  public User authenticate(String username, String passsword)
          throws AccountException, ConfigException {
    return authManager.authenticate(username, passsword, this);
  }

  @Override
  public String getServiceName() {
    return "SQL Authentification";
  }

  @Override
  public String[] requestUserData(String username) throws AccountException, ConfigException {
    try {
      Optional<User> userOp = authManager.selectUserByUsername(username);
      User user = userOp.orElseThrow(() -> new AccountException(INVALID_CREDENTIALS));
      return new String[] { user.getFirst(), user.getLast(), user.getEmail() };
    } catch (SQLException e) {
      throw new ConfigException(INVALID_CONFIG, e);
    }
  }

}
