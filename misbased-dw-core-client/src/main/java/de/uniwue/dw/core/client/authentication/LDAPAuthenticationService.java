package de.uniwue.dw.core.client.authentication;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.group.AuthManager;
import de.uniwue.dw.core.client.authentication.group.Group;
import de.uniwue.misc.util.ConfigException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.naming.NamingException;
import javax.security.auth.login.AccountException;
import java.io.IOException;
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class LDAPAuthenticationService extends AuthenticationService {

  private static Logger logger = LogManager.getLogger(LDAPAuthenticationService.class);

  private AuthManager authManager;

  public LDAPAuthenticationService() throws SQLException {
    authManager = DwClientConfiguration.getInstance().getAuthManager();
  }

  @Override
  public User authenticate(String username, String password)
          throws AccountException, ConfigException {
    if (password == null || password.equals("")) {
      throw new AccountException("Given passwort is empty");
    } else {
      LdapUtils ldapUtils = new LdapUtils("ldap://" + DwClientConfiguration.getLDAPServer());
      try {
        Optional<Set<String>> groupNamesOp = ldapUtils.authenticateUser(username, password);
        if (groupNamesOp.isPresent()) {
          Set<String> groupNames = groupNamesOp.get();
          logger.trace("Groups of " + username + ": " + String.join(", ", groupNames));

          String[] userData = ldapUtils.requestUserData(username);
          String firstName = userData[0];
          String lastName = userData[1];
          String email = userData[2];
          User user = new User(username, firstName, lastName, email);
          List<Group> sqlGroups = authManager.loadGroups(user);
          logger.trace("SqlGroups of " + username + ": "
                  + sqlGroups.stream().map(Group::getName).collect(Collectors.joining(", ")));
          List<Group> groups = authManager.getGroups(new ArrayList<>(groupNames));
          groups.addAll(sqlGroups);
          user.setGroups(groups);
          return user;
        } else {
          throw new AccountException(INVALID_CREDENTIALS);
        }
      } catch (NamingException | SQLException e) {
        e.printStackTrace();
        throw new ConfigException(INVALID_CONFIG, e);
      }
    }
  }

  @Override
  public boolean isOnline() {
    try {
      String ldapURL = DwClientConfiguration.getLDAPServer();
      if (ldapURL != null) {
        InetAddress address = InetAddress.getByName(ldapURL);
        if (address.isReachable(10000)) {
          return true;
        }
      }
    } catch (IOException e) {
      System.err.println("LDAP not reachable");
    }
    return false;
  }

  @Override
  public String getServiceName() {
    return "PaDaWaN LDAP Service";
  }

  @Override
  public String[] requestUserData(String username) throws AccountException, ConfigException {
    LdapUtils ldapUtils = new LdapUtils("ldap://" + DwClientConfiguration.getLDAPServer());
    return ldapUtils.requestUserData(username);
  }
}
