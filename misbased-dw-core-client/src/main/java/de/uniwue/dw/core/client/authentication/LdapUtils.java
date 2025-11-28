package de.uniwue.dw.core.client.authentication;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.misc.util.ConfigException;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import javax.naming.ldap.LdapContext;
import javax.security.auth.login.AccountException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class provides utils for ldap login. Users can reach two states: authenticated and
 * authorized.
 */
public class LdapUtils {

  private static final Pattern groupNamePattern = Pattern.compile("CN=(\\w+)");

  private final Hashtable<String, Object> env = new Hashtable<String, Object>();

  private String url;

  private DirContext mainCtx = null;

  /**
   * try to login at configured ldap server
   */
  public LdapUtils(String anUrl) {
    try {
      url = anUrl;
      // Set up the environment for creating the initial context
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      env.put(Context.PROVIDER_URL, url);
      env.put(Context.SECURITY_PROTOCOL, "ssl");
      env.put(Context.SECURITY_AUTHENTICATION, "simple");
      String ldaPou = DwClientConfiguration.getLDAPou();
      String ldaPdc = DwClientConfiguration.getLDAPdc();
      if (ldaPou == null) {
        System.out.println("No ldapou parameter given. LDAP access does not work !");
        return;
      }
      if (ldaPdc == null) {
        System.out.println("No ldapdc parameter given. LDAP access does not work !");
        return;
      }
      String dn = "cn=ldap_public, ou=" + ldaPou + "," + ldaPdc;
      env.put(Context.SECURITY_PRINCIPAL, dn);
      String ldapPublicPW = "ldpu";
      env.put(Context.SECURITY_CREDENTIALS, ldapPublicPW);
      mainCtx = new InitialDirContext(env);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static Optional<String> extractGroupName(String input) {
    Matcher matcher = groupNamePattern.matcher(input);
    if (matcher.find()) {
      String group = matcher.group(1);
      return Optional.of(group);
    }
    System.out.println("false");
    return Optional.empty();
  }

  public Optional<String> getDistinguishedName(String username) throws NamingException {
    SearchControls ctrl = new SearchControls();
    ctrl.setSearchScope(SearchControls.SUBTREE_SCOPE);
    String ldaPou = DwClientConfiguration.getLDAPou();
    String ldaPdc = DwClientConfiguration.getLDAPdc();
    if (ldaPou == null) {
      throw new NamingException("No ldapou parameter given. LDAP access does not work!");
    }
    if (ldaPdc == null) {
      throw new NamingException("No ldapou parameter given. LDAP access does not work!");
    }
    NamingEnumeration<SearchResult> enumeration = mainCtx.search("ou=" + ldaPou + "," + ldaPdc,
            "(&(objectClass=user)(cn=" + username + "))", ctrl);
    if (enumeration.hasMore()) {
      List<String> groups = new ArrayList<>();
      SearchResult next = enumeration.next();
      Attributes attributes = next.getAttributes();
      Attribute dnAttr = attributes.get("distinguishedName");
      String dnUser = (String) dnAttr.get();
      return Optional.of(dnUser);
    }
    return Optional.empty();
  }

  /**
   * @param username
   * @param pw
   * @return Optional<Set < String>> with group names. If the authentication succeeds, a list with
   * the group names of the user will be returned. Otherwise the Optional will be empty.
   * @throws NamingException
   */
  public Optional<Set<String>> authenticateUser(String username, String pw)
          throws NamingException {
    SearchControls ctrl = new SearchControls();
    ctrl.setSearchScope(SearchControls.SUBTREE_SCOPE);
    String ldaPou = DwClientConfiguration.getLDAPou();
    String ldaPdc = DwClientConfiguration.getLDAPdc();
    if (ldaPou == null) {
      throw new NamingException("No ldapou parameter given. LDAP access does not work!");
    }
    if (ldaPdc == null) {
      throw new NamingException("No ldapou parameter given. LDAP access does not work!");
    }
    NamingEnumeration<SearchResult> enumeration = mainCtx.search("ou=" + ldaPou + "," + ldaPdc,
            "(&(objectClass=user)(cn=" + username + "))", ctrl);
    if (enumeration.hasMore()) {
      Set<String> groups = new HashSet<>();
      SearchResult next = enumeration.next();
      Attributes attributes = next.getAttributes();
      Attribute dnAttr = attributes.get("distinguishedName");
      String dnUser = (String) dnAttr.get();
      Hashtable<String, String> env1 = new Hashtable<String, String>();
      env1.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      env1.put(Context.PROVIDER_URL, url);
      env1.put(Context.SECURITY_PROTOCOL, "ssl");
      env.put(LdapContext.CONTROL_FACTORIES, "com.sun.jndi.ldap.ControlFactory");
      env1.put(Context.SECURITY_AUTHENTICATION, "simple");
      env1.put(Context.SECURITY_PRINCIPAL, dnUser);
      env1.put(Context.SECURITY_CREDENTIALS, pw);
      new InitialDirContext(env1);
      // if we reach this point we are authenticated !
      // checking for different user rights per group membership
      Attribute memberOf = attributes.get("memberOf");
      NamingEnumeration<?> all = memberOf.getAll();
      while (all.hasMore()) {
        String value = (String) all.next();
        Optional<String> groupName = extractGroupName(value);
        groupName.ifPresent(groups::add);
      }

      try {
        Set<String> queriedGroups = getAllGroups(dnUser);
        groups.addAll(queriedGroups);
      } catch (Exception e) {
        e.printStackTrace();
      }

      try {
        Attribute primaryGroupID = attributes.get("primarygroupid");
        NamingEnumeration<?> allPrimaryGroupIDs = primaryGroupID.getAll();
        boolean isDomainUser = false;
        while (allPrimaryGroupIDs.hasMore()) {
          String value = (String) allPrimaryGroupIDs.next();
          if (value.equals("513")) { // 513 is the default LDAP group id for the "Domain Users" group
            isDomainUser = true;
            break;
          }
        }

        if (isDomainUser) {
          String domainUsersDistinguishedName = "CN=Domain Users,CN=Users," + DwClientConfiguration.getLDAPdc();
          Set<String> queriedGroups = getAllGroups(domainUsersDistinguishedName);
          groups.addAll(queriedGroups);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }

      return Optional.of(groups);
    }
    return Optional.empty();

  }

  public Set<String> getAllGroups(String distinguishedName) throws ConfigException {
    Set<String> result = new HashSet<>();
    SearchControls ctrl = new SearchControls();
    ctrl.setSearchScope(SearchControls.SUBTREE_SCOPE);

    try {
      String ldaPou = DwClientConfiguration.getLDAPou();
      String ldaPdc = DwClientConfiguration.getLDAPdc();
      String groupOu = DwClientConfiguration.getLDAPGroupOU();
      if (ldaPou == null || ldaPdc == null) {
        throw new ConfigException("No ldapou parameter given. LDAP access does not work!");
      } else {
        String name = groupOu + "," + ldaPdc;
        String matchingAttributes = "(" + "&" + "(objectCategory=group)"
                + "(member" + ":1.2.840.113556.1.4.1941:" + "=" + distinguishedName + ")" + ")";
        NamingEnumeration<SearchResult> answer = mainCtx.search(name, matchingAttributes, ctrl);
        int i = 0;
        while (answer.hasMore()) {
          SearchResult rslt = answer.next();
          Attributes attrs = rslt.getAttributes();
          String groupName = (String) attrs.get("cn").get();
          result.add(groupName);
        }

      }
    } catch (NamingException e) {
      e.printStackTrace();
      throw new ConfigException("Incorrect config for Ldap", e);
    }
    return result;
  }

  public void searchGroup(String groupname) throws ConfigException {
    SearchControls ctrl = new SearchControls();
    ctrl.setSearchScope(SearchControls.SUBTREE_SCOPE);

    try {
      String ldaPou = DwClientConfiguration.getLDAPou();
      String ldaPdc = DwClientConfiguration.getLDAPdc();
      if (ldaPou == null || ldaPdc == null) {
        throw new ConfigException("No ldapou parameter given. LDAP access does not work!");
      } else {
        String name = "ou=groups"
                + "," + ldaPdc;
        String matchingAttributes = "(&(objectClass=*)" + "(cn=" + groupname + "))";

        System.out.println(name);
        System.out.println(matchingAttributes);
        NamingEnumeration<SearchResult> answer = mainCtx.search(name, matchingAttributes, ctrl);

        int i = 0;
        while (answer.hasMore()) {
          System.out.print(++i + " ");
          SearchResult rslt = answer.next();
          Attributes attrs = rslt.getAttributes();
          System.out.println(attrs.get("cn").get());
        }

      }
    } catch (NamingException e) {
      e.printStackTrace();
      throw new ConfigException("Incorrect config for Ldap", e);
    }
  }

  private void printAllValus(Attributes attrs) throws NamingException {
    NamingEnumeration<? extends Attribute> enumeration = attrs.getAll();
    while (enumeration.hasMore()) {
      Attribute attribute = enumeration.next();
      System.out.println(attribute);
    }
  }

  /**
   * @param username
   * @return a String[3] with {first name, last name, email} for the given username
   * @throws AccountException
   * @throws ConfigException
   */
  public String[] requestUserData(String username) throws AccountException, ConfigException {
    SearchControls ctrl = new SearchControls();
    ctrl.setSearchScope(SearchControls.SUBTREE_SCOPE);

    try {
      String ldaPou = DwClientConfiguration.getLDAPou();
      String ldaPdc = DwClientConfiguration.getLDAPdc();
      if (ldaPou == null || ldaPdc == null) {
        throw new ConfigException("No ldapou parameter given. LDAP access does not work!");
      } else {
        NamingEnumeration<SearchResult> enumeration = mainCtx.search("ou=" + ldaPou + "," + ldaPdc,
                "(&(objectClass=user)(cn=" + username + "))", ctrl);
        if (enumeration.hasMore()) {
          SearchResult next = enumeration.next();
          Attributes attributes = next.getAttributes();
          String firstName = attributes.get("givenName").get(0).toString();
          String lastName = attributes.get("sn").get(0).toString();
          String email = attributes.get("mail").get(0).toString();
          return new String[] { firstName, lastName, email };
        } else {
          throw new AccountException("username not known");
        }
      }
    } catch (NamingException e) {
      throw new ConfigException("Incorrect config for Ldap", e);
    }
  }

}
