package de.uniwue.dw.core.client.authentication;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.group.Group;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class User {

  private static final int DEFAULT_K_ANONYMITY = 0;

  private String username;

  private String first;

  private String last;

  private String email;

  @Deprecated
  private boolean admin;

  @Deprecated
  private boolean superuser;

  private AuthenticationService system;

  private UserSettings settings;

  private List<Group> groups = new ArrayList<>();

  @Deprecated
  public User(String username, String first, String last, String email, boolean superuser,
          boolean admin, AuthenticationService system) {
    this.username = username.toLowerCase();
    this.first = first;
    this.last = last;
    this.email = email;
    this.admin = admin;
    this.superuser = superuser;
    this.system = system;
  }

  public User(String username, String firstname, String lastname, String email) {
    this.username = username.toLowerCase();
    this.first = firstname;
    this.last = lastname;
    this.email = email;
  }

  public List<Group> getGroups() {
    return groups;
  }

  public void setGroups(List<Group> groups) {
    this.groups = groups;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getFirst() {
    return first;
  }

  public void setFirst(String first) {
    this.first = first;
  }

  public String getLast() {
    return last;
  }

  public void setLast(String last) {
    this.last = last;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public AuthenticationService getSystem() {
    return system;
  }

  public void setSystem(AuthenticationService system) {
    this.system = system;
  }

  public boolean isAdmin() {
    return groups.stream().anyMatch(Group::isAdmin);
  }

  @Deprecated
  public void setAdmin(boolean admin) {
    this.admin = admin;
  }

  @Deprecated
  public boolean isSuperuser() {
    return superuser;
  }

  @Deprecated
  public void setSuperuser(boolean superuser) {
    this.superuser = superuser;
  }

  @Override
  public String toString() {
    String[] infos = { "username: " + username, "first name: " + first, "last name: " + last, "email: " + email,
            "system: " + system };
    return Arrays.asList(infos).stream().collect(Collectors.joining(", "));
  }

  public User setSettings(UserSettings settings) {
    this.settings = settings;
    return this;
  }

  public UserSettings getSettings() {
    return settings;
  }

  public boolean isAllowedToUseCaseQuery() {
    return groups.stream().anyMatch(Group::isAllowedToUseCaseQuery);
  }

  public int getKAnonymity() {
    Integer min = groups.stream().map(Group::getkAnonymity).min(Integer::compare)
            .orElse(getDefaultKAnonymity());
    return min;
  }

  private Integer getDefaultKAnonymity() {
    int kAnonymity = DEFAULT_K_ANONYMITY;
    try {
      kAnonymity = DwClientConfiguration.getInstance().getDefaultKAnonymity();
    } catch (NumberFormatException e) {
      // ignore. parameter is not set
    }
    return kAnonymity;
  }

}
