package de.uniwue.dw.core.client.authentication.group;

import java.util.HashSet;
import java.util.Set;

import de.uniwue.dw.core.model.data.CatalogEntry;

public class Group {

  private static final int DEFAULT_K_ANONYMITY = 10;

  private int id;

  private String name;

  private int kAnonymity = DEFAULT_K_ANONYMITY;

  private boolean isAllowedToUseCaseQuery = false;

  private Set<CatalogEntry> catalogWhiteList = new HashSet<CatalogEntry>();

  private Set<CatalogEntry> catalogBlackList = new HashSet<CatalogEntry>();

  private Set<String> caseWhiteList = new HashSet<String>();

  private Set<String> caseBlackList = new HashSet<String>();

  private boolean admin;

  public Group(int id, String name, int kAnonymity, boolean caseQuery, boolean admin) {
    this.id = id;
    this.name = name;
    this.kAnonymity = kAnonymity;
    this.isAllowedToUseCaseQuery = caseQuery;
    this.admin = admin;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getkAnonymity() {
    return kAnonymity;
  }

  public void setkAnonymity(int kAnonymity) {
    this.kAnonymity = kAnonymity;
  }

  public boolean isAllowedToUseCaseQuery() {
    return isAllowedToUseCaseQuery;
  }

  public void setAllowedToUseCaseQuery(boolean isAllowedToUseCaseQuery) {
    this.isAllowedToUseCaseQuery = isAllowedToUseCaseQuery;
  }

  public Set<CatalogEntry> getCatalogWhiteList() {
    return catalogWhiteList;
  }

  public void setCatalogWhiteList(Set<CatalogEntry> catalogWhiteList) {
    this.catalogWhiteList = catalogWhiteList;
  }

  public Set<CatalogEntry> getCatalogBlackList() {
    return catalogBlackList;
  }

  public void setCatalogBlackList(Set<CatalogEntry> catalogBlackList) {
    this.catalogBlackList = catalogBlackList;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public boolean isAdmin() {
    return admin;
  }

  public void setAdmin(boolean admin) {
    this.admin = admin;
  }

  public boolean catalogWhiteListIsActive() {
    return !catalogBlackListIsActive();
  }

  public boolean catalogBlackListIsActive() {
    return catalogWhiteList.isEmpty();
  }

  public Set<String> getCaseWhiteList() {
    return caseWhiteList;
  }

  public void setCaseWhiteList(Set<String> caseWhiteList) {
    this.caseWhiteList = caseWhiteList;
  }

  public Set<String> getCaseBlackList() {
    return caseBlackList;
  }

  public void setCaseBlackList(Set<String> caseBlackList) {
    this.caseBlackList = caseBlackList;
  }

  public boolean caseWhiteListIsActive() {
    return !caseBlackListIsActive();
  }

  public boolean caseBlackListIsActive() {
    return caseWhiteList.isEmpty();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + id;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Group other = (Group) obj;
    if (id != other.id)
      return false;
    return true;
  }

  
  public String toString(){
    return name;
  }
}
