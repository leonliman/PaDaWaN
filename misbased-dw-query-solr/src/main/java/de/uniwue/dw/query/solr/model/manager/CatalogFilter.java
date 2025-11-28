package de.uniwue.dw.query.solr.model.manager;

import java.util.List;

import org.apache.solr.client.solrj.SolrQuery.ORDER;

import de.uniwue.dw.core.client.authentication.CountType;
import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.query.solr.suggest.CatalogIndexer;

public class CatalogFilter {

  public static final ORDER ASC = ORDER.asc;

  public static final ORDER DESC = ORDER.desc;

  private Integer attrid ;

  private String name;

  private String extID;

  private int parentID = -1;

  private CatalogEntryType dataType;

  private String project;

  private String uniqueName;

  private String description;

  private int countDocument = -1;

  private int countInfo = -1;

  private int countDocumentGroup = -1;

  private int countDocumentMin = -1;

  private int countInfoMin = -1;

  private int countDocumentGroupMin = -1;

  private String namePrefix;

  private User user;

//  private int limitResuls = Integer.MAX_VALUE;
  private int limitResuls = 1000000;

  private String orderByField;

  private ORDER orderByOrientation;

  public Integer getAttrid() {
    return attrid;
  }

  public CatalogFilter setAttrid(Integer attrid) {
    this.attrid = attrid;
    return this;
  }

  public String getName() {
    return name;
  }

  public CatalogFilter setName(String name) {
    this.name = name;
    return this;
  }

  public String getExtID() {
    return extID;
  }

  public CatalogFilter setExtID(String extID) {
    this.extID = extID;
    return this;
  }

  public int getParentID() {
    return parentID;
  }

  public CatalogFilter setParentID(int parentID) {
    this.parentID = parentID;
    return this;
  }

  public CatalogEntryType getDataType() {
    return dataType;
  }

  public CatalogFilter setDataType(CatalogEntryType dataType) {
    this.dataType = dataType;
    return this;
  }

  public String getProject() {
    return project;
  }

  public CatalogFilter setProject(String project) {
    this.project = project;
    return this;
  }

  public String getUniqueName() {
    return uniqueName;
  }

  public CatalogFilter setUniqueName(String uniqueName) {
    this.uniqueName = uniqueName;
    return this;
  }

  public String getDescription() {
    return description;
  }

  public CatalogFilter setDescription(String description) {
    this.description = description;
    return this;
  }

  public int getCountDocument() {
    return countDocument;
  }

  public CatalogFilter setCountDocument(int countDocument) {
    this.countDocument = countDocument;
    return this;
  }

  public int getCountInfo() {
    return countInfo;
  }

  public CatalogFilter setCountInfo(int countInfo) {
    this.countInfo = countInfo;
    return this;
  }

  public int getCountDocumentGroup() {
    return countDocumentGroup;
  }

  public CatalogFilter setCountDocumentGroup(int countDocumentGroup) {
    this.countDocumentGroup = countDocumentGroup;
    return this;
  }

  public int getLimitResults() {
    return limitResuls;
  }

  public CatalogFilter setLimitResuls(int limitResuls) {
    this.limitResuls = limitResuls;
    return this;
  }

  public String getOrderByField() {
    return orderByField;
  }

  public CatalogFilter setOrderByField(String oderByField) {
    this.orderByField = oderByField;
    return this;
  }

  public ORDER getOrderByOrientation() {
    return orderByOrientation;
  }

  public CatalogFilter setOrderByOrientation(ORDER orderByOrientation) {
    this.orderByOrientation = orderByOrientation;
    return this;
  }

  public String getUserAsValueString() {
    return CatalogIndexer.getUserGroupsAsValueString(user);
  }
  
  public List<String> getUserGroupsAsStringList() {
    return CatalogIndexer.getUserGroupsAsStringList(user);
  }

  public CatalogFilter setUser(User user) {
    this.user = user;
    return this;
  }

  public int getCountDocumentMin() {
    return countDocumentMin;
  }

  public CatalogFilter setCountDocumentMin(int countDocumentMin) {
    this.countDocumentMin = countDocumentMin;
    return this;
  }

  public int getCountInfoMin() {
    return countInfoMin;
  }

  public CatalogFilter setCountInfoMin(int countInfoMin) {
    this.countInfoMin = countInfoMin;
    return this;
  }

  public int getCountDocumentGroupMin() {
    return countDocumentGroupMin;
  }

  public CatalogFilter setCountDocumentGroupMin(int countDocumentGroupMin) {
    this.countDocumentGroupMin = countDocumentGroupMin;
    return this;
  }

  public String getNamePrefixAsValue() {
    if (namePrefix == null)
      return null;
    return "\"" + namePrefix + "*\"";
  }

  public CatalogFilter setNamePrefix(String namePrefix) {
    this.namePrefix = namePrefix;
    return this;
  }

  public CatalogFilter setMinOccurrence(CountType countType, int minOccurrence) {
    if (countType != null && minOccurrence > 0) {
      switch (countType) {
        case absolute:
          setCountInfoMin(minOccurrence);
          break;
        case distinctCaseID:
          setCountDocumentMin(minOccurrence);
          break;
        case distinctPID:
          setCountDocumentGroupMin(minOccurrence);
          break;
        default:
          break;
      }
    }
    return this;
  }

}
