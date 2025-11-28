package de.uniwue.dw.query.solr.model.manager;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;

public class SolrCatalogEntry extends CatalogEntry {

  private List<String> userGroups;
  //
  // private static final SolrCatalogEntry root = createRoot();
  //
  // public static SolrCatalogEntry createRoot() {
  // CatalogEntry rootAsCE = CatalogEntry.createRoot();
  // return new SolrCatalogEntry(rootAsCE);
  // }

  public SolrCatalogEntry(int anAttrID, String aName, CatalogEntryType aDataType, String anExtID,
          int aParentID, double anOrderValue, String aProject, String aUniqueName,
          String aDescription, Timestamp aCreationTime) {
    super(anAttrID, aName, aDataType, anExtID, aParentID, anOrderValue, aProject, aUniqueName,
            aDescription, aCreationTime);
  }

  public SolrCatalogEntry(int anAttrID, String aName, CatalogEntryType aDataType, String anExtID,
          int aParentID, double anOrderValue, String aProject, String aUniqueName,
          String aDescription, Timestamp aCreationTime, int countDocument, int countDocumentGroup,
          int countInfo, List<String> userGroups) {
    this(anAttrID, aName, aDataType, anExtID, aParentID, anOrderValue, aProject, aUniqueName,
            aDescription, aCreationTime);
    setCountDistinctCaseID(countDocument);
    setCountDistinctPID(countDocumentGroup);
    setCountAbsolute(countInfo);
    setUserGroups(userGroups);
  }

  public SolrCatalogEntry(CatalogEntry e) {
    super(e.getAttrId(), e.getName(), e.getDataType(), e.getExtID(), e.getParentID(),
            e.getOrderValue(), e.getProject(), e.getUniqueName(), e.getDescription(),
            e.getCreationTime());
  }

  public SolrCatalogEntry(int anAttrID, String aName, CatalogEntryType aDataType, String anExtID,
          int aParentID, double anOrderValue, String aProject, String aUniqueName,
          String aDescription, Timestamp aCreationTime, int countDocument, int countDocumentGroup,
          int countInfo, List<String> userGroups, double lowBound, double highBound) {
    this(anAttrID, aName, aDataType, anExtID, aParentID, anOrderValue, aProject, aUniqueName,
            aDescription, aCreationTime, countDocument, countDocumentGroup, countInfo, userGroups);
    setLowBound(lowBound);
    setHighBound(highBound);
  }

  @Override
  public CatalogEntry getParent() {
    return SolrCatalogClientManager.getInst().getParent(this);
  }

  @Override
  public List<CatalogEntry> getChildren() {
    List<CatalogEntry> result = new ArrayList<>();
    List<CatalogEntry> myChilds = SolrCatalogClientManager.getInst().getChildsOf(this);
    result.addAll(myChilds);
    return result;
  }

  public List<String> getUserGroups() {
    return userGroups;
  }

  public SolrCatalogEntry setUserGroups(List<String> userGroups) {
    this.userGroups = userGroups;
    return this;
  }

  public List<CatalogEntry> getDescendants() {
    return SolrCatalogClientManager.getInst().getSiblings(this);
  }

  public boolean isRoot() {
    return SolrCatalogClientManager.getInst().isRoot(this);
    // return (root.getExtID().equals(this.getExtID()) &&
    // root.getProject().equals(this.getProject()));
  }

}
