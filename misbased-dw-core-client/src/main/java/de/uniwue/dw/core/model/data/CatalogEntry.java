package de.uniwue.dw.core.model.data;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import de.uniwue.dw.core.client.authentication.CountType;

/**
 * The CatalogEntry is the data class for the DataWarehouse catalog. It represents a node in a tree
 * structure (linked by the "parent"-member)
 */
public class CatalogEntry implements Comparable<CatalogEntry> {

  /**
   * The attrId is the internal ID of the node. The IDs are given by the database
   */
  private int attrId;

  /**
   * The parentID is the attrID of the node's parent. It is set when the node is created by the
   * SQLAdapter. It is used to connect the node to its parent node's instance during
   * "finalizeReading"
   */
  private int parentID;

  // The speaking name of the catalog entry
  private String name;

  // The dataType of the catalog entry
  private CatalogEntryType dataType;

  // The extID is a name of this node in an external catalog.
  private String extID;

  // The name of the domain or project to which this node belongs to
  private String project;

  // A number which is used to order all children of the parent in the GUI
  private double orderValue;

  // The object instance of the parent node of this node. It is set during
  // "finalizeReading". The parent is identified by the parentID
  private CatalogEntry parent;

  // A name which is unique compared to all other catalog entries. It is either derived by the
  // catalogManager or given during import of the catalog by the particular importer. It it noted
  // that the uniqueness is not enforced by the database so that this member should not be used by
  // model functions but only for display purposes !
  private String uniqueName;

  // A description of the catalog entry
  private String description;

  // The list of child nodes of this node
  private List<CatalogEntry> children = new ArrayList<CatalogEntry>();

  // The Set of child nodes of this node. This Set exists for performance reasons and is only used
  // internally
  private Set<CatalogEntry> childSet = new HashSet<CatalogEntry>();

  // If this node has the data type "number" those are the values extreme values given by an
  // external catalogue (not to be mistaken with the maximum minimum values in the database)
  private double lowBound, highBound;

  // If this node has the data type "number" this is the unit of those numbers
  private String unit;

  // The absolute amount of information instances for this entry in the database
  private long countAbsolute;

  // The amount of information instances for this entry in the database distincted on PIDs
  private long countDistinctPID;

  // The amount of information instances for this entry in the database distincted on Cases
  private long countDistinctCaseID;

  // The timestamp when this catalog entry was created
  private Timestamp creationTime;

  // The format with which the facts are exported or shown to users
  // TODO: implement everything for this member !!!
  private String format;

  // If the data type of this entry is of type SingleChoice this member contains all different
  // choices that the values of the entry can be
  private Set<String> singleChoiceChoice = new HashSet<String>();

  // This member is for i2b2 concepts to indicate if the entry is a modifier. If not null it is,
  // else it is a standard concept
  public ModifierConfig modifierConfig;

  private CatalogEntry() {
  }

  /**
   * catalog entries should not be created manually, see getOrCreate... of catalog manager
   * 
   * @param anAttrID
   *          {@link CatalogEntry#attrId}
   * @param aName
   * @param aDataType
   * @param anExtID
   * @param aParentID
   *          {@link CatalogEntry#parentID}
   * @param anOrderValue
   * @param aProject
   */
  public CatalogEntry(int anAttrID, String aName, CatalogEntryType aDataType, String anExtID, int aParentID,
          double anOrderValue, String aProject, String aUniqueName, String aDescription, Timestamp aCreationTime) {
    name = aName;
    dataType = aDataType;
    setAttrId(anAttrID);
    extID = anExtID;
    orderValue = anOrderValue;
    parentID = aParentID;
    project = aProject;
    uniqueName = aUniqueName;
    description = aDescription;
    creationTime = aCreationTime;
  }

  public static CatalogEntry createRoot() {
    int id = 0;
    String name = "root";
    String extId = "";
    String project = "";
    int aParentID = -1;
    String aDescription = "root";
    String aUniqueName = "root";
    int oderValue = 0;
    CatalogEntry root = new CatalogEntry(id, name, CatalogEntryType.Structure, extId, aParentID, oderValue, project,
            aUniqueName, aDescription, new Timestamp(0));
    return root;
  }

  public boolean isModifier() {
    return modifierConfig != null;
  }

  public boolean isRoot() {
    return parent == null;
  }

  public String getProject() {
    return project;
  }

  @Override
  public String toString() {
    return name + "_" + getAttrId();
  }

  public String toStringVerbose() {
    Object[] values = { name, attrId, extID, project, getUniqueName(), countAbsolute, countDistinctCaseID,
        countDistinctPID };
    String text = Arrays.stream(values).filter(n -> n != null).map(Object::toString).collect(Collectors.joining(" "));
    return text;
  }

  public String toStringTree() {
    return toStringTree(0, false);
  }

  public String toStringTreeVerbose() {
    return toStringTree(0, true);
  }

  private String toStringTree(int indention, boolean verbose) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i <= indention; i++)
      sb.append(" ");
    if (verbose)
      sb.append(toStringVerbose() + "\n");
    else
      sb.append(toString());
    for (CatalogEntry child : getChildren())
      sb.append(child.toStringTree(indention + 1, verbose));
    return sb.toString();
  }

  public List<CatalogEntry> getChildren() {
    return children;
  }

  /*
   * Returns the last entry in the list of children of this entry
   */
  public CatalogEntry getLastChild() {
    if (getChildren().size() > 0) {
      return children.get(children.size() - 1);
    } else {
      return null;
    }
  }

  // returns all descendants of the entry, not including the entry itself
  public List<CatalogEntry> getDescendants() {
    List<CatalogEntry> result = new ArrayList<CatalogEntry>();
    result.addAll(children);
    for (CatalogEntry aChild : children) {
      result.addAll(aChild.getDescendants());
    }
    return result;
  }

  /*
   * Adds a new child to the list of children of this entry
   */
  public void addChild(CatalogEntry aChild) {
    // this does not call "addChild(int index, CatalogEntry aChild)" because the new position in the
    // child list is not relevant
    if (!childSet.contains(aChild)) {
      if ((aChild.getParent() != null) && (aChild.getParent() != this)) {
        aChild.getParent().children.remove(aChild);
        aChild.getParent().childSet.remove(aChild);
      }
      aChild.setParent(this);
      children.add(aChild);
      childSet.add(aChild);
    }
  }

  /*
   * Removes the given child from the list of children of this entry
   */
  public void removeChild(CatalogEntry aChild) {
    aChild.setParent(null);
    children.remove(aChild);
    childSet.remove(aChild);
  }

  /*
   * Adds a child at the given position to the list of children of this entry. If the child already
   * exists in the list it is repositioned. If the child formerly was the child of another entry it
   * is removed from his list.
   */
  public void addChild(int index, CatalogEntry aChild) {
    if (childSet.contains(aChild)) {
      children.remove(aChild);
      index--;
    } else if (aChild.getParent() != null) {
      aChild.getParent().children.remove(aChild);
      aChild.getParent().childSet.remove(aChild);
    }
    aChild.setParent(this);
    children.add(index, aChild);
    childSet.add(aChild);
  }

  /*
   * Returns the child with the given name if existing or null otherwise
   */
  public CatalogEntry getChildByName(String aName) {
    for (CatalogEntry aChild : children) {
      if (aChild.name.equals(aName)) {
        return aChild;
      }
    }
    return null;
  }

  /*
   * Returns the child with the given extID if existing or null otherwise
   */
  public CatalogEntry getChildByExtID(String anExtID) {
    for (CatalogEntry aChild : children) {
      if (aChild.extID.equals(anExtID)) {
        return aChild;
      }
    }
    return null;
  }

  /*
   * Returns the child with the given internal ID if existing or null otherwise
   */
  public CatalogEntry getChildByID(int anID) {
    for (CatalogEntry aChild : children) {
      if (aChild.getAttrId() == anID) {
        return aChild;
      }
    }
    return null;
  }

  /*
   * Returns the names of the entry and all its ancestors concatenated by "|"
   */
  public String getPath() {
    StringBuilder builder = new StringBuilder();
    getPath(builder);
    return builder.toString();
  }

  private void getPath(StringBuilder builder) {
    builder.insert(0, name.replaceAll("\\|", ":"));
    if (getParent().getParent() != null) {
      builder.insert(0, "|");
      getParent().getPath(builder);
    }
  }

  /*
   * Sorts the children of this entry ordered by their member "orderValue"
   */
  public void sortChildren() {
    Collections.sort(children);
  }

  public CatalogEntry getParent() {
    return parent;
  }

  private void getAncestors(List<CatalogEntry> result) {
    if (parent != null) {
      result.add(parent);
      parent.getAncestors(result);
    }
  }

  /**
   * Returns all ancestors of this entry including the root
   */
  public List<CatalogEntry> getAncestors() {
    List<CatalogEntry> result = new ArrayList<CatalogEntry>();
    getAncestors(result);
    return result;
  }

  /**
   * Returns all descendants of this entry
   */
  public List<CatalogEntry> getAllChildren() {
    List<CatalogEntry> result = new ArrayList<CatalogEntry>();
    getAllChildren(result);
    return result;
  }

  private void getAllChildren(List<CatalogEntry> result) {
    List<CatalogEntry> childs = getChildren();
    if (childs != null && !childs.isEmpty()) {
      result.addAll(childs);
      childs.forEach(n -> n.getAllChildren(result));
    }
  }

  /*
   * This method is for internal use only! Use parent.addChild(this) instead! Sets the entries
   * parent and also the parentID. Beware that this method does not add the entry to the new parents
   * children!
   */
  public CatalogEntry setParent(CatalogEntry parent) {
    this.parent = parent;
    if (parent != null) {
      parentID = parent.getAttrId();
    } else {
      int x = 0;
    }
    return this;
  }

  /*
   * Compares the entry to another entry based on their member "orderValue"
   */
  @Override
  public int compareTo(CatalogEntry arg0) {
    return (((Double) orderValue).compareTo(arg0.orderValue));
  }

  public int getAttrId() {
    return attrId;
  }

  public CatalogEntry setAttrId(int attrId) {
    this.attrId = attrId;
    return this;
  }

  public int getParentID() {
    return parentID;
  }

  public CatalogEntry setParentID(int parentID) {
    this.parentID = parentID;
    return this;
  }

  public String getName() {
    return name;
  }

  public CatalogEntry setName(String name) {
    this.name = name;
    return this;
  }

  public CatalogEntryType getDataType() {
    return dataType;
  }

  public CatalogEntry setDataType(CatalogEntryType dataType) {
    this.dataType = dataType;
    return this;
  }

  public String getExtID() {
    return extID;
  }

  public CatalogEntry setExtID(String extID) {
    this.extID = extID;
    return this;
  }

  public String getDescription() {
    return description;
  }

  public CatalogEntry setDescrption(String aDescription) {
    description = aDescription;
    return this;
  }

  public String getUniqueName() {
    return uniqueName;
  }

  public CatalogEntry setUniqueName(String aUniqueName) {
    uniqueName = aUniqueName;
    return this;
  }

  public double getOrderValue() {
    return orderValue;
  }

  public CatalogEntry setOrderValue(double orderValue) {
    this.orderValue = orderValue;
    return this;
  }

  public String getUnit() {
    return unit;
  }

  public void setUnit(String aUnit) {
    this.unit = aUnit;
  }

  public double getLowBound() {
    return lowBound;
  }

  public CatalogEntry setLowBound(double lowBound) {
    this.lowBound = lowBound;
    return this;
  }

  public double getHighBound() {
    return highBound;
  }

  public CatalogEntry setHighBound(double highBound) {
    this.highBound = highBound;
    return this;
  }

  public long getCountAbsolute() {
    return countAbsolute;
  }

  public CatalogEntry setCountAbsolute(long countAbsolute) {
    this.countAbsolute = countAbsolute;
    return this;
  }

  public long getCountDistinctPID() {
    return countDistinctPID;
  }

  public CatalogEntry setCountDistinctPID(long countDistinctPID) {
    this.countDistinctPID = countDistinctPID;
    return this;
  }

  public long getCountDistinctCaseID() {
    return countDistinctCaseID;
  }

  public CatalogEntry setCountDistinctCaseID(long countDistinctCaseID) {
    this.countDistinctCaseID = countDistinctCaseID;
    return this;
  }

  public CatalogEntry setProject(String project) {
    this.project = project;
    return this;
  }

  public CatalogEntry setChildren(List<CatalogEntry> children) {
    this.children = children;
    for (CatalogEntry anEntry : children) {
      childSet.add(anEntry);
    }
    return this;
  }

  public Timestamp getCreationTime() {
    return creationTime;
  }

  public CatalogEntry setCreationTime(Timestamp creationTime) {
    this.creationTime = creationTime;
    return this;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + attrId;
    result = prime * result + ((dataType == null) ? 0 : dataType.hashCode());
    result = prime * result + ((extID == null) ? 0 : extID.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    long temp;
    temp = Double.doubleToLongBits(orderValue);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    result = prime * result + ((project == null) ? 0 : project.hashCode());
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
    CatalogEntry other = (CatalogEntry) obj;
    if (attrId != other.attrId)
      return false;
    if (dataType != other.dataType)
      return false;
    if (extID == null) {
      if (other.extID != null)
        return false;
    } else if (!extID.equals(other.extID))
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    if (Double.doubleToLongBits(orderValue) != Double.doubleToLongBits(other.orderValue))
      return false;
    if (project == null) {
      if (other.project != null)
        return false;
    } else if (!project.equals(other.project))
      return false;
    return true;
  }

  /*
   * Creates a deep copy of this entry.
   */
  public CatalogEntry deepCopy() {
    //@formatter:off
    CatalogEntry result = new CatalogEntry()
            .setAttrId(attrId)
            .setCountAbsolute(countAbsolute)
            .setCountDistinctCaseID(countDistinctCaseID)
            .setCountDistinctPID(countDistinctPID)
            .setDataType(dataType).setExtID(extID)
            .setHighBound(highBound)
            .setLowBound(lowBound)
            .setName(name)
            .setOrderValue(orderValue)
            .setParent(parent)
            .setParentID(parentID)
            .setProject(project);
    //@formatter:on
    for (CatalogEntry aChild : children.toArray(new CatalogEntry[0])) {
      CatalogEntry newChild = aChild.deepCopy();
      result.addChild(newChild);
    }
    return result;
  }

  /*
   * Creates a shallow copy of this entry. Beware that the copies children do not have the new copy
   * as their parent!
   */
  public CatalogEntry copyWihtoutReferences() {
    //@formatter:off
    return new CatalogEntry()
            .setAttrId(attrId)
            //.setChildren(children)
            .setCountAbsolute(countAbsolute)
            .setCountDistinctCaseID(countDistinctCaseID)
            .setCountDistinctPID(countDistinctPID)
            .setDataType(dataType).setExtID(extID)
            .setHighBound(highBound)
            .setLowBound(lowBound)
            .setName(name)
            .setOrderValue(orderValue)
            //.setParent(parent)
            .setParentID(parentID)
            .setProject(project)
            .setCreationTime(creationTime)
            .setDescrption(description)
            .setUniqueName(uniqueName);
    //@formatter:on
  }

  public String toStringID() {
    Object[] content = { attrId, countAbsolute, countDistinctCaseID, countDistinctPID, creationTime, dataType,
        description, extID, highBound, lowBound, name, orderValue, parentID, project, uniqueName };
    String delimiter = "||";

    return Arrays.asList(content).stream().map(n -> n == null ? "" : n.toString())
            .collect(Collectors.joining(delimiter));
  }

  public Set<String> getSingleChoiceChoice() {
    return singleChoiceChoice;
  }

  public void addSingleChoiceChoice(String aChoice) {
    this.singleChoiceChoice.add(aChoice);
  }

  public long getCount(CountType countType) {
    switch (countType) {
      case absolute:
        return countAbsolute;
      case distinctCaseID:
        return countDistinctCaseID;
      case distinctPID:
        return countDistinctPID;

      default:
        return countDistinctCaseID;
    }
  }

}
