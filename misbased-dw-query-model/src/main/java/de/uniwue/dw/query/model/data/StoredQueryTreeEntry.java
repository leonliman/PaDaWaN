package de.uniwue.dw.query.model.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.*;

@JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class, property = "id")
@JsonIgnoreProperties({ "id", "query", "parent", "structure" })
public class StoredQueryTreeEntry {

  private RawQuery query;

  private String label;

  private StoredQueryTreeEntry parent;

  private List<StoredQueryTreeEntry> childs = new ArrayList<>();

  private StoredQueryTreeEntry() {
  }

  public StoredQueryTreeEntry(RawQuery query, StoredQueryTreeEntry parent) {
    this.query = query;
    setAndLinkParent(parent);
  }

  public StoredQueryTreeEntry(String displayName, StoredQueryTreeEntry parent) {
    this.label = displayName;
    setAndLinkParent(parent);
  }

  private void setAndLinkParent(StoredQueryTreeEntry parent) {
    this.parent = parent;
    if (parent != null)
      parent.addChild(this);
  }

  public boolean isStructure() {
    return query == null;
  }

  public String getLabel() {
    if (label != null) {
      return label;
    } else {
      String[] containersAndQueryLeaf = query.getName().split("/");
      String displayName = containersAndQueryLeaf[containersAndQueryLeaf.length - 1];
      return displayName;
    }
  }

  public StoredQueryTreeEntry setLabel(String name) {
    this.label = name;
    return this;
  }

  public RawQuery getQuery() {
    return query;
  }

  public StoredQueryTreeEntry setQuery(RawQuery query) {
    this.query = query;
    return this;
  }

  public StoredQueryTreeEntry getParent() {
    return parent;
  }

  public StoredQueryTreeEntry setParent(StoredQueryTreeEntry parent) {
    this.parent = parent;
    return this;
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<StoredQueryTreeEntry> getChilds() {
    return childs;
  }

  public StoredQueryTreeEntry setChilds(List<StoredQueryTreeEntry> childs) {
    this.childs = childs;
    return this;
  }

  public boolean hasChilds() {
    return !this.childs.isEmpty();
  }

  public void addChild(StoredQueryTreeEntry queryTreeEntry) {
    this.childs.add(queryTreeEntry);
  }

  public String getPath() {
    if (query != null)
      return query.getName();
    else
      return Optional.ofNullable(parent).filter(n -> n.parent != null).map(n -> n.getPath() + "/")
              .orElse("") + label;
    // return parent == null ? label : parent.getPath() + "/" + label;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("label: " + label);
    sb.append(" structure: " + isStructure());
    sb.append(" path: " + getPath());
    return sb.toString();
  }

  public StoredQueryTreeEntry copy() {
    return new StoredQueryTreeEntry().setChilds(childs).setLabel(label).setParent(parent)
            .setQuery(query);
  }

  public static void main(String[] args) {
    StoredQueryTreeEntry a = new StoredQueryTreeEntry("root", null);
    StoredQueryTreeEntry b = new StoredQueryTreeEntry("b", a);
    StoredQueryTreeEntry c = new StoredQueryTreeEntry("c", b);
    System.out.println(a.getPath());
    System.out.println(b.getPath());
    System.out.println(c.getPath());
  }

}
