package de.uniwue.dw.query.model.index.tree;

import java.util.ArrayList;
import java.util.List;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.data.Information;

/**
 * A node represents all measurements of an attribute for one entity (eg. case/patient). It contains
 * a catalog entry an all information of the entity. Hierarchical information are stored
 * additionally.
 * 
 * @author e_dietrich_g
 *
 */
public class Node {

  private CatalogEntry catalogEntry;

  private Node parent;

  private List<Node> sons = new ArrayList<>();

  private List<Information> instances = new ArrayList<Information>();

  private List<Information> propagatedInfos = new ArrayList<Information>();

  /**
   * The name of the node.
   */
  private String leafsText;

  /**
   * The count of the leaf withe the most instances/information.
   */
  private int maxInstancesCountOfOneLeave = 0;

  /**
   * The number of leaves.
   */
  private int leafCount = 0;

  private Information first = null;

  private Information last = null;

  private Information min = null;

  private Information max = null;

  public Node(CatalogEntry catalogEntry) {
    this.catalogEntry = catalogEntry;
  }

  public CatalogEntry getCatalogEntry() {
    return catalogEntry;
  }

  public void setCatalogEntry(CatalogEntry catalogEntry) {
    this.catalogEntry = catalogEntry;
  }

  public Node getParent() {
    return parent;
  }

  public void setParent(Node parent) {
    this.parent = parent;
  }

  public List<Node> getSons() {
    return sons;
  }

  public void setSons(List<Node> sons) {
    this.sons = sons;
  }

  public List<Information> getInstances() {
    return instances;
  }

  public List<Information> getPropagatedInfos() {
    return propagatedInfos;
  }

  public void setInstances(List<Information> instances) {
    this.instances = instances;
  }

  public int getMaxInstancesCountOfOneLeave() {
    if (isLeaf())
      return instances.size();
    else
      return maxInstancesCountOfOneLeave;
  }

  public void setMaxInstancesCountOfOneLeave(int maxInstancesCountOfOneLeave) {
    this.maxInstancesCountOfOneLeave = maxInstancesCountOfOneLeave;
  }

  public int getLeafCount() {
    if (isLeaf())
      return 1;
    else
      return leafCount;
  }

  public void setLeafCount(int leafCount) {
    this.leafCount = leafCount;
  }

  public String getLeafsText() {
    return leafsText;
  }

  public void setLeafsText(String leafsText) {
    this.leafsText = leafsText;
  }

  public void addSon(Node son) {
    this.sons.add(son);
  }

  public void acceptVisitor(INodeVisitor visitor) {
    for (Node son : sons)
      son.acceptVisitor(visitor);
    visitor.visit(this);
  }

  public boolean isLeaf() {
    return sons.size() == 0;
  }

  public boolean isRoot() {
    return parent == null;
  }

  public void addInfo(Information info) {
    instances.add(info);
    updateExtremeValues(info);
  }

  private void updateExtremeValues(Information info) {
    if (first == null || info.getMeasureTime().before(first.getMeasureTime()))
      first = info;
    if (last == null || info.getMeasureTime().after(last.getMeasureTime()))
      last = info;
    if (catalogEntry.getDataType() == CatalogEntryType.Number) {
      if (min == null || info.getValueDec() < min.getValueDec())
        min = info;
      if (max == null || info.getValueDec() > max.getValueDec())
        max = info;
    }
  }

  public void addPropagatedInfo(Information info) {
    propagatedInfos.add(info);
  }

  public void printTree(int indention) {
    for (int i = 0; i <= indention; i++)
      System.out.print(" ");
    System.out.println(toString());
    for (Node n : sons)
      n.printTree(indention + 1);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(catalogEntry + " ");
    sb.append("[" + catalogEntry.getDataType() + "] ");
    sb.append("instances: " + instances.size() + " ");
    sb.append("sons: " + sons.size() + " ");
    sb.append("maxSonCount: " + maxInstancesCountOfOneLeave + " ");
    sb.append("leafCount: " + leafCount + " ");
    sb.append("leafstext: " + leafsText + " ");

    return sb.toString();
  }

  public Information getFirst() {
    return first;
  }

  public Information getLast() {
    return last;
  }

  public Information getMin() {
    return min;
  }

  public Information getMax() {
    return max;
  }
  
  
}
