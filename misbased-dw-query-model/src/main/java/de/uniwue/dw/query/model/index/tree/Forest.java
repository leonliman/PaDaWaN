package de.uniwue.dw.query.model.index.tree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Facet over a catalog and its entries.
 * 
 * Essentially, in contrast to the data entries (CatalogEntry, Information) it allows to use the
 * visitor pattern.
 */
public class Forest {

  private List<Node> roots = new ArrayList<Node>();

  private HashMap<Integer, Node> attrID2Node = new HashMap<Integer, Node>();

  public List<Node> getRoots() {
    return roots;
  }

  public HashMap<Integer, Node> getAttrID2Node() {
    return attrID2Node;
  }

  public Node getNode(int attrID) {
    return attrID2Node.get(attrID);
  }

  public void addRoot(Node root) {
    roots.add(root);
  }

  public void addNode2HM(Node node) {
    attrID2Node.put(node.getCatalogEntry().getAttrId(), node);
  }

  public boolean contains(int attrID) {
    return attrID2Node.containsKey(attrID);
  }

  public Node get(int attrID) {
    return attrID2Node.get(attrID);
  }

  public void printForest() {
    System.out.println("<Forest>");
    for (Node root : roots) {
      root.printTree(0);
      System.out.println("---");
    }
    System.out.println("</Forest>");
  }

  public void printAllTrees() {
    System.out.println("<Trees>");
    for (Node n : attrID2Node.values())
      System.out.println(n);
    System.out.println("</Trees>");
  }

}
