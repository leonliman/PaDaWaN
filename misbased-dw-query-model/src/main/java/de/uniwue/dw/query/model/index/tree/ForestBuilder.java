package de.uniwue.dw.query.model.index.tree;

import java.sql.SQLException;
import java.util.List;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.core.model.manager.CatalogManager;

/**
 * This class is used to build a tree for given information. The tree is build by the hierarchical
 * information from the catalog entries. The root node is not included in the tree, so that many
 * trees will be generated. SingelChoise-nodes will be transformed to a structure node with the
 * choices as its sons. The new created son catalog entries will be stored in the db.
 * 
 * @author e_dietrich_g
 *
 */
public class ForestBuilder {

  private CatalogManager catalogManager;

  private Forest forest = new Forest();

  public ForestBuilder(CatalogManager catalogManager) throws SQLException {
    this.catalogManager = catalogManager;
  }

  public void insertInformation(Information info) throws SQLException {
    info = transformPossibleSingleChoice(info);
    if (info == null) {
      return;
    }
    plantTreeInForestByOneLeaf(info.getAttrID(), null);
    Node node = forest.get(info.getAttrID());
    if (node != null) {
      node.addInfo(info);
    }
  }

  private void plantTreeInForestByOneLeaf(int lefAttrID, Node son) throws SQLException {
    Node node = forest.get(lefAttrID);
    if (node == null) {
      CatalogEntry catalogEntry = catalogManager.getEntryByID(lefAttrID);
      if (catalogEntry != null) {
        node = new Node(catalogEntry);
        forest.addNode2HM(node);
        CatalogEntry parent = node.getCatalogEntry().getParent();
        if (parent != null) {
          int parentAttrid = parent.getAttrId();
          plantTreeInForestByOneLeaf(parentAttrid, node);
        } else
          forest.addRoot(node);
      }
    }
    if (son != null) {
      node.addSon(son);
      son.setParent(node);
    }
  }

  public void insertInformation(List<Information> infos) throws SQLException {
    for (Information info : infos) {
      insertInformation(info);
    }
  }

  public Forest getForest() {
    return forest;
  }

  private Information transformPossibleSingleChoice(Information i) throws SQLException {
    CatalogEntry catalogEntry = catalogManager.getEntryByID(i.getAttrID());
    if ((catalogEntry != null) && (catalogEntry.getDataType() == CatalogEntryType.SingleChoice)) {
      String newName = catalogEntry.getName() + "=" + i.getValue();
      String extID = catalogManager.cleanExtID(newName);
      CatalogEntry transformedCatalogEntry = catalogManager.getEntryByRefID(extID,
              catalogEntry.getProject(), false);
      if (transformedCatalogEntry != null) {
        Information newInfo = new Information(i.getInfoID(), transformedCatalogEntry.getAttrId(),
                i.getPid(), i.getCaseID(), i.getMeasureTime(), i.getImportTime(),  i.getUpdateTime(), "x", "x", null,
                i.getRef(), i.getDocID(), i.getGroupID());
        return newInfo;
      } else {
        return null;
      }
    }
    return i;
  }

}
