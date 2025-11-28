package de.uniwue.dw.query.model.index.tree;

import de.uniwue.dw.core.model.data.Information;

/**
 * The PropagatorVisitor visits a tree an propagates hierarchical information from the leafs to all
 * ancestors.
 * 
 * @author e_dietrich_g
 *
 */
public class PropagatorVisitor implements INodeVisitor {

  public void popagateHierachyInformation(Node n) {
    visit(n);
  }

  @Override
  public void visit(Node n) {
    propagateTexts(n);
    propagateNumbers(n);
  }

  private void propagateNumbers(Node n) {
    int maxInstancesCount = 0;
    int leafCount = 0;
    for (Node son : n.getSons()) {
      if (maxInstancesCount < son.getMaxInstancesCountOfOneLeave())
        maxInstancesCount = son.getMaxInstancesCountOfOneLeave();
      leafCount += son.getLeafCount();
    }
    n.setLeafCount(leafCount);
    n.setMaxInstancesCountOfOneLeave(maxInstancesCount);
  }

  private void propagateTexts(Node n) {
    if (n.isLeaf()) {
      n.setLeafsText(n.getCatalogEntry().getName());
    } else {
      boolean first = true;
      StringBuilder leafsText = new StringBuilder();
      for (Node son : n.getSons()) {
        if (first) {
          first = false;
        } else {
          leafsText.append(", ");
        }
        leafsText.append(son.getLeafsText());
        for (Information anInfo : son.getInstances()) {
          Information newInfo = new Information(anInfo.getInfoID(), son.getCatalogEntry().getAttrId(), anInfo.getPid(),
                  anInfo.getCaseID(), anInfo.getMeasureTime(), anInfo.getImportTime(), anInfo.getUpdateTime(), son.getCatalogEntry().getName(),
                  son.getCatalogEntry().getName(), null, anInfo.getRef(), anInfo.getDocID(), anInfo.getGroupID());
          n.addPropagatedInfo(newInfo);
        }
      }
      n.setLeafsText(leafsText.toString());
    }
  }

}
