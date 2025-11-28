package de.uniwue.dw.imports.configured.xml;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.model.util.DataTypeGuesser;
import de.uniwue.dw.imports.configured.data.ConfigDataXML;

public class XMLNode {

  public XMLData data;

  public XMLNode parent;

  public String name = "";

  public String id = "";

  public int listID = 0;

  public String listDiscriminator = "";

  public CatalogEntryType dataType;

  public boolean keep = false;
  
  public String extIDOverride;
  
  public String nodeAttrIbuteOverride;
  
  public Map<String, Integer> values = new LinkedHashMap<String, Integer>();

  public Map<String, XMLNode> children = new LinkedHashMap<String, XMLNode>();

  public XMLNode(XMLNode aParent, String anID, String aName) {
    this(aParent.data, anID, aName);
    parent = aParent;
    parent.children.put(anID, this);
  }

  public XMLNode(XMLData aData, String anID, String aName) {
    data = aData;
    id = anID;
    name = aName;
  }

  public boolean isListWithDiscriminator() {
    for (XMLNode aBrotherNode : parent.children.values()) {
      if (!aBrotherNode.listDiscriminator.isEmpty()) {
        return true;
      }
    }
    return false;
  }
  
  public void write(StringBuilder builder) {
    int depth = getDepth();
    writeDepth(builder, depth);
    writeID(builder, 0);
    writeKeep(builder);
    writeListID(builder);
    writeListDiscr(builder);
    writeName(builder, name);
    writeComments(builder);
    writeTypes(builder);
    writeExamples(builder);
    writeCount(builder);
    builder.append("\t"); // extIDOverride
    builder.append("\t"); // attributeNameOverride
    builder.append("\n");
    for (XMLNode aChild : children.values()) {
      aChild.write(builder);
    }
  }

  private int getDepth() {
    if (parent == null) {
      return 0;
    } else {
      return parent.getDepth() + 1;
    }
  }

  private void writeID(StringBuilder builder, int depth) {
    String idWithIndent = "";
    for (int i = 0; i < depth; i++) {
      idWithIndent += "  ";
    }
    idWithIndent += id;
    builder.append(idWithIndent + "\t");
  }

  private void writeDepth(StringBuilder builder, int depth) {
    builder.append(depth + "\t");
  }

  private void writeKeep(StringBuilder builder) {
    if (listID == 0) {
      builder.append("x");
    }
    builder.append("\t");
  }

  private void writeName(StringBuilder builder, String name) {
    builder.append(name + "\t");
  }

  private void writeTypes(StringBuilder builder) {
    List<String> valueList = getValues();
    dataType = guessType(valueList);
    if (dataType == CatalogEntryType.Number) {
      builder.append("x");
    }
    builder.append("\t");
    // unit
    builder.append("\t");
    if (dataType == CatalogEntryType.Bool) {
      builder.append("x");
    }
    builder.append("\t");
    if (dataType == CatalogEntryType.SingleChoice) {
      builder.append("x");
    }
    builder.append("\t");
    if (dataType == CatalogEntryType.DateTime) {
      builder.append("x");
    }
    builder.append("\t");
    if (dataType == CatalogEntryType.Structure) {
      builder.append("x");
    }
    builder.append("\t");
    if (dataType == CatalogEntryType.Text) {
      builder.append("x");
    }
    builder.append("\t");
  }

  private List<String> getValues() {
    List<String> valueList = new ArrayList<String>();
    for (String aValue : values.keySet()) {
      int amount = values.get(aValue);
      for (int i = 0; i < amount; i++) {
        String trimmedValue = aValue.replaceAll("\\s", " ");
        trimmedValue = trimmedValue.trim();
        valueList.add(trimmedValue);
      }
    }
    return valueList;
  }

  private void writeCount(StringBuilder builder) {
    int count = 0;
    for (Integer aCount : values.values()) {
      count += aCount;
    }
    builder.append(count);
  }
  
  private CatalogEntryType guessType(List<String> valueList) {
    DataTypeGuesser guesser = new DataTypeGuesser();
    CatalogEntryType guessedType = guesser.guessAndUpdateDataTypes(valueList);
    return guessedType;
  }

  private void writeExamples(StringBuilder builder) {
    int x = 0;
    for (String aValue : values.keySet()) {
      if (x > 5) {
        continue;
      }
      x++;
      String example = aValue.replaceAll("\\s", " ");
      builder.append(example + ";");
    }
    builder.append("\t");
  }

  private void writeListID(StringBuilder builder) {
    if (listID != 0) {
      builder.append(listID);
    }
    builder.append("\t");
  }

  private void writeListDiscr(StringBuilder builder) {
    if (!listDiscriminator.isEmpty()) {
      builder.append(listDiscriminator);
    }
    builder.append("\t");
  }

  private void writeComments(StringBuilder builder) {
    builder.append("\t");
  }

  public void createCatalog(CatalogManager catalogManager, ConfigDataXML xmlConfig,
          CatalogEntry parent) throws SQLException {
    if (listID != 0) {
      CatalogEntry myEntry = catalogManager.getOrCreateEntry(name, dataType, id, parent,
              xmlConfig.getProject());
      for (XMLNode aNode : children.values()) {
        aNode.createCatalog(catalogManager, xmlConfig, myEntry);
      }
    }
  }

  public XMLNode getNode(String nodeID) {
    if (id.equals(nodeID)) {
      return this;
    }
    if (children.containsKey(nodeID)) {
      return children.get(nodeID);
    }
    for (XMLNode aChild : children.values()) {
      XMLNode result = aChild.getNode(nodeID);
      if (result != null) {
        return result;
      }
    }
    return null;
  }

}
