package de.uniwue.dw.imports.configured.xml;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.io.FileUtils;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.imports.configured.data.ConfigDataXML;
import de.uniwue.misc.util.StringUtilsUniWue;

public class XMLData {

  public static String rootName = "#root";

  public XMLNode root;

  public int maxListID = 0;

  public XMLData() {
    root = new XMLNode(this, rootName, rootName);
  }

  public void write(File aFile) throws IOException {
    StringBuilder builder = new StringBuilder();
    String header = StringUtilsUniWue.concat(
            new String[] { "depth", "ID", "keep", "listID", "listDiscr", "name", "comments",
                "numType", "unit", "boolType", "choiceType", "dateType", "structureType",
                "textType", "examples", "count", "extIDOverride", "attributeNameOverride" },
            "\t");
    builder.append(header);
    builder.append("\n");
    root.write(builder);
    FileUtils.writeStringToFile(aFile, builder.toString(), "UTF-8");
  }

  public void createCatalog(CatalogManager catalogManager, ConfigDataXML xmlConfig,
          CatalogEntry domainRoot) throws SQLException {
    for (XMLNode aNode : root.children.values()) {
      aNode.createCatalog(catalogManager, xmlConfig, domainRoot);
    }
  }

  public XMLNode getNode(String nodeID) {
    XMLNode node = root.getNode(nodeID);
    return node;
  }

}
