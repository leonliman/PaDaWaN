package de.uniwue.dw.imports.configured;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.imports.IDataElem;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.ImporterFullFile;
import de.uniwue.dw.imports.configured.data.ConfigAcceptedID;
import de.uniwue.dw.imports.configured.data.ConfigDataXML;
import de.uniwue.dw.imports.configured.xml.XMLData;
import de.uniwue.dw.imports.configured.xml.XMLNode;
import de.uniwue.dw.imports.data.DocInfo;
import de.uniwue.dw.imports.manager.ImporterManager;

public class ConfiguredXMLDataImporter extends ImporterFullFile {

  private ConfigDataXML config;

  public XMLData supportingCatalog;

  private Map<String, Integer> extIDToCountMap = new HashMap<String, Integer>();

  public ConfiguredXMLDataImporter(ImporterManager anImportManager, ConfigDataXML aXMLConfig)
          throws ImportException {
    super(anImportManager, aXMLConfig.getParentEntry(), aXMLConfig.getProject(),
            aXMLConfig.getDataSource());
    config = aXMLConfig;
    if (config.docRegex != null) {
      docIDRegex = Pattern.compile(config.docRegex);
    }
    if (config.caseRegex != null) {
      caseIDRegex = Pattern.compile(config.caseRegex);
    }
    getCatalogImporter().useAbstractNameForRootProject = false;
  }

  @Override
  protected boolean processImportInfoFile(IDataElem aFile, DocInfo docInfo) throws ImportException {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db;
    NodeList nodeList;
    Document doc;
    InputStreamReader in = null;
    InputSource input = null;

    if (!isFirstFactDataImport()) {
      delete(docInfo.docID);
    }
    try {
      db = dbf.newDocumentBuilder();
      in = aFile.getInputStreamReader();
      input = new InputSource(in);
      doc = db.parse(input);
      nodeList = doc.getChildNodes();
      for (int i = 0; i < nodeList.getLength(); i++) {
        Node aNode = nodeList.item(i);
        boolean createPath = false;
        if (supportingCatalog != null) {
          // wenn ein externer Katalog per CSV angehängt ist müssen alle Knoten des kompletten
          // Dokuments durchgegenagen werden
          createPath = true;
        }
        CatalogEntry parentEntry = getDomainRoot();
        readDataNode(aNode, docInfo, parentEntry, createPath);
      }
    } catch (ParserConfigurationException e) {
      throw new ImportException(ImportExceptionType.DATA_PARSING_ERROR, " error " + e.getMessage());
    } catch (SAXException e) {
      throw new ImportException(ImportExceptionType.DATA_PARSING_ERROR, " error " + e.getMessage());
    } catch (FileNotFoundException e) {
      throw new ImportException(ImportExceptionType.DATA_PARSING_ERROR, " error " + e.getMessage());
    } catch (UnsupportedEncodingException e) {
      throw new ImportException(ImportExceptionType.DATA_PARSING_ERROR, " error " + e.getMessage());
    } catch (IOException e) {
      throw new ImportException(ImportExceptionType.IO_ERROR, " error " + e.getMessage());
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          throw new ImportException(ImportExceptionType.DATA_PARSING_ERROR,
                  " error " + e.getMessage());
        }
      }
    }
    return true;
  }

  private void importInnerText(Node aSubNode, String nodeName, CatalogEntry parent, DocInfo docInfo)
          throws ImportException {
    String textContent = aSubNode.getTextContent();
    CatalogEntry anEntry = getOrCreateEntry(nodeName, CatalogEntryType.Text, nodeName, parent);
    try {
      insert(anEntry, docInfo.PID, textContent, docInfo.creationTime, docInfo.caseID,
              docInfo.docID);
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, "", getProject(), e);
    }
  }

  private CatalogEntry getListEntry(Node anXMLNode, CatalogEntry parent, XMLNode aNode)
          throws ImportException {
    // der Knoten trägt Daten und braucht zur Identifikation seines Katalogeintrags die
    // Values seiner Bruderknoten
    CatalogEntry anEntry;
    NodeList brotherNodeList = anXMLNode.getParentNode().getChildNodes();
    String nodeName = "";
    String nodeExtID = "";
    String parentName = "";
    String parentExtID = "";
    String unit = "";
    for (int i = 0; i < brotherNodeList.getLength(); i++) {
      Node aBrotherXMLNode = brotherNodeList.item(i);
      String brotherNodeName = aBrotherXMLNode.getNodeName();
      XMLNode brotherNode = aNode.parent.children.get(brotherNodeName);
      if (brotherNode.listDiscriminator.toLowerCase().equals("name")) {
        String value = getValue(aBrotherXMLNode);
        if (!nodeName.isEmpty()) {
          nodeName += " ";
        }
        nodeName += value;
      } else if (brotherNode.listDiscriminator.toLowerCase().equals("extid")) {
        String value = getValue(aBrotherXMLNode);
        if (!brotherNode.extIDOverride.isEmpty()) {
          parentExtID = brotherNode.extIDOverride.replace("[EXTID]", value);
        }
        if (!nodeExtID.isEmpty()) {
          nodeExtID += " ";
        }
        nodeExtID += value;
      } else if (brotherNode.listDiscriminator.toLowerCase().equals("unit")) {
        String value = getValue(aBrotherXMLNode);
        if (!unit.isEmpty()) {
          unit += " ";
        }
        unit += value;
      }
    }
    if (nodeName.isEmpty()) {
      nodeName = nodeExtID;
    }
    if (parentName.isEmpty()) {
      parentName = nodeName;
    }
    if (parentExtID.isEmpty()) {
      parentExtID = nodeExtID;
    }
    CatalogEntry listParentEntry = getOrCreateEntry(parentName, CatalogEntryType.Structure,
            parentExtID, parent);
    nodeName += " " + aNode.name;
    if (aNode.extIDOverride.isEmpty()) {
      nodeExtID += " " + aNode.id;
    } else {
      nodeExtID = aNode.extIDOverride.replace("[EXTID]", nodeExtID);
    }
    anEntry = getOrCreateEntry(nodeName, CatalogEntryType.Bool, nodeExtID, listParentEntry);
    if (!unit.isEmpty()) {
      try {
        getCatalogManager().insertNumericMetaData(anEntry, unit, 0, 0);
      } catch (SQLException e) {
        throw new ImportException(ImportExceptionType.SQL_ERROR, "", getProject(), e);
      }
    }
    return anEntry;
  }

  private CatalogEntry getEntry(Node anXMLNode, CatalogEntry parent) throws ImportException {
    CatalogEntry anEntry = null;
    String nodeName = anXMLNode.getNodeName();
    if (supportingCatalog != null) {
      XMLNode aNode = supportingCatalog.getNode(nodeName);
      if (aNode != null) {
        if (!aNode.keep) {
          return null;
        }
        if ((aNode.listID == 0) || !aNode.isListWithDiscriminator()) {
          // der Knoten ist ein regulärer Knoten
          anEntry = getOrCreateEntry(aNode.name, aNode.dataType, nodeName, parent);
        } else {
          // der Knoten ist Teil einer Liste
          if (aNode.listDiscriminator.isEmpty()) {
            anEntry = getListEntry(anXMLNode, parent, aNode);
          } else {
            // der Knoten trägt selber keine Daten sondern dient zur Identifikation des
            // Katalogeintrags von anderen Knoten
            return null;
          }
        }
      } else {
        // der Knoten ist manuell gelöscht worden
      }
    } else {
      String text = anXMLNode.getAttributes().getNamedItem("TEXT").getNodeValue().trim();
      anEntry = getOrCreateEntry(text, CatalogEntryType.Text, nodeName, parent);
    }
    return anEntry;
  }

  private String getValue(Node aNode) {
    String value = "";
    String attributeName = "VALUE";
    XMLNode aSupportNode = supportingCatalog.getNode(aNode.getNodeName());
    if (!aSupportNode.nodeAttrIbuteOverride.isEmpty()) {
      attributeName = aSupportNode.nodeAttrIbuteOverride;
    }
    if (attributeName.equals("#TEXT")) {
      value = aNode.getTextContent();
    } else {
      Node namedItem = aNode.getAttributes().getNamedItem(attributeName);
      if (namedItem != null) {
        value = namedItem.getNodeValue().trim();
      }
    }
    return value;
  }

  private void readDataNode(Node aNode, DocInfo docInfo, CatalogEntry parent, boolean createPath)
          throws ImportException {
    NodeList nodeList = aNode.getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node aSubNode = nodeList.item(i);
      CatalogEntry newParent = parent;
      String nodeName = aSubNode.getNodeName();
      if (config.acceptedExtID.containsKey(nodeName) || createPath) {
        ConfigAcceptedID extID = config.acceptedExtID.get(nodeName);
        XMLNode aSupportNode = null;
        if (supportingCatalog != null) {
          aSupportNode = supportingCatalog.getNode(nodeName);
        }
        if (((aSubNode.getAttributes() != null)
                && (aSubNode.getAttributes().getNamedItem("TEXT") != null))
                || ((aSupportNode != null) && (!aSupportNode.nodeAttrIbuteOverride.isEmpty()))) {
          CatalogEntry anEntry = null;
          // entweder will ich den node selbst oder ich brauche ihn wegen seiner child nodes
          if (createPath || !extID.isRoot) {
            String value = getValue(aSubNode);
            anEntry = getEntry(aSubNode, parent);
            String extid = anEntry.getExtID();
            if (anEntry != null) {
              if ((value != null) && !value.isEmpty()) {
                try {
                  if (!extIDToCountMap.containsKey(extid)) {
                    extIDToCountMap.put(extid, 0);
                  } else {
                    extIDToCountMap.put(extid, extIDToCountMap.get(extid) + 1);
                  }              
                  long groupID = extIDToCountMap.get(extid);
                  insert(anEntry, docInfo.PID, value, docInfo.creationTime, docInfo.caseID,
                          docInfo.docID, groupID);
                } catch (SQLException e) {
                  throw new ImportException(ImportExceptionType.SQL_ERROR, "", getProject(), e);
                }
              }
              newParent = anEntry;
            }
          }
        }
        if ((extID != null) && extID.isRoot) {
          // entweder ist man bereits auf einem path oder der aktuelle Knoten könnte eine Root
          // sein
          createPath = true;
        }
        if ((extID != null) && extID.importText) {
          importInnerText(aSubNode, nodeName, parent, docInfo);
        }
      }
      if (aSubNode.hasChildNodes()) {
        readDataNode(aSubNode, docInfo, newParent, createPath);
      }
    }
  }
}
