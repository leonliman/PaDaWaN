package de.uniwue.dw.imports;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.configured.data.ConfigCatalogEntry;
import de.uniwue.dw.imports.configured.data.ConfigDataSource;
import de.uniwue.dw.imports.data.DocInfo;
import de.uniwue.dw.imports.manager.ImporterManager;

public class ImporterGenericPMD extends ImporterFullFile {

  private String rootDataNodeName;

  private List<String> whiteList = new ArrayList<String>();

  private List<String> blackList = new ArrayList<String>();

  private Map<String, Integer> extIDToCountMap;

  public ImporterGenericPMD(ImporterManager anImporterManager, ConfigCatalogEntry aParentEntry,
          String projectName, ConfigDataSource aDataSource, String aRootDataNodeName)
          throws ImportException {
    super(anImporterManager, aParentEntry, projectName, aDataSource);
    rootDataNodeName = aRootDataNodeName;
  }

  /**
   * zero arguments constructor needed for plugin functionality
   */
  public ImporterGenericPMD() {
  }

  protected void addToWhitelist(String nodename) {
    whiteList.add(nodename);
  }

  protected void addToBlacklist(String nodename) {
    blackList.add(nodename);
  }

  @Override
  protected boolean processImportInfoFile(IDataElem aFile, DocInfo docInfo) throws ImportException {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db;
    NodeList nodeList;
    Document doc;
    InputStream inStream = null;
    InputStreamReader in = null;
    InputSource input = null;

    if (!isFirstFactDataImport()) {
      delete(docInfo.docID);
    }
    extIDToCountMap = new HashMap<>();
    try {
      db = dbf.newDocumentBuilder();
      in = aFile.getInputStreamReader();
      input = new InputSource(in);
      doc = db.parse(input);
      nodeList = doc.getChildNodes();
      for (int i = 0; i < nodeList.getLength(); i++) {
        Node aNode = nodeList.item(i);
        if (aNode.getNodeName().equalsIgnoreCase(rootDataNodeName)) {
          // here comes the node which carries the actual data for the DW. Everything surrounding is
          // XML-meta-data
          readMainNode(aNode, docInfo);
        } else {
          throw new ImportException(ImportExceptionType.XML_ELEMENT_NOT_FOUND,
                  "no main xml element found");
        }
      }
    } catch (ParserConfigurationException e) {
      throw new ImportException(ImportExceptionType.DATA_PARSING_ERROR, " error " + e.getMessage());
    } catch (SAXException e) {
      throw new ImportException(ImportExceptionType.DATA_PARSING_ERROR, " error " + e.getMessage());
    } catch (IOException e) {
    } finally {
      if (inStream != null) {
        try {
          inStream.close();
        } catch (IOException e) {
          throw new ImportException(ImportExceptionType.DATA_PARSING_ERROR,
                  " error " + e.getMessage());
        }
      }
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

  private void readMainNode(Node aNode, DocInfo docInfo) throws ImportException {
    NodeList nodeList = aNode.getChildNodes();

    for (int i = 0; i < nodeList.getLength(); i++) {
      Node aSubNode = nodeList.item(i);
      if (aSubNode.getNodeName().equals("Befunddaten")) {
        readBefundDatenNode(aSubNode, docInfo, getDomainRoot());
      }
    }
  }

  private void readBefundDatenNode(Node aNode, DocInfo docInfo, CatalogEntry parent)
          throws ImportException {
    NodeList nodeList = aNode.getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node aSubNode = nodeList.item(i);
      if (aSubNode.getNodeName().matches("Eintrag")) {
        readBefundDatenNode(aSubNode, docInfo, parent);
      } else if (!aSubNode.getNodeName().equals("#text")) {
        // ist das beste adhoc-kriterium was ich gefunden habe
        String text = aSubNode.getAttributes().getNamedItem("TEXT").getNodeValue().trim();
        String nodeName = aSubNode.getNodeName();
        if (blackList.contains(nodeName)
                || (whiteList.size() > 0 && !whiteList.contains(nodeName))) {
          continue;
        }
        String value = "";
        if (aSubNode.getAttributes().getNamedItem("VALUE") != null) {
          value = aSubNode.getAttributes().getNamedItem("VALUE").getNodeValue().trim();
        }
        try {
          boolean valExists = (value != null) && !value.isEmpty();
          if (text.trim().length() <= 0) {
            if (valExists)
              text = nodeName.replace("_", " ");
            else
              continue;
          }

          CatalogEntry anEntry = getOrCreateEntry(text, CatalogEntryType.Text, nodeName, parent);

          if (valExists) {
            if (!extIDToCountMap.containsKey(nodeName)) {
              extIDToCountMap.put(nodeName, 0);
            } else {
              extIDToCountMap.put(nodeName, extIDToCountMap.get(nodeName) + 1);
            }
            insert(anEntry, docInfo.PID, value, docInfo.creationTime, docInfo.caseID, docInfo.docID,
                    extIDToCountMap.get(nodeName));
          }
          if (aSubNode.hasChildNodes()) {
            readBefundDatenNode(aSubNode, docInfo, anEntry);
          }

        } catch (SQLException e) {
          throw new ImportException(ImportExceptionType.SQL_ERROR, e);
        }
      }
    }
  }
}
