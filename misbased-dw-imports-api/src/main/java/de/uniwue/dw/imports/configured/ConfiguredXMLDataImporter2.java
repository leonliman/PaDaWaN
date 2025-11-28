package de.uniwue.dw.imports.configured;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
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
import de.uniwue.dw.imports.DBDataElemIterator;
import de.uniwue.dw.imports.IDataElem;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.ImporterFullFile;
import de.uniwue.dw.imports.configured.data.ConfigDataXML;
import de.uniwue.dw.imports.data.DocInfo;
import de.uniwue.dw.imports.manager.ImporterManager;

public class ConfiguredXMLDataImporter2 extends ImporterFullFile {

  private ConfigDataXML config;

  public ConfiguredXMLDataImporter2(ImporterManager anImportManager, ConfigDataXML aXMLConfig) throws ImportException {
    super(anImportManager, aXMLConfig.getParentEntry(), aXMLConfig.getProject(), aXMLConfig.getDataSource());
    config = aXMLConfig;
    if (config.docRegex != null) {
      docIDRegex = Pattern.compile(config.docRegex);
    }
    getCatalogImporter().useAbstractNameForRootProject = false;
  }

  protected DocInfo getDocInfo(IDataElem file) throws ImportException {
    DBDataElemIterator source = (DBDataElemIterator) file;
    String docIDString = source.getItem("Dokumentnummer");
    Long docID = Long.valueOf(docIDString);
    DocInfo docInfo = getDocManager().getDoc(docID);
    return docInfo;
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
      Node aNode = nodeList.item(0);
      nodeList = aNode.getChildNodes(); // the root node is "content"
      aNode = nodeList.item(0);
      nodeList = aNode.getChildNodes(); // the next node is "master"
      CatalogEntry parentEntry = getDomainRoot();
      for (int i = 0; i < nodeList.getLength(); i++) {
        Node aSubNode = nodeList.item(i);
        readDataNode(aSubNode, docInfo, parentEntry);
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
          throw new ImportException(ImportExceptionType.DATA_PARSING_ERROR, " error " + e.getMessage());
        }
      }
    }
    return true;
  }

  private Set<String> validTags = new HashSet<String>(Arrays.asList(new String[] { "section", "element" }));

  private void readDataNode(Node aNode, DocInfo docInfo, CatalogEntry parent) throws ImportException {
    String nodeName = aNode.getNodeName();
    if (!validTags.contains(nodeName)) {
      return;
    }
    String extID = aNode.getAttributes().getNamedItem("alias").getNodeValue().trim();
    if (aNode.getAttributes().getNamedItem("title") == null) {
      return;
    }
    String entryName = aNode.getAttributes().getNamedItem("title").getNodeValue().trim();
    CatalogEntryType type = CatalogEntryType.Text;
    if (nodeName.equals("section")) {
      type = CatalogEntryType.Structure;
      extID += "_section";
    }
    CatalogEntry anEntry = getOrCreateEntry(entryName, type, extID, parent);
    NodeList nodeList = aNode.getChildNodes();
    if ((nodeList.getLength() == 1) && nodeList.item(0).getNodeName().equals("#text")) {
      String textContent = aNode.getTextContent();
      if (!textContent.isEmpty()) {
        try {
          insert(anEntry, docInfo.PID, textContent, docInfo.creationTime, docInfo.caseID, docInfo.docID, 0);
        } catch (SQLException e) {
          throw new ImportException(ImportExceptionType.SQL_ERROR, e);
        }
      }
    } else {
      for (int i = 0; i < nodeList.getLength(); i++) {
        Node aSubNode = nodeList.item(i);
        readDataNode(aSubNode, docInfo, anEntry);
      }
    }
  }
}
