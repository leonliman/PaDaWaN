package de.uniwue.dw.imports.configured.xml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.misc.util.FileUtilsUniWue;

public class XMLAnalyzer {

  public static void main(String[] args) throws IOException {
    XMLAnalyzer analyzer = new XMLAnalyzer();
    XMLConfig config = new XMLConfig();
    config.encoding = "Windows-1252";
    config.idAttributeName = XMLAnalyzer.nodeNameAttribute;
    config.nameAttributeName = "TEXT";
    config.valueAttributeName = "VALUE";
    String rootDir = "\\\\wflw227\\D$\\Import\\KIS\\PMDsGenerisch";
    String outputDir = "C:\\tmp\\productiveConfigs";
    for (File aFile : new File(rootDir).listFiles()) {
      File inputDir = new File(rootDir, aFile.getName());
      XMLData data = analyzer.analyze(inputDir, config);
      File outputFile = new File(outputDir, aFile.getName() + "_IDs.txt");
      data.write(outputFile);
    }
    // File inputDir = new File(
    // "C:\\Code\\DW\\misbased-dw\\misbased-dw-core\\misbased-dw-tests\\src\\main\\resources\\TestDaten\\Herzkatheter\\Dokumente");
    // XMLData data = analyzer.analyze(inputDir, config);
    // File outputFile = new File("C:\\tmp\\Herzkatheter_IDs.txt");
    // data.write(outputFile);
  }

  public static String innerTextName = "#innerText";

  public static String nodeNameAttribute = "#nodeName";

  public static String textNodeName = "#text";

  private Set<String> existingIDs = new HashSet<String>();

  public XMLData analyze(File dir, XMLConfig config) {
    XMLData data = new XMLData();
    File[] files = dir.listFiles();
    System.out.println("Files to analyze: " + files.length);
    int x = 0;
    if (files != null) {
      for (File aFile : files) {
        analyzeFile(aFile, data, config);
        x++;
        if (x % 1000 == 0) {
           System.out.println("analyzed " + x + " files.");
        }
        if (x > 10000) {
          break;
        }
      }
    }
    return data;
  }

  public XMLData read(File aFile) throws IOException {
    XMLData result = new XMLData();
    String text = FileUtilsUniWue.file2String(aFile, "UTF-8");
    String[] split = text.split("\n");
    XMLNode lastNode = result.root;
    int lastDepth = 0;
    int lineIndex = 0;
    for (String aLine : split) {
      lineIndex++;
      if (lineIndex <= 1) {
        continue;
      }
      String[] tokens = aLine.trim().split("\t", -1);
      int depth = Integer.parseInt(tokens[0]);
      String take = tokens[2];
      String id = tokens[1].trim();
      if (id.equals(XMLData.rootName)) {
        lastNode = result.root;
        continue;
      }
      String listIDString = tokens[3];
      int listID = 0;
      if (!listIDString.isEmpty()) {
        listID = Integer.valueOf(listIDString);
      }
      String listDiscriminator = tokens[4];
      String name = tokens[5];
      String dataTypeNum = tokens[7];
      String dataTypeBool = tokens[9];
      String dataTypeChoice = tokens[10];
      String dataTypeDateTime = tokens[11];
      String dataTypeStructure = tokens[12];
      String dataTypeText = tokens[13];
      String extIDOverride = "";
      if (tokens.length > 16) {
        extIDOverride = tokens[16];
      }
      String nodeAttrIbuteOverride = "";
      if (tokens.length > 17) {
        nodeAttrIbuteOverride = tokens[17];
      }
      XMLNode currentParent;
      if (lastDepth < depth) {
        currentParent = lastNode;
      } else if (lastDepth == depth) {
        currentParent = lastNode.parent;
      } else {
        currentParent = lastNode.parent;
        for (int i = depth; i < lastDepth; i++) {
          currentParent = currentParent.parent;
        }
      }
      lastNode = new XMLNode(currentParent, id, name);
      lastNode.listDiscriminator = listDiscriminator;
      lastNode.listID = listID;
      if (take.equals("x")) {
        lastNode.keep = true;
      }
      if (dataTypeNum.equals("x")) {
        lastNode.dataType = CatalogEntryType.Number;
      }
      if (dataTypeBool.equals("x")) {
        lastNode.dataType = CatalogEntryType.Bool;
      }
      if (dataTypeChoice.equals("x")) {
        lastNode.dataType = CatalogEntryType.SingleChoice;
      }
      if (dataTypeText.equals("x")) {
        lastNode.dataType = CatalogEntryType.Text;
      }
      if (dataTypeDateTime.equals("x")) {
        lastNode.dataType = CatalogEntryType.DateTime;
      }
      if (dataTypeStructure.equals("x")) {
        lastNode.dataType = CatalogEntryType.Structure;
      }
      lastNode.extIDOverride = extIDOverride;
      lastNode.nodeAttrIbuteOverride = nodeAttrIbuteOverride;
      lastDepth = depth;
    }
    return result;
  }

  private void analyzeNode(String aFileName, Node anXMLNode, XMLNode aDataNode, XMLConfig config) {
    String value = getNodeAttribute(anXMLNode, config.valueAttributeName);
    if (value != null) {
      if (aDataNode.values.containsKey(value)) {
        Integer integer = aDataNode.values.get(value);
        aDataNode.values.put(value, ++integer);
      } else {
        aDataNode.values.put(value, 1);
      }
    }
    NodeList nodeList = anXMLNode.getChildNodes();
    boolean runningList = false;
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node aNode = nodeList.item(i);
      if (aNode.getNodeName().equals(textNodeName)) {
        continue;
      }
      String nodeID = getNodeAttribute(aNode, config.idAttributeName);
      String nodeName = nodeID;
      if (config.nameAttributeName != null) {
        nodeName = getNodeAttribute(aNode, config.nameAttributeName);
        if (nodeName == null) {
          nodeName = nodeID;
        }
      }
      XMLNode childNode;
      if (aDataNode.children.containsKey(nodeID)) {
        childNode = aDataNode.children.get(nodeID);
      } else {
        childNode = new XMLNode(aDataNode, nodeID, nodeName);
      }
      if (existingIDs.contains(nodeID)) {
        if (!runningList) {
          aDataNode.data.maxListID++;
        }
        runningList = true;
        if (childNode.listID == 0) {
          childNode.listID = aDataNode.data.maxListID;
        }
      } else {
        runningList = false;
      }
      existingIDs.add(nodeID);
      analyzeNode(aFileName, aNode, childNode, config);
    }
  }

  private String getNodeAttribute(Node anXMLNode, String attributeName) {
    String result = null;
    if (attributeName.equals(nodeNameAttribute)) {
      result = anXMLNode.getNodeName();
    } else {
      if (anXMLNode.getAttributes() != null) {
        Node attributeNode = anXMLNode.getAttributes().getNamedItem(attributeName);
        if (attributeNode != null) {
          String attributeValue = attributeNode.getNodeValue();
          if ((attributeValue != null) && !attributeValue.isEmpty()) {
            result = attributeValue;
          }
        }
      }
    }
    return result;
  }

  private void analyzeFile(File aFile, XMLData data, XMLConfig config) {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db;
    NodeList nodeList;
    Document doc;
    InputStream inStream = null;
    InputStreamReader in = null;
    InputSource input = null;
    existingIDs.clear();

    try {
      db = dbf.newDocumentBuilder();
      inStream = new FileInputStream(aFile);
      in = new InputStreamReader(inStream, config.encoding);
      input = new InputSource(in);
      doc = db.parse(input);
      nodeList = doc.getChildNodes();
      for (int i = 0; i < nodeList.getLength(); i++) {
        Node aNode = nodeList.item(i);
        analyzeNode(aFile.getName(), aNode, data.root, config);
      }
    } catch (ParserConfigurationException e) {
      System.out.println(e.toString());
    } catch (SAXException e) {
      System.out.println(e.toString());
    } catch (FileNotFoundException e) {
      System.out.println(e.toString());
    } catch (UnsupportedEncodingException e) {
      System.out.println(e.toString());
    } catch (IOException e) {
    } finally {
      if (inStream != null) {
        try {
          inStream.close();
        } catch (IOException e) {
          System.out.println(e.toString());
        }
      }
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          System.out.println(e.toString());
        }
      }
    }

  }

}
