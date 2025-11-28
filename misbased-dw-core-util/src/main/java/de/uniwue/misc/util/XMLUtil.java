package de.uniwue.misc.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.text.ParseException;
import java.util.Date;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class XMLUtil {

  private static StringWriter stream = null;

  public static boolean validateAgainstXSD(InputStream xml, InputStream xsd) throws SAXException, IOException {
    SchemaFactory factory = SchemaFactory
            .newInstance("http://www.w3.org/2001/XMLSchema" /* XMLConstants.W3C_XML_SCHEMA_NS_URI */ );
    Schema schema = factory.newSchema(new StreamSource(xsd));
    Validator validator = schema.newValidator();
    validator.validate(new StreamSource(xml));
    return true;
  }


  public static XMLStreamWriter createStreamWriter() {
    if (stream != null) {
      System.out.println("ERROR: there is already an existing stream.");
    }
    stream = new StringWriter();
    XMLOutputFactory xmlFactory = XMLOutputFactory.newInstance();
    try {
      XMLStreamWriter writer = xmlFactory.createXMLStreamWriter(stream);
      writer.writeStartDocument();
      return writer;
    } catch (XMLStreamException e) {
      e.printStackTrace();
    }
    return null;
  }


  public static String writeWriterContent(XMLStreamWriter writer)
          throws XMLStreamException, ParserConfigurationException, SAXException, IOException {
    writer.writeEndDocument();
    writer.close();
    String xml = stream.toString();
    stream.close();
    stream = null;
    return XMLUtil.getIndented(xml);
  }


  public static void writeWriterContentToFile(XMLStreamWriter writer, File outputFile)
          throws ParserConfigurationException, SAXException, IOException, XMLStreamException {
    writer.writeEndDocument();
    writer.close();
    String xml = stream.toString();
    stream.close();
    stream = null;
    XMLUtil.writeIndented(xml, outputFile);
  }


  public static String getIndented(Document aDoc) {
    String outputXml = "";
    try {
      TransformerFactory tf = TransformerFactory.newInstance();
      // tf.setAttribute("indent-number", new Integer(4));
      Transformer transformer;
      transformer = tf.newTransformer();
      transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
      transformer.setOutputProperty(OutputKeys.INDENT, "no");
      transformer.setOutputProperty(OutputKeys.METHOD, "xml");
      transformer.setOutputProperty(OutputKeys.MEDIA_TYPE, "text/xml");
      transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
      StreamResult result = new StreamResult(new StringWriter());
      DOMSource source = new DOMSource(aDoc);
      transformer.transform(source, result);
      outputXml = result.getWriter().toString();
    } catch (TransformerConfigurationException e) {
      e.printStackTrace();
    } catch (TransformerException e) {
      e.printStackTrace();
    }
    return outputXml;
  }


  public static String getIndented(String inputXML) throws ParserConfigurationException, SAXException, IOException {
    Document aDoc = getDoc(inputXML);
    return getIndented(aDoc);
  }


  public static void writeIndented(Document aDoc, File file) {
  }


  public static void writeIndented(String inputXML, File file)
          throws ParserConfigurationException, SAXException, IOException {
    String intended = getIndented(inputXML);
    try {
      FileUtilsUniWue.saveString2File(intended, file, "UTF-8");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  public static String getDirectTextContent(Node aNode) {
    String result = "";
    NodeList nodeList = aNode.getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node aSubNode = nodeList.item(i);
      if (aSubNode.getNodeType() == Node.TEXT_NODE) {
        result += aSubNode.getNodeValue();
      }
    }
    return result;
  }


  private static String getValueIgnoreCase(Node aNode, String aName) {
    for (int i = 0; i < aNode.getAttributes().getLength(); i++) {
      Node namedItem = aNode.getAttributes().item(i);
      if (namedItem.getNodeName().equalsIgnoreCase(aName)) {
        return namedItem.getNodeValue();
      }
    }
    return null;
  }


  public static double getDouble(Node aNode, String attr) {
    return getDouble(aNode, attr, 0);
  }


  public static double getDouble(Node aNode, String attr, double defaultValue) {
    String aValue = getValueIgnoreCase(aNode, attr);
    if (aValue != null) {
      double year = Double.parseDouble(aValue);
      return year;
    } else {
      return defaultValue;
    }
  }


  public static int getInt(Node aNode, String attr, int defaultValue) {
    String aValue = getValueIgnoreCase(aNode, attr);
    if (aValue != null) {
      int year = Integer.parseInt(aValue);
      return year;
    } else {
      return defaultValue;
    }
  }


  public static long getLong(Node aNode, String attr, long defaultValue) {
    String aValue = getValueIgnoreCase(aNode, attr);
    if (aValue != null) {
      Long year = Long.parseLong(aValue);
      return year;
    } else {
      return defaultValue;
    }
  }


  public static int getInt(Node aNode, String attr) {
    return getInt(aNode, attr, Integer.MIN_VALUE);
  }


  public static long getTimeLong(Node aNode, String attr) {
    String aValue = getValueIgnoreCase(aNode, attr);
    if (aValue != null) {
      try {
        Date time = TimeUtil.getSdfWithTime().parse(aValue);
        return time.getTime();
      } catch (ParseException e) {
        e.printStackTrace();
        return 0;
      }
    } else {
      return 0;
    }
  }


  public static boolean getBoolean(Node aNode, String attr, boolean defaultValue) {
    String aValue = getValueIgnoreCase(aNode, attr);
    if (aValue != null) {
      if (aValue.equalsIgnoreCase("true")) {
        return true;
      }
      if (aValue.equalsIgnoreCase("false")) {
        return false;
      }
      System.out.println("strange boolean value '" + aValue + "'");
      return defaultValue;
    } else {
      return defaultValue;
    }
  }


  public static String getString(Node aNode, String attr) {
    return getString(aNode, attr, null);
  }


  public static String getString(Node aNode, String attr, String aDefault) {
    String aValue = getValueIgnoreCase(aNode, attr);
    if (aValue != null) {
      return aValue;
    } else {
      return aDefault;
    }
  }


  public static Document getDoc(String xml) throws ParserConfigurationException, SAXException, IOException {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setValidating(false);
    dbf.setFeature("http://xml.org/sax/features/namespaces", false);
    dbf.setFeature("http://xml.org/sax/features/validation", false);
    dbf.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
    dbf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource source = new InputSource(new ByteArrayInputStream(xml.getBytes("utf-8")));
    Document doc = db.parse(source);
    return doc;
  }


  public static NodeList getNodeList(File aFile) throws IOException, ParserConfigurationException, SAXException {
    String xmlString = FileUtilsUniWue.file2String(aFile, "UTF-8");
    return getNodeList(xmlString);
  }


  public static NodeList getNodeList(String anXMLString)
          throws ParserConfigurationException, SAXException, IOException {
    Document doc = getDoc(anXMLString);
    doc.getDocumentElement().normalize();
    NodeList nodeList = doc.getChildNodes();
    return nodeList;
  }

}
