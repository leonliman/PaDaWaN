package de.uniwue.dw.imports.configured;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.api.DWImportsConfig;
import de.uniwue.dw.imports.configured.data.ConfigAcceptedID;
import de.uniwue.dw.imports.configured.data.ConfigAllColumns;
import de.uniwue.dw.imports.configured.data.ConfigCatalog;
import de.uniwue.dw.imports.configured.data.ConfigCatalogEntry;
import de.uniwue.dw.imports.configured.data.ConfigCatalogTable;
import de.uniwue.dw.imports.configured.data.ConfigData;
import de.uniwue.dw.imports.configured.data.ConfigDataColumn;
import de.uniwue.dw.imports.configured.data.ConfigDataSourceCSVDir;
import de.uniwue.dw.imports.configured.data.ConfigDataSourceCSVFile;
import de.uniwue.dw.imports.configured.data.ConfigDataSourceFilesystem;
import de.uniwue.dw.imports.configured.data.ConfigDataTable;
import de.uniwue.dw.imports.configured.data.ConfigDataText;
import de.uniwue.dw.imports.configured.data.ConfigDataXML;
import de.uniwue.dw.imports.configured.data.ConfigFilter;
import de.uniwue.dw.imports.configured.data.ConfigMetaCases;
import de.uniwue.dw.imports.configured.data.ConfigMetaDocs;
import de.uniwue.dw.imports.configured.data.ConfigMetaGroupCase;
import de.uniwue.dw.imports.configured.data.ConfigMetaMovs;
import de.uniwue.dw.imports.configured.data.ConfigMetaPatients;
import de.uniwue.dw.imports.configured.data.ConfigStructureElem;
import de.uniwue.dw.imports.configured.data.ImportsConfig;
import de.uniwue.misc.util.XMLUtil;

public class ImportConfigReader_v_0_1 {

  public ImportConfigReader_v_0_1() {
  }

  public ImportsConfig readImportConfig(File aConfigFile)
          throws IOException, ParserConfigurationException, SAXException, ImportException {
    String xmlConfigText = FileUtils.readFileToString(aConfigFile, "UTF-8");
    return readImportConfig(xmlConfigText);
  }

  public ImportsConfig readImportConfig(String xmlConfigText)
          throws IOException, ParserConfigurationException, SAXException, ImportException {
    ImportsConfig result = null;
    InputStream xmlStream = new ByteArrayInputStream(xmlConfigText.getBytes("UTF-8"));
    PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver(
            this.getClass().getClassLoader());
    Resource xsdResource = resolver.getResource("classpath:/ConfImport_v0_1.xsd");
    InputStream xsdStream = xsdResource.getInputStream();
    boolean isValid = true;
    try {
      isValid = XMLUtil.validateAgainstXSD(xmlStream, xsdStream);
    } catch (Exception e) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "ImportConfigFile is not valid.", e);
    }
    if (!isValid) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "ImportConfigFile is not valid.");
    }
    NodeList nodeList = XMLUtil.getNodeList(xmlConfigText);
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node aNode = nodeList.item(i);
      if (aNode.getNodeName().equals("ImportConfig")) {
        result = readConfig(aNode);
      }
    }
    return result;
  }

  private ImportsConfig readConfig(Node aRootNode) throws ImportException {
    ImportsConfig result = new ImportsConfig();
    NodeList nodeList = aRootNode.getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node aSubNode = nodeList.item(i);
      if (aSubNode.getNodeName().equals("Catalog")) {
        readCatalog(aSubNode, result);
      }
      if (aSubNode.getNodeName().equals("Data")) {
        readData(aSubNode, result);
      }
      if (aSubNode.getNodeName().equals("MetaDataPatient")) {
        ConfigMetaPatients metaPatients = readMetaDataPatients(aSubNode, result);
        result.metaPatients.add(metaPatients);
      }
      if (aSubNode.getNodeName().equals("MetaDataCase")) {
        ConfigMetaCases metaCases = readMetaDataCases(aSubNode, result);
        result.metaCases.add(metaCases);
      }
      if (aSubNode.getNodeName().equals("MetaDataDoc")) {
        ConfigMetaDocs metaDocs = readMetaDataDocs(aSubNode, result);
        result.metaDocs.add(metaDocs);
      }
      if (aSubNode.getNodeName().equals("MetaDataMov")) {
        ConfigMetaMovs metaMovs = readMetaDataMovs(aSubNode, result);
        result.metaMovs.add(metaMovs);
      }
      if (aSubNode.getNodeName().equals("MetaDataGroupCase")) {
        ConfigMetaGroupCase metaGroupCase = readMetaDataGroupCase(aSubNode, result);
        result.metaGroupCase.add(metaGroupCase);
      }
    }
    return result;
  }

  private void readData(Node aRootNode, ImportsConfig aParent) throws ImportException {
    NodeList nodeList = aRootNode.getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node aSubNode = nodeList.item(i);
      if (aSubNode.getNodeName().equals("XML")) {
        ConfigData dataXml = readDataXML(aSubNode, aParent);
        aParent.datas.add(dataXml);
      }
      if (aSubNode.getNodeName().equals("CSVData")) {
        ConfigDataTable dataCSV = readDataCSV(aSubNode, aParent);
        aParent.datas.add(dataCSV);
      }
      if (aSubNode.getNodeName().equals("TextData")) {
        ConfigDataText dataText = readDataText(aSubNode, aParent);
        aParent.datas.add(dataText);
      }
    }
    for (ConfigData aData : aParent.datas) {
      String project = XMLUtil.getString(aRootNode, "Project");
      aData.setProject(project);
      String rootName = XMLUtil.getString(aRootNode, "RootName", aData.getProject());
      aData.getParentEntry().name = rootName;
      String rootExtID = XMLUtil.getString(aRootNode, "RootExtID", aData.getProject());
      aData.getParentEntry().extID = rootExtID;
      String rootProject = XMLUtil.getString(aRootNode, "RootProject", aData.getProject());
      aData.getParentEntry().projectName = rootProject;
      boolean useMetaData = XMLUtil.getBoolean(aRootNode, "UseMetaData", true);
      aData.useMetaData = useMetaData;
      aData.priority = XMLUtil.getInt(aRootNode, "Priority");
    }
  }

  private ConfigDataXML readDataXML(Node aRootNode, ImportsConfig data) {
    ConfigDataXML result = new ConfigDataXML(data);
    NodeList nodeList = aRootNode.getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node aSubNode = nodeList.item(i);
      if (aSubNode.getNodeName().equals("AcceptedExtIDs")) {
        result.acceptedExtID = readAcceptedExtIDs(aSubNode, result);
      }
      if (aSubNode.getNodeName().equals("RejectedExtIDs")) {
        result.rejectedExtID = readExtIDs(aSubNode, result);
      }
    }
    ConfigDataSourceCSVDir dataSource = new ConfigDataSourceCSVDir(result);
    result.setDataSource(dataSource);
    String dirString = XMLUtil.getString(aRootNode, "Dir");
    if (new File(dirString).isAbsolute()) {
      dataSource.dir = new File(dirString);
    } else {
      dataSource.dir = new File(DWImportsConfig.getSAPImportDir(), dirString);
    }
    result.caseRegex = XMLUtil.getString(aRootNode, "CaseIDRegex");
    result.pidRegex = XMLUtil.getString(aRootNode, "PIDRegex");
    result.docRegex = XMLUtil.getString(aRootNode, "DocIDRegex");
    result.idCSVFile = XMLUtil.getString(aRootNode, "IDCSVFile");
    // result.parentExtID = XMLUtil.getString(aRootNode, "ParentExtID");
    // result.parentProject = XMLUtil.getString(aRootNode, "ParentProject");
    dataSource.encoding = XMLUtil.getString(aRootNode, "Encoding", "UTF-8");
    return result;
  }

  private ConfigDataText readDataText(Node aRootNode, ImportsConfig data) {
    ConfigDataText result = new ConfigDataText(data);
    ConfigDataSourceFilesystem dataSource = new ConfigDataSourceFilesystem(result);
    result.setDataSource(dataSource);
    String dirString = XMLUtil.getString(aRootNode, "Dir");
    if (new File(dirString).isAbsolute()) {
      dataSource.dir = new File(dirString);
    } else {
      dataSource.dir = new File(DWImportsConfig.getSAPImportDir(), dirString);
    }
    result.extID = XMLUtil.getString(aRootNode, "ExtID");
    result.caseRegex = XMLUtil.getString(aRootNode, "CaseIDRegex");
    result.pidRegex = XMLUtil.getString(aRootNode, "PIDRegex");
    result.docRegex = XMLUtil.getString(aRootNode, "DocIDRegex");
    result.measureTimeRegex = XMLUtil.getString(aRootNode, "MeasureTimeRegex");
    result.stornoRegex = XMLUtil.getString(aRootNode, "StornoRegex");
    dataSource.encoding = XMLUtil.getString(aRootNode, "Encoding", "UTF-8");
    return result;
  }

  private List<String> readExtIDs(Node aRootNode, ConfigDataXML aParent) {
    List<String> result = new ArrayList<String>();
    NodeList nodeList = aRootNode.getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node aSubNode = nodeList.item(i);
      if (aSubNode.getNodeName().equals("ExtID")) {
        String extID = XMLUtil.getString(aSubNode, "Value");
        result.add(extID);
      }
    }
    return result;
  }

  private HashMap<String, ConfigAcceptedID> readAcceptedExtIDs(Node aRootNode,
          ConfigStructureElem aParent) {
    HashMap<String, ConfigAcceptedID> result = new HashMap<String, ConfigAcceptedID>();
    NodeList nodeList = aRootNode.getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node aSubNode = nodeList.item(i);
      if (aSubNode.getNodeName().equals("ExtID")) {
        String extID = XMLUtil.getString(aSubNode, "Value");
        boolean isRoot = XMLUtil.getBoolean(aSubNode, "IsRoot", false);
        boolean importText = XMLUtil.getBoolean(aSubNode, "ImportText", false);
        ConfigAcceptedID ID = new ConfigAcceptedID(aParent);
        ID.ID = extID;
        ID.isRoot = isRoot;
        ID.importText = importText;
        result.put(extID, ID);
      }
    }
    return result;
  }

  private ConfigDataTable readDataCSV(Node aRootNode, ImportsConfig configData)
          throws ImportException {
    ConfigDataTable result = new ConfigDataTable(configData);
    NodeList nodeList = aRootNode.getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node aSubNode = nodeList.item(i);
      if (aSubNode.getNodeName().equals("DataColumn")) {
        ConfigDataColumn dataColumn = readDataColumn(aSubNode, result);
        result.dataColumns.add(dataColumn);
      }
      if (aSubNode.getNodeName().equals("AllColumns")) {
        ConfigAllColumns allColumns = readAllColumns(aSubNode, result);
        result.allColumns = allColumns;
      }
      if (aSubNode.getNodeName().equals("Filter")) {
        ConfigFilter filter = readFilter(aSubNode, result);
        result.filter.add(filter);
      }

    }
    ConfigDataSourceCSVDir dataSource = new ConfigDataSourceCSVDir(result);
    result.setDataSource(dataSource);
    String dirString = XMLUtil.getString(aRootNode, "Dir");
    if (new File(dirString).isAbsolute()) {
      dataSource.dir = new File(dirString);
    } else {
      dataSource.dir = new File(DWImportsConfig.getSAPImportDir(), dirString);
    }
    String aDel = XMLUtil.getString(aRootNode, "Delimiter", "\t");
    String unescDel = StringEscapeUtils.unescapeJava(aDel);
    if (unescDel.length() != 1) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "delimiter for csv file has to have a length of exact one but is '" + unescDel + "'");
    }
    dataSource.delimiter = unescDel.charAt(0);
    String quoteModeString = XMLUtil.getString(aRootNode, "QuoteMode", "NONE");
    try {
      dataSource.quoteMode = QuoteMode.valueOf(quoteModeString);
    } catch (Exception e) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "QuoteMode for csv file is malformed '" + quoteModeString + "'");
    }
    String anEscapeChar = XMLUtil.getString(aRootNode, "EscapeChar", "\\");
    if (anEscapeChar.length() != 1) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "escapeChar for csv file has to have a length of exact one but is '" + anEscapeChar
                      + "'");
    }
    dataSource.escapeChar = anEscapeChar.charAt(0);
    result.extIDColumn = XMLUtil.getString(aRootNode, "ExtIDColumn");
    result.valueColumn = XMLUtil.getString(aRootNode, "ValueColumn");
    result.pidColumn = XMLUtil.getString(aRootNode, "PIDColumn");
    result.caseIDColumn = XMLUtil.getString(aRootNode, "CaseIDColumn");
    result.docIDColumn = XMLUtil.getString(aRootNode, "DocIDColumn");
    result.groupIDColumn = XMLUtil.getString(aRootNode, "DocIDColumn");
    result.measureTimestampColumn = XMLUtil.getString(aRootNode, "MeasureTimestampColumn");
    result.timestampFormat = XMLUtil.getString(aRootNode, "MeasureTimestampFormat");
    result.measureDateColumn = XMLUtil.getString(aRootNode, "MeasureDateColumn");
    result.dateFormat = XMLUtil.getString(aRootNode, "MeasureDateFormat");
    result.measureTimeColumn = XMLUtil.getString(aRootNode, "MeasureTimeColumn");
    result.timeFormat = XMLUtil.getString(aRootNode, "MeasureTimeFormat");
    result.unknownExtIDEntryExtID = XMLUtil.getString(aRootNode, "UnknownExtIDEntryExtID");
    result.stornoColumn = XMLUtil.getString(aRootNode, "StornoColumn");
    result.special = XMLUtil.getString(aRootNode, "Special");
    dataSource.encoding = XMLUtil.getString(aRootNode, "Encoding", "UTF-8");
    return result;
  }

  private HashMap<String, String> readReplacements(Node aRootNode) {
    HashMap<String, String> result = new HashMap<String, String>();
    NodeList nodeList = aRootNode.getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node aSubNode = nodeList.item(i);
      if (aSubNode.getNodeName().equals("ReplaceValue")) {
        String value = XMLUtil.getString(aSubNode, "Value");
        String replacement = XMLUtil.getString(aSubNode, "Replacement");
        result.put(value, replacement);
      }
    }
    return result;
  }

  private ConfigMetaPatients readMetaDataPatients(Node aRootNode, ImportsConfig aConfig)
          throws ImportException {
    ConfigMetaPatients result = new ConfigMetaPatients(aConfig);
    ConfigDataSourceCSVDir dataSource = new ConfigDataSourceCSVDir(result);
    result.setDataSource(dataSource);
    dataSource.dir = new File(DWImportsConfig.getSAPImportDir(),
            XMLUtil.getString(aRootNode, "Dir"));
    result.pidColumn = XMLUtil.getString(aRootNode, "PIDColumn");
    result.YOBColumn = XMLUtil.getString(aRootNode, "YOBColumn");
    result.YOBRegex = "(.*)";
    result.sexColumn = XMLUtil.getString(aRootNode, "SexColumn");
    result.stornoColumn = XMLUtil.getString(aRootNode, "StornoColumn");
    dataSource.encoding = XMLUtil.getString(aRootNode, "Encoding", "UTF-8");
    String aDel = XMLUtil.getString(aRootNode, "Delimiter", "\t");
    String unescDel = StringEscapeUtils.unescapeJava(aDel);
    if (unescDel.length() != 1) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "delimiter for csv file has to have a length of exact one but is '" + unescDel + "'");
    }
    dataSource.delimiter = unescDel.charAt(0);
    String quoteModeString = XMLUtil.getString(aRootNode, "QuoteMode", "NONE");
    try {
      dataSource.quoteMode = QuoteMode.valueOf(quoteModeString);
    } catch (Exception e) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "QuoteMode for csv file is malformed '" + quoteModeString + "'");
    }
    String anEscapeChar = XMLUtil.getString(aRootNode, "EscapeChar", "\\");
    if (anEscapeChar.length() != 1) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "escapeChar for csv file has to have a length of exact one but is '" + anEscapeChar
                      + "'");
    }
    dataSource.escapeChar = anEscapeChar.charAt(0);
    return result;
  }

  private ConfigMetaGroupCase readMetaDataGroupCase(Node aRootNode, ImportsConfig aConfig) {
    ConfigMetaGroupCase result = new ConfigMetaGroupCase(aConfig);
    ConfigDataSourceCSVDir dataSource = new ConfigDataSourceCSVDir(result);
    result.setDataSource(dataSource);
    dataSource.dir = new File(DWImportsConfig.getSAPImportDir(),
            XMLUtil.getString(aRootNode, "Dir"));
    result.groupIDColumn = XMLUtil.getString(aRootNode, "GroupIDColumn");
    result.caseIDColumn = XMLUtil.getString(aRootNode, "CaseIDColumn");
    result.listTypeColumn = XMLUtil.getString(aRootNode, "ListTypeColumn");
    return result;
  }

  private ConfigMetaCases readMetaDataCases(Node aRootNode, ImportsConfig aConfig)
          throws ImportException {
    ConfigMetaCases result = new ConfigMetaCases(aConfig);
    ConfigDataSourceCSVDir dataSource = new ConfigDataSourceCSVDir(result);
    result.setDataSource(dataSource);
    dataSource.dir = new File(DWImportsConfig.getSAPImportDir(),
            XMLUtil.getString(aRootNode, "Dir"));
    result.pidColumn = XMLUtil.getString(aRootNode, "PIDColumn");
    result.caseIDColumn = XMLUtil.getString(aRootNode, "CaseIDColumn");
    result.creationTimeStampColumn = XMLUtil.getString(aRootNode, "CreationTimeStampColumn");
    result.creationTimeStampFormat = XMLUtil.getString(aRootNode, "CreationTimeStampFormat");
    result.creationTimeColumn = XMLUtil.getString(aRootNode, "CreationTimeColumn");
    result.creationTimeFormat = XMLUtil.getString(aRootNode, "CreationTimeFormat");
    result.creationDateColumn = XMLUtil.getString(aRootNode, "CreationDateColumn");
    result.creationDateFormat = XMLUtil.getString(aRootNode, "CreationDateFormat");
    if ((result.creationTimeStampColumn == null) && (result.creationTimeFormat == null)
            && (result.creationDateFormat == null)) {
      result.creationDateFormat = "dd.MM.yyyy";
    }
    result.caseTypeColumn = XMLUtil.getString(aRootNode, "CaseTypeColumn");
    result.stornoColumn = XMLUtil.getString(aRootNode, "StornoColumn");
    dataSource.encoding = XMLUtil.getString(aRootNode, "Encoding", "UTF-8");
    String aDel = XMLUtil.getString(aRootNode, "Delimiter", "\t");
    String unescDel = StringEscapeUtils.unescapeJava(aDel);
    if (unescDel.length() != 1) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "delimiter for csv file has to have a length of exact one but is '" + unescDel + "'");
    }
    dataSource.delimiter = unescDel.charAt(0);
    String quoteModeString = XMLUtil.getString(aRootNode, "QuoteMode", "NONE");
    try {
      dataSource.quoteMode = QuoteMode.valueOf(quoteModeString);
    } catch (Exception e) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "QuoteMode for csv file is malformed '" + quoteModeString + "'");
    }
    String anEscapeChar = XMLUtil.getString(aRootNode, "EscapeChar", "\\");
    if (anEscapeChar.length() != 1) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "escapeChar for csv file has to have a length of exact one but is '" + anEscapeChar
                      + "'");
    }
    dataSource.escapeChar = anEscapeChar.charAt(0);
    return result;
  }

  private ConfigMetaDocs readMetaDataDocs(Node aRootNode, ImportsConfig aConfig)
          throws ImportException {
    ConfigMetaDocs result = new ConfigMetaDocs(aConfig);
    ConfigDataSourceCSVDir dataSource = new ConfigDataSourceCSVDir(result);
    result.setDataSource(dataSource);
    dataSource.dir = new File(DWImportsConfig.getSAPImportDir(),
            XMLUtil.getString(aRootNode, "Dir"));
    result.pidColumn = XMLUtil.getString(aRootNode, "PIDColumn");
    result.caseIDColumn = XMLUtil.getString(aRootNode, "CaseIDColumn");
    result.docIDColumn = XMLUtil.getString(aRootNode, "DocIDColumn");
    result.timeStampColumn = XMLUtil.getString(aRootNode, "TimeStampColumn");
    result.timeStampFormat = XMLUtil.getString(aRootNode, "TimeStampFormat");
    result.timeColumn = XMLUtil.getString(aRootNode, "TimeColumn");
    result.timeFormat = XMLUtil.getString(aRootNode, "TimeFormat");
    result.dateColumn = XMLUtil.getString(aRootNode, "DateColumn");
    result.dateFormat = XMLUtil.getString(aRootNode, "DateFormat");
    if ((result.timeStampFormat == null) && (result.timeFormat == null)
            && (result.dateFormat == null)) {
      result.timeStampFormat = "dd.MM.yyyy HH:mm:ss";
    }
    result.stornoColumn = XMLUtil.getString(aRootNode, "StornoColumn");
    result.docTypeColumn = XMLUtil.getString(aRootNode, "DocTypeColumn");
    dataSource.encoding = XMLUtil.getString(aRootNode, "Encoding", "UTF-8");
    String aDel = XMLUtil.getString(aRootNode, "Delimiter", "\t");
    String unescDel = StringEscapeUtils.unescapeJava(aDel);
    if (unescDel.length() != 1) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "delimiter for csv file has to have a length of exact one but is '" + unescDel + "'");
    }
    dataSource.delimiter = unescDel.charAt(0);
    String quoteModeString = XMLUtil.getString(aRootNode, "QuoteMode", "NONE");
    try {
      dataSource.quoteMode = QuoteMode.valueOf(quoteModeString);
    } catch (Exception e) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "QuoteMode for csv file is malformed '" + quoteModeString + "'");
    }
    String anEscapeChar = XMLUtil.getString(aRootNode, "EscapeChar", "\\");
    if (anEscapeChar.length() != 1) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "escapeChar for csv file has to have a length of exact one but is '" + anEscapeChar
                      + "'");
    }
    dataSource.escapeChar = anEscapeChar.charAt(0);
    return result;
  }

  private ConfigMetaMovs readMetaDataMovs(Node aRootNode, ImportsConfig aConfig)
          throws ImportException {
    ConfigMetaMovs result = new ConfigMetaMovs(aConfig);
    ConfigDataSourceCSVDir dataSource = new ConfigDataSourceCSVDir(result);
    result.setDataSource(dataSource);
    dataSource.dir = new File(DWImportsConfig.getSAPImportDir(),
            XMLUtil.getString(aRootNode, "Dir"));
    result.caseIDColumn = XMLUtil.getString(aRootNode, "CaseIDColumn");
    result.fromTimeStampColumn = XMLUtil.getString(aRootNode, "FromTimeStampColumn");
    result.fromTimeStampFormat = XMLUtil.getString(aRootNode, "FromTimeStampFormat");
    result.fromTimeColumn = XMLUtil.getString(aRootNode, "FromTimeColumn");
    result.fromTimeFormat = XMLUtil.getString(aRootNode, "FromTimeFormat");
    result.fromDateColumn = XMLUtil.getString(aRootNode, "FromDateColumn");
    result.fromDateFormat = XMLUtil.getString(aRootNode, "FromDateFormat");
    if ((result.fromTimeStampFormat == null) && (result.fromTimeFormat == null)
            && (result.fromDateFormat == null)) {
      result.fromTimeStampFormat = "dd.MM.yyyy HH:mm:ss";
    }
    result.endTimeStampColumn = XMLUtil.getString(aRootNode, "EndTimeStampColumn");
    result.endTimeStampFormat = XMLUtil.getString(aRootNode, "EndTimeStampFormat");
    result.endTimeColumn = XMLUtil.getString(aRootNode, "EndTimeColumn");
    result.endTimeFormat = XMLUtil.getString(aRootNode, "EndTimeFormat");
    result.endDateColumn = XMLUtil.getString(aRootNode, "EndDateColumn");
    result.endDateFormat = XMLUtil.getString(aRootNode, "EndDateFormat");
    if ((result.endTimeStampFormat == null) && (result.endTimeFormat == null)
            && (result.endDateFormat == null)) {
      result.endTimeStampFormat = "dd.MM.yyyy HH:mm:ss";
    }
    result.moveTypeColumn = XMLUtil.getString(aRootNode, "MoveTypeColumn");
    result.stornoColumn = XMLUtil.getString(aRootNode, "StornoColumn");
    dataSource.encoding = XMLUtil.getString(aRootNode, "Encoding", "UTF-8");
    String aDel = XMLUtil.getString(aRootNode, "Delimiter", "\t");
    String unescDel = StringEscapeUtils.unescapeJava(aDel);
    if (unescDel.length() != 1) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "delimiter for csv file has to have a length of exact one but is '" + unescDel + "'");
    }
    dataSource.delimiter = unescDel.charAt(0);
    String quoteModeString = XMLUtil.getString(aRootNode, "QuoteMode", "NONE");
    try {
      dataSource.quoteMode = QuoteMode.valueOf(quoteModeString);
    } catch (Exception e) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "QuoteMode for csv file is malformed '" + quoteModeString + "'");
    }
    String anEscapeChar = XMLUtil.getString(aRootNode, "EscapeChar", "\\");
    if (anEscapeChar.length() != 1) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "escapeChar for csv file has to have a length of exact one but is '" + anEscapeChar
                      + "'");
    }
    dataSource.escapeChar = anEscapeChar.charAt(0);
    return result;
  }

  private ConfigDataColumn readDataColumn(Node aRootNode, ConfigStructureElem aParent) {
    ConfigDataColumn result = new ConfigDataColumn(aParent);
    result.valueColumn = XMLUtil.getString(aRootNode, "ValueColumn");
    result.extID = XMLUtil.getString(aRootNode, "ExtID");
    String timestampFormatString = XMLUtil.getString(aRootNode, "TimestampFormat",
            "yyyy-MM-dd hh:mm:ss");
    result.timestampFormat = new SimpleDateFormat(timestampFormatString);
    HashMap<String, String> replacements = readReplacements(aRootNode);
    result.replacements = replacements;
    return result;
  }

  private ConfigAllColumns readAllColumns(Node aRootNode, ConfigDataTable dataCSV) {
    ConfigAllColumns result = new ConfigAllColumns(dataCSV);
    String timestampFormatString = XMLUtil.getString(aRootNode, "TimestampFormat",
            "yyyy-MM-dd hh:mm:ss");
    result.timestampFormat = new SimpleDateFormat(timestampFormatString);
    return result;
  }

  private ConfigFilter readFilter(Node aRootNode, ConfigDataTable dataCSV) {
    ConfigFilter result = new ConfigFilter(dataCSV);
    result.filterColumn = XMLUtil.getString(aRootNode, "FilterColumn");
    result.filterValue = XMLUtil.getString(aRootNode, "FilterValue");
    return result;
  }

  private void readCatalog(Node aRootNode, ImportsConfig aParent) throws ImportException {
    String project = XMLUtil.getString(aRootNode, "Project", "");
    NodeList nodeList = aRootNode.getChildNodes();
    List<ConfigCatalog> createdCatalogs = new ArrayList<ConfigCatalog>();
    int currentPrio = 0;
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node aSubNode = nodeList.item(i);
      if (aSubNode.getNodeName().equals("Entry")) {
        readEntry(aSubNode, aParent, project, createdCatalogs, currentPrio);
        currentPrio--;
      }
      if (aSubNode.getNodeName().equals("CSVCatalog")) {
        ConfigCatalogTable catalogCSV = readCatalogCSV(aSubNode, aParent);
        aParent.catalogs.add(catalogCSV);
        createdCatalogs.add(catalogCSV);
      }
    }
    for (ConfigCatalog aConfig : createdCatalogs) {
      aConfig.setProject(project);
      String projectName = XMLUtil.getString(aRootNode, "ProjectName", project);
      aConfig.projectName = projectName;
      String parentProject = XMLUtil.getString(aRootNode, "ParentProject", "");
      aConfig.setParentProject(parentProject);
      String parentExtID = XMLUtil.getString(aRootNode, "ParentExtID", "");
      aConfig.setParentExtID(parentExtID);
      // the + is for reasons of the possible inheritance of the entry nodes;
      aConfig.priority += XMLUtil.getInt(aRootNode, "Priority", 0);
    }
  }

  private ConfigCatalogEntry readEntry(Node aRootNode, ImportsConfig aParent, String project,
          List<ConfigCatalog> createdCatalogs, int currentPrio) {
    ConfigCatalogEntry result = new ConfigCatalogEntry(aParent);
    aParent.catalogs.add(result);
    createdCatalogs.add(result);
    String dataTypeString = XMLUtil.getString(aRootNode, "DataType",
            CatalogEntryType.Structure.toString());
    result.dataType = CatalogEntryType.valueOf(dataTypeString);
    result.extID = XMLUtil.getString(aRootNode, "ExtID", "");
    result.name = XMLUtil.getString(aRootNode, "Name", "");
    result.setParentExtID(XMLUtil.getString(aRootNode, "ParentExtID", ""));
    result.setParentProject(XMLUtil.getString(aRootNode, "ParentProject", ""));
    result.priority = currentPrio;
    NodeList nodeList = aRootNode.getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node aSubNode = nodeList.item(i);
      if (aSubNode.getNodeName().equals("Entry")) {
        ConfigCatalogEntry entry = readEntry(aSubNode, aParent, project, createdCatalogs,
                currentPrio);
        entry.setParentExtID(result.extID);
        entry.setParentProject(project);
        entry.priority = result.priority - 1; // the parent nodes have to be created first
      }
    }
    return result;
  }

  private ConfigCatalogTable readCatalogCSV(Node aRootNode, ImportsConfig catalogConfig)
          throws ImportException {
    ConfigCatalogTable result = new ConfigCatalogTable(catalogConfig);
    result.extIDColumn = XMLUtil.getString(aRootNode, "ExtIDColumn");
    result.nameColumn = XMLUtil.getString(aRootNode, "NameColumn");
    result.uniqueNameColumn = XMLUtil.getString(aRootNode, "UniqueNameColumn");
    result.dataTypeColumn = XMLUtil.getString(aRootNode, "DataTypeColumn");
    result.parentExtIDColumn = XMLUtil.getString(aRootNode, "ParentExtIDColumn");
    result.parentProjectColumn = XMLUtil.getString(aRootNode, "ParentProjectOverrideColumn");
    result.projectColumn = XMLUtil.getString(aRootNode, "ProjectOverrideColumn");
    result.stornoColumn = XMLUtil.getString(aRootNode, "StornoColumn");
    result.upperBoundColumn = XMLUtil.getString(aRootNode, "UpperBoundColumn");
    result.lowerBoundColumn = XMLUtil.getString(aRootNode, "LowerBoundColumn");
    result.unitColumn = XMLUtil.getString(aRootNode, "UnitColumn");
    ConfigDataSourceCSVFile dataSource = new ConfigDataSourceCSVFile(result);
    result.setDataSource(dataSource);
    dataSource.encoding = XMLUtil.getString(aRootNode, "Encoding", "UTF-8");
    dataSource
            .setFile(new File(DWImportsConfig.getTermDir(), XMLUtil.getString(aRootNode, "File")));
    String aDel = XMLUtil.getString(aRootNode, "Delimiter", "\t");
    String unescDel = StringEscapeUtils.unescapeJava(aDel);
    if (unescDel.length() != 1) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "delimiter for csv file has to have a length of exact one but is '" + unescDel + "'");
    }
    dataSource.delimiter = unescDel.charAt(0);
    String quoteModeString = XMLUtil.getString(aRootNode, "QuoteMode", "NONE");
    try {
      dataSource.quoteMode = QuoteMode.valueOf(quoteModeString);
    } catch (Exception e) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "QuoteMode for csv file is malformed '" + quoteModeString + "'");
    }
    String anEscapeChar = XMLUtil.getString(aRootNode, "EscapeChar", "\\");
    if (anEscapeChar.length() != 1) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "escapeChar for csv file has to have a length of exact one but is '" + anEscapeChar
                      + "'");
    }
    dataSource.escapeChar = anEscapeChar.charAt(0);
    return result;
  }

}
