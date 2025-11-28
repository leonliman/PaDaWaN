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
import de.uniwue.dw.imports.configured.data.ConfigDataSource;
import de.uniwue.dw.imports.configured.data.ConfigDataSourceCSVDir;
import de.uniwue.dw.imports.configured.data.ConfigDataSourceCSVFile;
import de.uniwue.dw.imports.configured.data.ConfigDataSourceDatabase;
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
import de.uniwue.dw.imports.configured.data.ConfigStructureWithDataSource;
import de.uniwue.dw.imports.configured.data.ImportsConfig;
import de.uniwue.misc.util.TimeUtil;
import de.uniwue.misc.util.XMLUtil;

public class ImportConfigReader {

  public ImportConfigReader() {
  }


  public ImportsConfig readImportConfig(File aConfigFile)
          throws IOException, ParserConfigurationException, SAXException, ImportException {
    String xmlConfigText = FileUtils.readFileToString(aConfigFile, "UTF-8");
    return readImportConfig(xmlConfigText);
  }


  public ImportsConfig readImportConfig(String xmlConfigText)
          throws IOException, ParserConfigurationException, SAXException, ImportException {
    // so that SQL-Comment are removed from XML-Attributes
    xmlConfigText = xmlConfigText.replaceAll("--.*?\n", "\n");

    ImportsConfig result = null;
    String version = "0.1";
    if (xmlConfigText.contains("Version=\"0.2\"")) {
      version = "0.2";
    }
    if (xmlConfigText.contains("Version=\"0.3\"")) {
      version = "0.3";
    }
    if (version.equals("0.1")) {
      ImportConfigReader_v_0_1 version_0_1_reader = new ImportConfigReader_v_0_1();
      result = version_0_1_reader.readImportConfig(xmlConfigText);
      result.version = version;
      return result;
    }
    InputStream xmlStream = new ByteArrayInputStream(xmlConfigText.getBytes("UTF-8"));
    PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver(
            this.getClass().getClassLoader());
    Resource xsdResource = resolver.getResource("classpath:/ConfImport.xsd");
    InputStream xsdStream = xsdResource.getInputStream();
    boolean isValid = true;
    try {
      isValid = XMLUtil.validateAgainstXSD(xmlStream, xsdStream);
    } catch (Exception e) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED, "ImportConfigFile is not valid.", e);
    }
    if (!isValid) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED, "ImportConfigFile is not valid.");
    }
    NodeList nodeList = XMLUtil.getNodeList(xmlConfigText);
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node aNode = nodeList.item(i);
      if (aNode.getNodeName().equals("ImportConfig")) {
        result = readConfig(aNode);
      }
    }
    result.version = version;
    return result;
  }


  private ImportsConfig readConfig(Node aRootNode) throws ImportException {
    ImportsConfig result = new ImportsConfig();
    NodeList nodeList = aRootNode.getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node aSubNode = nodeList.item(i);
      if (aSubNode.getNodeName().equals("CatalogTable")) {
        ConfigCatalog catalog = readCatalogTable(aSubNode, result);
        result.catalogs.add(catalog);
      }
      if (aSubNode.getNodeName().equals("CatalogEntry")) {
        ConfigCatalog catalog = readCatalogEntry(aSubNode, result);
        result.catalogs.add(catalog);
      }
      if (aSubNode.getNodeName().equals("DataTable")) {
        ConfigData data = readDataTable(aSubNode, result);
        result.datas.add(data);
      }
      if (aSubNode.getNodeName().equals("DataXML")) {
        ConfigData data = readDataXML(aSubNode, result);
        result.datas.add(data);
      }
      if (aSubNode.getNodeName().equals("DataText")) {
        ConfigData data = readDataText(aSubNode, result);
        result.datas.add(data);
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


  private void readData(Node aRootNode, ConfigData result) throws ImportException {
    result.setProject(XMLUtil.getString(aRootNode, "Project"));
    result.getParentEntry().name = XMLUtil.getString(aRootNode, "RootName", result.getProject());
    result.getParentEntry().extID = XMLUtil.getString(aRootNode, "RootExtID", result.getProject());
    result.getParentEntry().projectName = XMLUtil.getString(aRootNode, "RootProject", result.getProject());
    result.useMetaData = XMLUtil.getBoolean(aRootNode, "UseMetaData", true);
    result.priority = XMLUtil.getInt(aRootNode, "Priority");
    ConfigDataSource dataSource = readDataSource(aRootNode, result, true);
    result.setDataSource(dataSource);
  }


  private ConfigDataXML readDataXML(Node aRootNode, ImportsConfig aParent) throws ImportException {
    ConfigDataXML result = new ConfigDataXML(aParent);
    readData(aRootNode, result);
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
    result.caseRegex = XMLUtil.getString(aRootNode, "CaseIDRegex");
    result.pidRegex = XMLUtil.getString(aRootNode, "PIDRegex");
    result.docRegex = XMLUtil.getString(aRootNode, "DocIDRegex");
    result.idCSVFile = XMLUtil.getString(aRootNode, "IDCSVFile");
    // result.parentExtID = XMLUtil.getString(aRootNode, "ParentExtID");
    // result.parentProject = XMLUtil.getString(aRootNode, "ParentProject");
    return result;
  }


  private ConfigDataText readDataText(Node aRootNode, ImportsConfig aParent) throws ImportException {
    ConfigDataText result = new ConfigDataText(aParent);
    readData(aRootNode, result);
    result.extID = XMLUtil.getString(aRootNode, "ExtID");
    result.caseRegex = XMLUtil.getString(aRootNode, "CaseIDRegex");
    result.pidRegex = XMLUtil.getString(aRootNode, "PIDRegex");
    result.docRegex = XMLUtil.getString(aRootNode, "DocIDRegex");
    result.measureTimeRegex = XMLUtil.getString(aRootNode, "MeasureTimeRegex");
    result.stornoRegex = XMLUtil.getString(aRootNode, "StornoRegex");
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


  private HashMap<String, ConfigAcceptedID> readAcceptedExtIDs(Node aRootNode, ConfigStructureElem aParent) {
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


  private ConfigDataSource readDataSource(Node aRootNode, ConfigStructureWithDataSource aParent, boolean isData)
          throws ImportException {
    ConfigDataSource result = null;
    NodeList nodeList = aRootNode.getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node aSubNode = nodeList.item(i);
      if (aSubNode.getNodeName().equals("DataSourceFilesystem")) {
        result = readDataSourceFilesystem(aSubNode, aParent, isData);
      } else if (aSubNode.getNodeName().equals("DataSourceCSVDir")) {
        result = readDataSourceCSVDir(aSubNode, aParent, isData);
      } else if (aSubNode.getNodeName().equals("DataSourceCSVFile")) {
        result = readDataSourceCSVFile(aSubNode, aParent, isData);
      } else if (aSubNode.getNodeName().equals("DataSourceDatabase")) {
        result = readDataSourceDatabase(aSubNode, aParent);
      }
      if (result != null) {
        result.encoding = XMLUtil.getString(aSubNode, "Encoding", "UTF-8");
        return result;
      }
    }
    return result;
  }


  private ConfigDataSourceDatabase readDataSourceDatabase(Node aRootNode, ConfigStructureElem aParent)
          throws ImportException {
    ConfigDataSourceDatabase result = new ConfigDataSourceDatabase(aParent);
//    String typeString = XMLUtil.getString(aRootNode, "SQLType", DBType.MSSQL.toString());
//    DBType type = DBType.valueOf(typeString);
//    String server = XMLUtil.getString(aRootNode, "SQLServer");
//    String database = XMLUtil.getString(aRootNode, "SQLDatabase");
//    String user = XMLUtil.getString(aRootNode, "SQLUser");
//    String password = XMLUtil.getString(aRootNode, "SQLPassword");
//    boolean useTrustedConnection = XMLUtil.getBoolean(aRootNode, "TrustedConnection", false);
    result.dbName = XMLUtil.getString(aRootNode, "SQLDatabase");
    result.selectString = XMLUtil.getString(aRootNode, "SelectString");
    result.sourceTable = XMLUtil.getString(aRootNode, "SourceTable");
    result.alternativeMaxRecordIDTable = XMLUtil.getString(aRootNode, "AlternativeMaxRecordIDTable");
    if ((result.alternativeMaxRecordIDTable == null) || result.alternativeMaxRecordIDTable.isEmpty()) {
      DWImportsConfig.getDBImportLogManager().addSourceTable(result.sourceTable);
    } else {
      DWImportsConfig.getDBImportLogManager().addSourceTable(result.alternativeMaxRecordIDTable);
    }
    result.project = XMLUtil.getString(aRootNode, "Project");
    result.rowIDColumn = XMLUtil.getString(aRootNode, "RowIDColumn");
    result.timestampColumnName = XMLUtil.getString(aRootNode, "TimestampColumnName");
    result.nameColumnName = XMLUtil.getString(aRootNode, "NameColumnName");
    result.contentColumnName = XMLUtil.getString(aRootNode, "ContentColumnName");
//    SQLConfig config = new SQLConfig(user, database, password, server, type, useTrustedConnection);
//    result.sqlConfig = config;
    return result;
  }


  private ConfigDataSourceFilesystem readDataSourceFilesystem(Node aRootNode, ConfigStructureElem aParent,
          boolean isData) throws ImportException {
    ConfigDataSourceFilesystem result = new ConfigDataSourceFilesystem(aParent);
    readDataSourceFilesystemAttributes(aRootNode, result, isData);
    return result;
  }


  private void readDataSourceFilesystemAttributes(Node aRootNode, ConfigDataSourceFilesystem result, boolean isData)
          throws ImportException {
    String dirName = XMLUtil.getString(aRootNode, "Dir");
    File dir;
    if (isData) {
      // at the moment only data configs import a directory of CSVs
      String dirString = XMLUtil.getString(aRootNode, "Dir");
      if (new File(dirString).isAbsolute()) {
        result.dir = new File(dirString);
      } else {
        result.dir = new File(DWImportsConfig.getSAPImportDir(), dirString);
      }
    }
  }


  private ConfigDataSourceCSVFile readDataSourceCSVFile(Node aRootNode, ConfigStructureElem aParent, boolean isData)
          throws ImportException {
    ConfigDataSourceCSVFile result = new ConfigDataSourceCSVFile(aParent);
    readDataSourceCSVDirAttributes(aRootNode, result, isData);
    String filename = XMLUtil.getString(aRootNode, "File");
    File file;
    if (!isData) {
      // at the moment only catalog configs import a single file
      if (new File(filename).isAbsolute()) {
        file = new File(filename);
      } else {
        file = new File(DWImportsConfig.getTermDir(), filename);
      }
      result.setFile(file);
    }
    return result;
  }


  private ConfigDataSourceCSVDir readDataSourceCSVDir(Node aRootNode, ConfigStructureElem aParent, boolean isData)
          throws ImportException {
    ConfigDataSourceCSVDir result = new ConfigDataSourceCSVDir(aParent);
    readDataSourceCSVDirAttributes(aRootNode, result, isData);
    return result;
  }


  private void readDataSourceCSVDirAttributes(Node aRootNode, ConfigDataSourceCSVDir result, boolean isData)
          throws ImportException {
    readDataSourceFilesystemAttributes(aRootNode, result, isData);
    String aDel = XMLUtil.getString(aRootNode, "Delimiter", "\t");
    String unescDel = StringEscapeUtils.unescapeJava(aDel);
    if (unescDel.length() != 1) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "delimiter for csv file has to have a length of exact one but is '" + unescDel + "'");
    }
    result.delimiter = unescDel.charAt(0);
    String quoteModeString = XMLUtil.getString(aRootNode, "QuoteMode", "NONE");
    try {
      result.quoteMode = QuoteMode.valueOf(quoteModeString);
    } catch (Exception e) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "QuoteMode for csv file is malformed '" + quoteModeString + "'");
    }
    String anEscapeChar = XMLUtil.getString(aRootNode, "EscapeChar", "\\");
    if (anEscapeChar.length() != 1) {
      throw new ImportException(ImportExceptionType.DATA_MALFORMED,
              "escapeChar for csv file has to have a length of exact one but is '" + anEscapeChar + "'");
    }
    result.escapeChar = anEscapeChar.charAt(0);
  }


  private ConfigDataTable readDataTable(Node aRootNode, ImportsConfig aParent) throws ImportException {
    ConfigDataTable result = new ConfigDataTable(aParent);
    readData(aRootNode, result);
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
    result.extIDColumn = XMLUtil.getString(aRootNode, "ExtIDColumn");
    result.extIDRegex = XMLUtil.getString(aRootNode, "ExtIDRegex");
    result.projectColumn = XMLUtil.getString(aRootNode, "ProjectColumn");
    result.valueColumn = XMLUtil.getString(aRootNode, "ValueColumn");
    result.pidColumn = XMLUtil.getString(aRootNode, "PIDColumn");
    result.caseIDColumn = XMLUtil.getString(aRootNode, "CaseIDColumn");
    result.docIDColumn = XMLUtil.getString(aRootNode, "DocIDColumn");
    result.groupIDColumn = XMLUtil.getString(aRootNode, "GroupIDColumn");
    result.refIDColumn = XMLUtil.getString(aRootNode, "RefIDColumn");
    result.measureTimestampColumn = XMLUtil.getString(aRootNode, "MeasureTimestampColumn");
    result.timestampFormat = XMLUtil.getString(aRootNode, "MeasureTimestampFormat");
    result.measureDateColumn = XMLUtil.getString(aRootNode, "MeasureDateColumn");
    result.dateFormat = XMLUtil.getString(aRootNode, "MeasureDateFormat");
    result.measureTimeColumn = XMLUtil.getString(aRootNode, "MeasureTimeColumn");
    result.timeFormat = XMLUtil.getString(aRootNode, "MeasureTimeFormat");
    result.unknownExtIDEntryExtID = XMLUtil.getString(aRootNode, "UnknownExtIDEntryExtID");
    result.stornoColumn = XMLUtil.getString(aRootNode, "StornoColumn");
    result.special = XMLUtil.getString(aRootNode, "Special");

    if ((result.measureTimestampColumn != null) && (result.timestampFormat == null)) {
      result.timestampFormat = TimeUtil.sdf_withTimeWithMillisecondsString;
    }
    if ((result.measureDateColumn != null) && (result.dateFormat == null)) {
      result.dateFormat = TimeUtil.sdf_withoutTimeString;
    }
    if ((result.measureTimeColumn != null) && (result.timeFormat == null)) {
      result.timeFormat = TimeUtil.sdf_onlyTimeString;
    }

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


  private ConfigMetaPatients readMetaDataPatients(Node aRootNode, ImportsConfig aConfig) throws ImportException {
    ConfigMetaPatients result = new ConfigMetaPatients(aConfig);
    result.pidColumn = XMLUtil.getString(aRootNode, "PIDColumn");
    result.YOBColumn = XMLUtil.getString(aRootNode, "YOBColumn");
    result.YOBRegex = XMLUtil.getString(aRootNode, "YOBRegex", "(.*)");
    result.sexColumn = XMLUtil.getString(aRootNode, "SexColumn");
    result.stornoColumn = XMLUtil.getString(aRootNode, "StornoColumn");
    ConfigDataSource dataSource = readDataSource(aRootNode, result, true);
    result.setDataSource(dataSource);
    result.setProject("MetaPatient");
    return result;
  }


  private ConfigMetaGroupCase readMetaDataGroupCase(Node aRootNode, ImportsConfig aConfig) throws ImportException {
    ConfigMetaGroupCase result = new ConfigMetaGroupCase(aConfig);
    result.groupIDColumn = XMLUtil.getString(aRootNode, "GroupIDColumn");
    result.caseIDColumn = XMLUtil.getString(aRootNode, "CaseIDColumn");
    result.listTypeColumn = XMLUtil.getString(aRootNode, "ListTypeColumn");
    ConfigDataSource dataSource = readDataSource(aRootNode, result, true);
    result.setDataSource(dataSource);
    return result;
  }


  private ConfigMetaCases readMetaDataCases(Node aRootNode, ImportsConfig aConfig) throws ImportException {
    ConfigMetaCases result = new ConfigMetaCases(aConfig);
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
    ConfigDataSource dataSource = readDataSource(aRootNode, result, true);
    result.setDataSource(dataSource);
    result.setProject("MetaFall");
    return result;
  }


  private ConfigMetaDocs readMetaDataDocs(Node aRootNode, ImportsConfig aConfig) throws ImportException {
    ConfigMetaDocs result = new ConfigMetaDocs(aConfig);
    result.pidColumn = XMLUtil.getString(aRootNode, "PIDColumn");
    result.caseIDColumn = XMLUtil.getString(aRootNode, "CaseIDColumn");
    result.docIDColumn = XMLUtil.getString(aRootNode, "DocIDColumn");
    result.timeStampColumn = XMLUtil.getString(aRootNode, "TimeStampColumn");
    result.timeStampFormat = XMLUtil.getString(aRootNode, "TimeStampFormat");
    result.timeColumn = XMLUtil.getString(aRootNode, "TimeColumn");
    result.timeFormat = XMLUtil.getString(aRootNode, "TimeFormat");
    result.dateColumn = XMLUtil.getString(aRootNode, "DateColumn");
    result.dateFormat = XMLUtil.getString(aRootNode, "DateFormat");
    if ((result.timeStampFormat == null) && (result.timeFormat == null) && (result.dateFormat == null)) {
      result.timeStampFormat = "dd.MM.yyyy HH:mm:ss";
    }
    result.stornoColumn = XMLUtil.getString(aRootNode, "StornoColumn");
    result.docTypeColumn = XMLUtil.getString(aRootNode, "DocTypeColumn");
    ConfigDataSource dataSource = readDataSource(aRootNode, result, true);
    result.setDataSource(dataSource);
    result.setProject("MetaDocument");
    return result;
  }


  private ConfigMetaMovs readMetaDataMovs(Node aRootNode, ImportsConfig aConfig) throws ImportException {
    ConfigMetaMovs result = new ConfigMetaMovs(aConfig);
    result.caseIDColumn = XMLUtil.getString(aRootNode, "CaseIDColumn");
    result.fromTimeStampColumn = XMLUtil.getString(aRootNode, "FromTimeStampColumn");
    result.fromTimeStampFormat = XMLUtil.getString(aRootNode, "FromTimeStampFormat");
    result.fromTimeColumn = XMLUtil.getString(aRootNode, "FromTimeColumn");
    result.fromTimeFormat = XMLUtil.getString(aRootNode, "FromTimeFormat");
    result.fromDateColumn = XMLUtil.getString(aRootNode, "FromDateColumn");
    result.fromDateFormat = XMLUtil.getString(aRootNode, "FromDateFormat");
    if ((result.fromTimeStampFormat == null) && (result.fromTimeFormat == null) && (result.fromDateFormat == null)) {
      result.fromTimeStampFormat = "dd.MM.yyyy HH:mm:ss";
    }
    result.endTimeStampColumn = XMLUtil.getString(aRootNode, "EndTimeStampColumn");
    result.endTimeStampFormat = XMLUtil.getString(aRootNode, "EndTimeStampFormat");
    result.endTimeColumn = XMLUtil.getString(aRootNode, "EndTimeColumn");
    result.endTimeFormat = XMLUtil.getString(aRootNode, "EndTimeFormat");
    result.endDateColumn = XMLUtil.getString(aRootNode, "EndDateColumn");
    result.endDateFormat = XMLUtil.getString(aRootNode, "EndDateFormat");
    if ((result.endTimeStampFormat == null) && (result.endTimeFormat == null) && (result.endDateFormat == null)) {
      result.endTimeStampFormat = "dd.MM.yyyy HH:mm:ss";
    }
    result.moveTypeColumn = XMLUtil.getString(aRootNode, "MoveTypeColumn");
    result.stornoColumn = XMLUtil.getString(aRootNode, "StornoColumn");
    ConfigDataSource dataSource = readDataSource(aRootNode, result, true);
    result.setDataSource(dataSource);
    result.setProject("MetaBewegung");
    return result;
  }


  private ConfigDataColumn readDataColumn(Node aRootNode, ConfigStructureElem aParent) {
    ConfigDataColumn result = new ConfigDataColumn(aParent);
    result.valueColumn = XMLUtil.getString(aRootNode, "ValueColumn");
    result.extID = XMLUtil.getString(aRootNode, "ExtID");
    result.isExtIDColumn = XMLUtil.getBoolean(aRootNode, "IsExtIDColumn", false);
    String timestampFormatString = XMLUtil.getString(aRootNode, "TimestampFormat", "yyyy-MM-dd hh:mm:ss");
    result.timestampFormat = new SimpleDateFormat(timestampFormatString);
    HashMap<String, String> replacements = readReplacements(aRootNode);
    result.replacements = replacements;
    return result;
  }


  private ConfigAllColumns readAllColumns(Node aRootNode, ConfigDataTable dataCSV) {
    ConfigAllColumns result = new ConfigAllColumns(dataCSV);
    String timestampFormatString = XMLUtil.getString(aRootNode, "TimestampFormat", "yyyy-MM-dd hh:mm:ss");
    result.timestampFormat = new SimpleDateFormat(timestampFormatString);
    return result;
  }


  private ConfigFilter readFilter(Node aRootNode, ConfigDataTable dataCSV) {
    ConfigFilter result = new ConfigFilter(dataCSV);
    result.filterColumn = XMLUtil.getString(aRootNode, "FilterColumn");
    result.filterValue = XMLUtil.getString(aRootNode, "FilterValue");
    result.isRegex = XMLUtil.getBoolean(aRootNode, "IsRegex", false);
    return result;
  }


  private void readCatalog(Node aRootNode, ConfigCatalog result) throws ImportException {
    result.setProject(XMLUtil.getString(aRootNode, "Project", ""));
    result.projectName = XMLUtil.getString(aRootNode, "ProjectName", result.getProject());
    result.setParentProject(XMLUtil.getString(aRootNode, "ParentProject", ""));
    result.setParentExtID(XMLUtil.getString(aRootNode, "ParentExtID", ""));
    result.priority = XMLUtil.getInt(aRootNode, "Priority");
    ConfigDataSource dataSource = readDataSource(aRootNode, result, false);
    result.setDataSource(dataSource);
  }


  private ConfigCatalogEntry readCatalogEntry(Node aRootNode, ImportsConfig aParent) throws ImportException {
    ConfigCatalogEntry result = new ConfigCatalogEntry(aParent);
    readCatalog(aRootNode, result);
    String dataTypeString = XMLUtil.getString(aRootNode, "DataType", CatalogEntryType.Structure.toString());
    result.dataType = CatalogEntryType.valueOf(dataTypeString);
    result.extID = XMLUtil.getString(aRootNode, "ExtID", "");
    result.name = XMLUtil.getString(aRootNode, "Name", "");
    result.setParentExtID(XMLUtil.getString(aRootNode, "ParentExtID", ""));
    result.setParentProject(XMLUtil.getString(aRootNode, "ParentProject", ""));
    return result;
  }


  private ConfigCatalogTable readCatalogTable(Node aRootNode, ImportsConfig aParent) throws ImportException {
    ConfigCatalogTable result = new ConfigCatalogTable(aParent);
    readCatalog(aRootNode, result);
    result.extIDColumn = XMLUtil.getString(aRootNode, "ExtIDColumn");
    result.projectColumn = XMLUtil.getString(aRootNode, "ProjectColumn");
    result.attrIDColumn = XMLUtil.getString(aRootNode, "AttrIDColumn");
    result.parentExtIDColumn = XMLUtil.getString(aRootNode, "ParentExtIDColumn");
    result.parentProjectColumn = XMLUtil.getString(aRootNode, "ParentProjectColumn");
    result.parentAttrIDColumn = XMLUtil.getString(aRootNode, "ParentAttrIDColumn");
    result.nameColumn = XMLUtil.getString(aRootNode, "NameColumn");
    result.orderValueColumn = XMLUtil.getString(aRootNode, "OrderValueColumn");
    result.descriptionColumn = XMLUtil.getString(aRootNode, "DescriptionColumn");
    result.uniqueNameColumn = XMLUtil.getString(aRootNode, "UniqueNameColumn");
    result.dataTypeColumn = XMLUtil.getString(aRootNode, "DataTypeColumn");
    result.stornoColumn = XMLUtil.getString(aRootNode, "StornoColumn");
    result.upperBoundColumn = XMLUtil.getString(aRootNode, "UpperBoundColumn");
    result.lowerBoundColumn = XMLUtil.getString(aRootNode, "LowerBoundColumn");
    result.unitColumn = XMLUtil.getString(aRootNode, "UnitColumn");
    return result;
  }

}
