package de.uniwue.dw.core.model.manager;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.manager.adapter.*;
import de.uniwue.misc.sql.BulkInserter;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;
import de.uniwue.misc.util.FileUtilsUniWue;
import de.uniwue.misc.util.StringUtilsUniWue;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The CatalogManager is a management class for the catalogEntry instances. It manages the loading,
 * saving and data management of the catalogEntries. All data management of the CatalogManager is
 * done without transactions. If a batch data manipulation job has to be performed methods iterating
 * on lists have to be used.
 */
public class CatalogManager {

  private static final String choicesDumpSplit = "~*~";

  public ICatalogAdapter catalogAdapter;

  public ICatalogCountAdapter countAdapter;

  public ICatalogNumDataAdapter numDataAdapter;

  public ICatalogChoiceAdapter choiceAdapter;

  // This map caches the catalog entries indexed by attribute IO: attrId -> catalogEntry
  protected Map<Integer, CatalogEntry> attrID2Entry = new HashMap<Integer, CatalogEntry>();

  /**
   * This map caches the catalog entries indexed by project and external ID: project -> (extID ->
   * catalogEntry)
   */
  private final Map<String, HashMap<String, CatalogEntry>> projectAndExtID2Entry = new HashMap<String, HashMap<String, CatalogEntry>>();

  // This is the root attribute for the tree of attributes. It has the attrID 0
  private CatalogEntry root;

  public CatalogManager(IDWClientAdapterFactory adapterFactory) throws SQLException {
    initializeAdapters(adapterFactory);
    initializeData();
  }

  protected CatalogManager() throws SQLException {
  }

  public static Predicate<CatalogEntry> createFilterPredicate(String word) {
    String caseInsensitiveWord = word.toLowerCase();
    return entry -> {
      return (entry.getName().toLowerCase().contains(caseInsensitiveWord));
    };
  }

  public static String cleanExtID(String extID) {
    String result = StringUtilsUniWue.cleanStringFromSpecialCharacters(extID);
    result = result.toLowerCase();
    return result;
  }

  public void addChoice(CatalogEntry anEntry, String aChoice) throws SQLException {
    if (!anEntry.getSingleChoiceChoice().contains(aChoice)) {
      choiceAdapter.insert(anEntry.getAttrId(), aChoice);
      anEntry.addSingleChoiceChoice(aChoice);
      choiceAdapter.commit();
    }
  }

  public void addChoice(CatalogEntry anEntry, Set<String> choices) throws SQLException {
    for (String aChoice : choices) {
      if (!anEntry.getSingleChoiceChoice().contains(aChoice)) {
        choiceAdapter.insert(anEntry.getAttrId(), aChoice);
        anEntry.addSingleChoiceChoice(aChoice);
      }
    }
    choiceAdapter.commit();
  }

  /*
   * clears all cached data and reloads all entries from the database
   */
  public void initializeData() throws SQLException {
    projectAndExtID2Entry.clear();
    attrID2Entry.clear();
    UniqueNameUtil.clearKnownUniqueNames();
    root = CatalogEntry.createRoot();
    attrID2Entry.put(root.getAttrId(), root);
    load();
  }

  public void initializeAdapters(IDWClientAdapterFactory adapterFactory) throws SQLException {
    catalogAdapter = adapterFactory.createCatalogAdapter(this);
    numDataAdapter = adapterFactory.createCatalogNumDataAdapter(this);
    countAdapter = adapterFactory.createCatalogCountAdapter(this);
    choiceAdapter = adapterFactory.createCatalogChoiceAdapter(this);
  }

  public void updateCounts(CatalogEntry anEntry, long pidCount, long caseIDCount, long absoluteCount)
          throws SQLException {
    anEntry.setCountAbsolute(absoluteCount);
    anEntry.setCountDistinctCaseID(caseIDCount);
    anEntry.setCountDistinctPID(pidCount);
    countAdapter.insertOrUpdateCounts(anEntry.getAttrId(), pidCount, caseIDCount, absoluteCount);
    countAdapter.commit();
  }

  /*
   * Add numeric meta information to an existing catalog entry
   */
  public void insertNumericMetaData(CatalogEntry anEntry, String unit, double lowBound, double highBound)
          throws SQLException {
    numDataAdapter.insert(anEntry.getAttrId(), unit, lowBound, highBound);
    anEntry.setUnit(unit);
    anEntry.setHighBound(highBound);
    anEntry.setLowBound(lowBound);
    numDataAdapter.commit();
  }

  protected void sortNodes() {
    for (CatalogEntry aNode : attrID2Entry.values()) {
      aNode.sortChildren();
    }
  }

  /*
   * Sort a branch of the catalog alphabetically and recursive
   */
  public void sortChildrenAlphabetically(CatalogEntry anEntry) throws SQLException {
    sortChildrenAlphabeticallyWithoutCommit(anEntry);
    catalogAdapter.commit();
  }

  /*
   * Sort a branch of the catalog alphabetically and non-recursive
   */
  public void sortChildrenAlphabeticallyNonRecursive(CatalogEntry anEntry) throws SQLException {
    sortChildrenAlphabeticallyNonRecursiveWithoutCommit(anEntry);
    catalogAdapter.commit();
  }

  private void sortChildrenAlphabeticallyWithoutCommit(CatalogEntry anEntry) throws SQLException {
    sortChildrenAlphabeticallyNonRecursiveWithoutCommit(anEntry);
    List<CatalogEntry> children = anEntry.getChildren();
    for (CatalogEntry aChild : children) {
      sortChildrenAlphabeticallyWithoutCommit(aChild);
    }
  }

  private void sortChildrenAlphabeticallyNonRecursiveWithoutCommit(CatalogEntry anEntry) throws SQLException {
    List<CatalogEntry> children = anEntry.getChildren();
    java.util.Collections.sort(children, new Comparator<CatalogEntry>() {
      @Override
      public int compare(CatalogEntry o1, CatalogEntry o2) {
        return o1.getName().toLowerCase().compareTo(o2.getName().toLowerCase());
      }
    });
    int i = 0;
    for (CatalogEntry aChild : children) {
      aChild.setOrderValue(i);
      updateEntryWithoutCommit(aChild);
      i += 100;
    }
  }

  public void commitAll() throws SQLException {
    catalogAdapter.commit();
    countAdapter.commit();
    numDataAdapter.commit();
    choiceAdapter.commit();
  }

  public void exportEntriesAsBulkExport(File outFile) throws IOException, SQLException {
    exportEntriesAsBulkExport(outFile, false, BulkInserter.DEFAULT_FIELD_TERMINATOR,
            BulkInserter.DEFAULT_ROW_TERMINATOR);
  }

  /*
   * Writes the catalog to the given file. The output is written in a non human readable fashion,
   * but can definitely be re-imported without problems (line separators are no newlines, etc.)
   */
  public void exportEntriesAsBulkExport(File outFile, boolean addHeader, String fieldTerminator, String rowTerminator)
          throws IOException, SQLException {
    StringBuilder text = new StringBuilder();
    load();
    DecimalFormat df = BulkInserter.getDecimalFormat();
    Collection<CatalogEntry> entries = getEntriesSorted();
    DBType dbType = SQLPropertiesConfiguration.getInstance().getSQLConfig().dbType;
    BulkInserter bulkInserter = new BulkInserter();
    if (addHeader) {
      String line = bulkInserter.getRow(fieldTerminator, rowTerminator, df, dbType, "Name", "Project", "ExtID",
              "AttrID", "ParentProject", "ParentExtID", "ParentAttrID", "OrderValue", "DataType", "CreationTime",
              "UniqueName", "Description", "Unit", "LowBound", "HighBound", "Choices");
      text.append(line + rowTerminator);
    }
    for (CatalogEntry anEntry : entries) {
      if (anEntry.isRoot()) {
        continue;
      }
      String lowBound = "";
      if (anEntry.getLowBound() != 0) {
        lowBound = Double.toString(anEntry.getLowBound());
      }
      String highBound = "";
      if (anEntry.getHighBound() != 0) {
        highBound = Double.toString(anEntry.getHighBound());
      }
      String choices = StringUtilsUniWue.concat(anEntry.getSingleChoiceChoice(), choicesDumpSplit);
      // beware: the AttrID and ParentAttrID are exported in the BulkFile. But do not import those
      // values without a bulkImporter. Otherwise new AttrIDs will be created by the database that
      // do not match those in the export file
      String line = bulkInserter.getRow(fieldTerminator, rowTerminator, df, dbType, anEntry.getName(),
              anEntry.getProject(), anEntry.getExtID(), anEntry.getAttrId(), anEntry.getParent().getProject(),
              anEntry.getParent().getExtID(), anEntry.getParentID(), anEntry.getOrderValue(), anEntry.getDataType(),
              anEntry.getCreationTime(), anEntry.getUniqueName(), anEntry.getDescription(), anEntry.getUnit(), lowBound,
              highBound, choices);
      text.append(line + rowTerminator);
    }
    FileUtilsUniWue.saveString2File(text.toString(), outFile);
  }

  /*
   * Writes the catalog in a "one line per catalogEntry"-format into the given file. This method
   * should be used when the output should be human readable. Be warned that this method can cause
   * problems when the data contains special characters like new lines which destroys the file
   * format.
   */
  public void exportEntry(File outFile, CatalogEntry anEntry) throws IOException, SQLException {
    StringBuilder text = new StringBuilder();
    text.append(StringUtilsUniWue.concat(
            new String[] { "Depth", "ExtID", "Project", "Name", "DataType", "ParentExtID", "ParentProject", "ToDelete",
                    "UniqueName", "Description", "Unit", "LowerBound", "UpperBound", "AttrID", "ParentAttrID" },
            "\t") + "\n");
    exportEntry(text, anEntry, 0);
    FileUtilsUniWue.saveString2File(text.toString(), outFile);
  }

  private void exportEntry(StringBuilder text, CatalogEntry aNode, int depth) throws SQLException {
    String parentExtID = "";
    String parentProject = "";
    if (!aNode.isRoot()) {
      parentExtID = aNode.getParent().getExtID();
      parentProject = aNode.getParent().getProject();
    }
    String line = StringUtilsUniWue.concat(new String[] { Integer.toString(depth), aNode.getExtID(), aNode.getProject(),
            aNode.getName(), aNode.getDataType().toString(), parentExtID, parentProject, "0", aNode.getUniqueName(),
            aNode.getDescription(), aNode.getUnit(), Double.toString(aNode.getLowBound()),
            Double.toString(aNode.getHighBound()), Integer.toString(aNode.getAttrId()),
            Integer.toString(aNode.getParent().getAttrId()) }, "\t");
    text.append(line + "\n");
    for (CatalogEntry aChildNode : aNode.getChildren()) {
      exportEntry(text, aChildNode, depth + 1);
    }
  }

  /*
   * Imports a human readable catalogFile which has been exported with the method "exportEntry"
   */
  public void importBranch(File inFile, CatalogEntry parent, InfoManager infoManager) throws IOException, SQLException {
    String text = FileUtilsUniWue.file2String(inFile);
    String[] lines = text.split("\n");
    int previousDepth = -1;
    CatalogEntry lastEntry = parent;
    boolean first = true;
    for (String aLine : lines) {
      if (first) {
        first = false;
        continue;
      }
      String[] tokens = aLine.split("\t");
      int depth = Integer.valueOf(tokens[0]);
      String extID = tokens[1];
      String project = tokens[2];
      extID = cleanExtID(extID);
      project = cleanExtID(project);
      String name = tokens[3];
      CatalogEntryType dataType = CatalogEntryType.valueOf(tokens[4]);
      // String parentExtID = tokens[5];
      // String parentProject = tokens[6];
      String loeschen = tokens[7];
      String uniqueName = tokens[8];
      String description = tokens[9];
      String unit = tokens[10];
      String lowBoundString = tokens[11];
      String highBoundString = tokens[12];
      CatalogEntry anEntry = getEntryByRefID(extID, project, false);
      if (anEntry == null) {
        anEntry = getOrCreateEntry(name, dataType, extID, parent, project, uniqueName, description);
        anEntry.setUnit(unit);
        double lowBound = 0;
        if ((lowBoundString != null) && !lowBoundString.isEmpty()) {
          lowBound = Double.valueOf(lowBoundString);
          anEntry.setLowBound(lowBound);
        }
        double highBound = 0;
        if ((highBoundString != null) && !highBoundString.isEmpty()) {
          highBound = Double.valueOf(highBoundString);
          anEntry.setHighBound(highBound);
        }
        insertNumericMetaData(anEntry, unit, lowBound, highBound);
      } else {
        if (loeschen.equals("1")) {
          deleteEntry(anEntry, infoManager);
          continue;
        }
      }
      anEntry.setDataType(dataType);
      anEntry.setName(name);
      double orderValue = 0;
      if (parent.getChildren().size() > 0) {
        CatalogEntry lastChild = parent.getLastChild();
        orderValue = lastChild.getOrderValue() + 100;
      }
      if (anEntry != lastEntry) { // if the file to be imported starts with the given parent
        if (depth > previousDepth) {
          parent = lastEntry;
        } else if (depth <= previousDepth) {
          for (int i = depth; i < previousDepth; i++) {
            parent = parent.getParent();
          }
        }
        parent.addChild(anEntry);
      }
      anEntry.setOrderValue(orderValue);
      if (anEntry != null) {
        updateEntry(anEntry);
      }
      previousDepth = depth;
      lastEntry = anEntry;
    }
  }

  /*
   * Import a dump file that has been exported with "exportEntriesAsBulkExport".
   */
  public void importBranch(File inFile) throws IOException, SQLException {
    String text = FileUtilsUniWue.file2String(inFile, "UTF-8");
    if (!text.contains(BulkInserter.DEFAULT_FIELD_TERMINATOR)) {
      // I have no idea why this is needed on the test server but it works this way...
      text = FileUtilsUniWue.file2String(inFile, "Windows-1252");
    }
    String splitter = Pattern.quote(BulkInserter.DEFAULT_ROW_TERMINATOR);
    String[] lines = text.split(splitter);
    if (SQLPropertiesConfiguration.getSQLBulkImportDir() != null) {
      catalogAdapter.setUseBulkInserts(SQLPropertiesConfiguration.getSQLBulkImportDir(), true);
    }
    for (String aLine : lines) {
      String[] tokens = aLine.split(Pattern.quote(BulkInserter.DEFAULT_FIELD_TERMINATOR), -1);
      if (tokens.length < 16) {
        System.out.println(
                "Errorneous line: " + tokens.length + " " + BulkInserter.DEFAULT_FIELD_TERMINATOR + " " + aLine);
      }
      String name = tokens[0];
      String project = tokens[1];
      String extID = tokens[2];
      String attrIDString = tokens[3];
      int attrID = Integer.valueOf(attrIDString);
      String parentAttrID = tokens[6];
      double orderValue = Double.valueOf(tokens[7]);
      CatalogEntryType type = CatalogEntryType.valueOf(tokens[8]);
      Timestamp creationTime = new Timestamp(new Date().getTime());
      String uniqueName = tokens[10];
      String description = tokens[11];
      if (SQLPropertiesConfiguration.getSQLBulkImportDir() != null) {
        catalogAdapter.insertByBulk(attrID, name, extID, parentAttrID, orderValue, type, project, creationTime,
                uniqueName, description);
      } else {
        String parentProject = tokens[4];
        String parentExtID = tokens[5];
        CatalogEntry aParent = getEntryByRefID(parentExtID, parentProject);
        catalogAdapter.insertEntry(attrID, name, type, extID, aParent.getAttrId(), orderValue, project, uniqueName,
                description);
      }
    }
    catalogAdapter.commit();
    initializeData();
    for (String aLine : lines) {
      String[] tokens = aLine.split(Pattern.quote(BulkInserter.DEFAULT_FIELD_TERMINATOR), -1);
      String project = tokens[1];
      String extID = tokens[2];
      CatalogEntry anEntry = getEntryByRefID(extID, project);
      String unit = tokens[12];
      String lowBoundString = tokens[13];
      String highBoundString = tokens[14];
      if (!unit.isEmpty() && !lowBoundString.isEmpty() && !highBoundString.isEmpty()) {
        double lowBound = Double.valueOf(lowBoundString);
        double highBound = Double.valueOf(highBoundString);
        insertNumericMetaData(anEntry, unit, lowBound, highBound);
      }
      String choices = tokens[15];
      if (!choices.isEmpty()) {
        String choiceSplitter = Pattern.quote(choicesDumpSplit);
        String[] split = choices.split(choiceSplitter);
        for (String aSplit : split) {
          anEntry.addSingleChoiceChoice(aSplit);
          choiceAdapter.insert(anEntry.getAttrId(), aSplit);
        }
      }
    }
    catalogAdapter.commit();
    catalogAdapter.setUseBulkInserts(null, false);
  }

  public void truncateCatalogTables() throws SQLException {
    catalogAdapter.truncateTable();
    choiceAdapter.truncateTable();
    countAdapter.truncateTable();
    numDataAdapter.truncateTable();
    initializeData();
  }

  /*
   * Dispose the respective query engine and free all resources that have been used by it
   */
  public void dispose() {
    catalogAdapter.dispose();
    countAdapter.dispose();
    numDataAdapter.dispose();
    choiceAdapter.dispose();
  }

  // loads all catalogEntries
  public void load() throws SQLException {
    catalogAdapter.readTables();
    finalizeLoading();
  }

  // does all stuff like sorting the children of all nodes or
  // correctly linking the catalog object instances with their parent child references
  protected void finalizeLoading() throws SQLException {
    for (CatalogEntry anEntry : attrID2Entry.values()) {
      if (anEntry.getAttrId() != 0) {
        CatalogEntry aParent = getEntryByID(anEntry.getParentID());
        if (aParent != null) {
          aParent.addChild(anEntry);
        }
      }
    }
    sortNodes();
    setDescriptionForAll();
    // add the counts to the entries
    countAdapter.readTables();
    // add high and low bounds to the entries
    numDataAdapter.readTables();
    choiceAdapter.readTables();
  }

  /*
   * Sets Parents description if there is no own Description
   */
  // TODO: why is the lifetime state of those catalog entries different from the database state ?
  // Shouldn't this be also done in the database ?
  private void setDescriptionForAll() {
    for (CatalogEntry anEntry : attrID2Entry.values()) {
      if ((anEntry.getDescription() == null || anEntry.getDescription().isEmpty()) && anEntry.getParent() != null
              && !anEntry.getParent().isRoot())
        anEntry.setDescrption(anEntry.getParent().getDescription());
    }
  }

  /*
   * Returns the catalogEntry with the given attrID or null if it doesn't exist.
   */
  public CatalogEntry getEntryByID(int attrID) throws SQLException {
    CatalogEntry result = null;
    if (attrID == 0) {
      result = getRoot();
    } else if (attrID2Entry.containsKey(attrID)) {
      result = attrID2Entry.get(attrID);
    }
    return result;
  }

  public CatalogEntry getEntryByRefID(String extID, String aProject) throws SQLException {
    return getEntryByRefID(extID, aProject, true);
  }

  public CatalogEntry getEntryByRefID(String extID, String aProject, boolean throwExceptionWhenNonExists)
          throws SQLException {
    if (extID == null) {
      throw new RuntimeException("extID is null");
    }
    if (aProject == null) {
      throw new RuntimeException("project is null");
    }
    extID = cleanExtID(extID);
    aProject = cleanExtID(aProject);
    if ((extID.isEmpty() || extID.equals(getRoot().getExtID()))
            && (aProject.isEmpty() || aProject.equals(getRoot().getProject()))) {
      return getRoot();
    }
    CatalogEntry result = null;
    if ((!projectAndExtID2Entry.containsKey(aProject) || !projectAndExtID2Entry.get(aProject).containsKey(extID))) {
      if (throwExceptionWhenNonExists) {
        throw new SQLException("An entry with extID " + extID + " and project " + aProject + " doesn't exist");
      }
    } else {
      result = projectAndExtID2Entry.get(aProject).get(extID);
    }
    // when all entries are already loaded at instantiation, why should it be necessary to reload a
    // single entry during runtime ???!
    // if (!projectAndExtID2Entry.containsKey(aProject)) {
    // projectAndExtID2Entry.put(aProject, new HashMap<String, CatalogEntry>());
    // }
    // if (projectAndExtID2Entry.get(aProject).containsKey(extID)) {
    // result = projectAndExtID2Entry.get(aProject).get(extID);
    // } else {
    // result = catalogAdapter.getEntryByRefID(extID, aProject, throwExceptionWhenNonExists);
    // if (result != null) {
    // projectAndExtID2Entry.get(aProject).put(extID, result);
    // }
    // }
    return result;
  }

  public CatalogEntry getOrCreateEntry(String name, String aProject) throws SQLException {
    return getOrCreateEntry(name, CatalogEntryType.Text, name, getRoot(), aProject, null, "");
  }

  public CatalogEntry getOrCreateEntry(String name, CatalogEntryType catalogEntryType, String aProject)
          throws SQLException {
    return getOrCreateEntry(name, catalogEntryType, name, getRoot(), aProject, null, "");
  }

  public CatalogEntry getOrCreateEntry(String name, String extID, String aProject) throws SQLException {
    return getOrCreateEntry(name, CatalogEntryType.Text, extID, getRoot(), aProject, null, "");
  }

  public CatalogEntry getOrCreateEntry(String name, CatalogEntryType dataType, String extID, CatalogEntry aParent,
          String aProject) throws SQLException {
    return getOrCreateEntry(name, dataType, extID, aParent, aProject, null);
  }

  public CatalogEntry getOrCreateEntry(String name, CatalogEntryType dataType, String extID, CatalogEntry aParent,
          String aProject, String uniqueName) throws SQLException {
    return getOrCreateEntry(name, dataType, extID, aParent, aProject, uniqueName, "");
  }

  public CatalogEntry getOrCreateEntry(String name, CatalogEntryType dataType, String extID, CatalogEntry aParent,
          double orderValue, String aProject) throws SQLException {
    return getOrCreateEntry(name, dataType, extID, aParent, orderValue, aProject, null, "");
  }

  public CatalogEntry getOrCreateEntry(String name, CatalogEntryType dataType, String extID, CatalogEntry aParent,
          String aProject, String uniqueName, String description) throws SQLException {
    CatalogEntry entryByRefID = getEntryByRefID(extID, aProject, false);
    if (entryByRefID == null) {
      return getOrCreateEntry(name, dataType, extID, aParent, getOrderValue(aParent), aProject, uniqueName,
              description);
    } else {
      return entryByRefID;
    }
  }

  public CatalogEntry getOrCreateEntryByID(int attrId, String name, CatalogEntryType dataType, String extID,
          CatalogEntry aParent, String aProject, String uniqueName, String description) throws SQLException {
    CatalogEntry entryByRefID = getEntryByID(attrId);
    if (entryByRefID == null) {
      return getOrCreateEntryByID(attrId, name, dataType, extID, aParent, getOrderValue(aParent), aProject, uniqueName,
              description);
    } else {
      return entryByRefID;
    }
  }

  private double getOrderValue(CatalogEntry aParent) throws SQLException {
    CatalogEntry parent = getEntryByID(aParent.getAttrId());
    CatalogEntry lastChild = parent.getLastChild();
    double orderValue = 0;
    if (lastChild != null) {
      orderValue = lastChild.getOrderValue() + 100;
    }
    return orderValue;
  }

  public CatalogEntry getOrCreateEntry(String name, CatalogEntryType dataType, String extID, String parentExtId,
          String parentProject, double orderValue, String aProject, String uniqueName, String description)
          throws SQLException {
    CatalogEntry parent = getEntryByRefID(parentExtId, parentProject);
    return getOrCreateEntry(name, dataType, extID, parent, orderValue, aProject, uniqueName, description);
  }

  public CatalogEntry getOrCreateEntry(String name, CatalogEntryType dataType, String extID, int parentAttrid,
          double orderValue, String aProject, String uniqueName, String description) throws SQLException {
    CatalogEntry parent = getEntryByID(parentAttrid);
    return getOrCreateEntry(name, dataType, extID, parent, orderValue, aProject, uniqueName, description);
  }

  public CatalogEntry getOrCreateEntry(String name, CatalogEntryType dataType, String extID, CatalogEntry aParent,
          double orderValue, String aProject, String uniqueName, String description) throws SQLException {
    extID = cleanExtID(extID);
    aProject = cleanExtID(aProject);
    if (orderValue == -1) {
      orderValue = getOrderValue(aParent);
    }
    CatalogEntry result = getEntryByRefID(extID, aProject, false);
    // when the catalog is transfered from one catalogManager to another one the attrIDs and the
    // instance of the parentEntries do not match, so the parent is reaccessed here
    CatalogEntry realParent = getEntryByRefID(aParent.getExtID(), aParent.getProject());
    return checkCatalogEntry(result, name, dataType, extID, realParent, orderValue, aProject, uniqueName, description);
  }

  public CatalogEntry getOrCreateEntryByID(int attrId, String name, CatalogEntryType dataType, String extID,
          CatalogEntry aParent, double orderValue, String aProject, String uniqueName, String description)
          throws SQLException {
    extID = cleanExtID(extID);
    aProject = cleanExtID(aProject);
    CatalogEntry result = getEntryByID(attrId);
    return checkCatalogEntry(result, name, dataType, extID, aParent, orderValue, aProject, uniqueName, description);
  }

  private CatalogEntry checkCatalogEntry(CatalogEntry result, String name, CatalogEntryType dataType, String extID,
          CatalogEntry aParent, double orderValue, String aProject, String uniqueName, String description)
          throws SQLException {
    if (result == null) {
      uniqueName = UniqueNameUtil.createOrRepairUniqueNameIfNecessary(name, aProject, aParent, uniqueName);
      result = catalogAdapter.insertEntry(name, dataType, extID, aParent.getAttrId(), orderValue, aProject, uniqueName,
              description);
      catalogAdapter.commit();
      addEntry(result);
    }
    if (result.getUniqueName() == null) {
      uniqueName = UniqueNameUtil.createOrRepairUniqueNameIfNecessary(name, aProject, aParent, uniqueName);
      result.setUniqueName(uniqueName);
      updateEntry(result);
    }
    if (result.getParent() != aParent) {
      aParent.addChild(result);
    }
    return result;
  }

  /*
   * Returns all extIDs of all catalogEntries of the given project
   */
  public Collection<String> getExtIDsOfProject(String project) {
    project = cleanExtID(project);
    HashMap<String, CatalogEntry> projectSet = projectAndExtID2Entry.get(project.toLowerCase());
    if (projectSet == null) {
      return new HashSet<String>();
    }
    return projectSet.keySet();
  }

  /*
   * Returns all catalogEntries for the given project
   */
  public Collection<CatalogEntry> getEntriesOfProject(String project) {
    HashSet<CatalogEntry> result = new HashSet<CatalogEntry>();
    project = cleanExtID(project);
    HashMap<String, CatalogEntry> projectSet = projectAndExtID2Entry.get(project);
    if (projectSet != null) {
      result.addAll(projectSet.values());
    }
    return result;
  }

  /*
   * Returns the name of all projects
   */
  public Set<String> getProjects() {
    Set<String> ps = projectAndExtID2Entry.keySet();
    return ps;
  }

  /*
   * Returns all entries
   */
  public Collection<CatalogEntry> getEntries() {
    return attrID2Entry.values();
  }

  /*
   * Returns all entries in a sorted manner (parent elements first, then their descendants)
   */
  public Collection<CatalogEntry> getEntriesSorted() {
    return getRoot().getDescendants();
  }

  // this method may only be called by the SQLCatalogAdapter or getOrCreateEntry !!!
  public void addEntry(CatalogEntry anEntry) throws SQLException {
    String project = cleanExtID(anEntry.getProject());
    String extiID = cleanExtID(anEntry.getExtID());
    if (!projectAndExtID2Entry.containsKey(project)) {
      projectAndExtID2Entry.put(project, new HashMap<String, CatalogEntry>());
    }
    projectAndExtID2Entry.get(project).put(extiID, anEntry);
    attrID2Entry.put(anEntry.getAttrId(), anEntry);
    String createOrRepairUniqueNameIfNecessary = UniqueNameUtil.createOrRepairUniqueNameIfNecessary(anEntry);
    if (!createOrRepairUniqueNameIfNecessary.equals(anEntry.getUniqueName())) {
      anEntry.setUniqueName(createOrRepairUniqueNameIfNecessary);
      if (DwClientConfiguration.getInstance().shouldFixInvalidUniqueNamesInDatabase()) {
        updateEntry(anEntry);
      }
    }
  }

  public CatalogEntry getRoot() {
    return root;
  }

  public void moveEntry(CatalogEntry anEntry, CatalogEntry newParent) throws SQLException {
    moveEntry(anEntry, newParent, null);
  }

  /*
   * Moves the given entry to the list of children of the given new parent after the given
   * predecessor entry
   */
  public void moveEntry(CatalogEntry anEntry, CatalogEntry newParent, CatalogEntry predecessor) throws SQLException {
    if (anEntry == predecessor) {
      return;
    }
    List<CatalogEntry> entriesToMove = new ArrayList<CatalogEntry>();
    entriesToMove.add(anEntry);
    moveEntries(entriesToMove.toArray(new CatalogEntry[0]), newParent, predecessor);
  }

  public void moveEntries(CatalogEntry[] entriesToMove, CatalogEntry newParent) throws SQLException {
    CatalogEntry predecessor = null;
    if (newParent.getChildren().size() != 0) {
      predecessor = newParent.getChildren().get(newParent.getChildren().size() - 1);
    }
    moveEntries(entriesToMove, newParent, predecessor);
  }

  /*
   * Remove the previous parent connections and add the new ones. Calculate the orderValue
   * boundaries between the predecessor and the successor and give the entries some values in
   * between
   */
  private void moveEntries(CatalogEntry[] entriesToMove, CatalogEntry newParent, CatalogEntry predecessor)
          throws SQLException {
    double preOrderValue = 0;
    CatalogEntry succ = null;

    Arrays.sort(entriesToMove);
    double incr;
    if (predecessor != null) {
      int preIndex = newParent.getChildren().indexOf(predecessor);
      preOrderValue = predecessor.getOrderValue();
      double succOrderValue;
      if (newParent.getChildren().size() > preIndex + 1) {
        succ = newParent.getChildren().get(preIndex + 1);
        succOrderValue = succ.getOrderValue();
      } else {
        succOrderValue = preOrderValue + (entriesToMove.length + 1) * 100;
      }
      incr = (succOrderValue - preOrderValue) / (entriesToMove.length + 1);
    } else {
      incr = 100;
      if (newParent.getChildren().size() > 0) {
        succ = newParent.getChildren().get(newParent.getChildren().size() - 1);
        preOrderValue = succ.getOrderValue();
      }
    }
    int i = 1;
    for (CatalogEntry anEntry : entriesToMove) {
      CatalogEntry parent = anEntry.getParent();
      if (parent != null)
        parent.removeChild(anEntry);
      newParent.addChild(anEntry);
      anEntry.setOrderValue(preOrderValue + i * incr);
      updateEntryWithoutCommit(anEntry);
      i++;
    }
    catalogAdapter.commit();
    newParent.sortChildren();
  }

  private void updateEntryWithoutCommit(CatalogEntry anEntry) throws SQLException {
    anEntry.setExtID(cleanExtID(anEntry.getExtID()));
    anEntry.setProject(cleanExtID(anEntry.getProject()));
    catalogAdapter.updateEntry(anEntry);
  }

  /*
   * Writes the data of the given entry into the database
   */
  public void updateEntry(CatalogEntry anEntry) throws SQLException {
    updateEntryWithoutCommit(anEntry);
    catalogAdapter.commit();
  }

  public void updateEntryRecursive(CatalogEntry catalogEntry) throws SQLException {
    updateEntryRecursiveInternal(catalogEntry);
    catalogAdapter.commit();
  }

  private void updateEntryRecursiveInternal(CatalogEntry catalogEntry) throws SQLException {
    if (!catalogEntry.isRoot()) {
      updateEntryWithoutCommit(catalogEntry);
    }
    for (CatalogEntry child : catalogEntry.getChildren()) {
      updateEntryRecursiveInternal(child);
    }
  }

  // deletes all entries belonging to a certain project and all sibling entries of those entries
  // this has to be done by deleting all entries individually as the branch of the whole project can
  // include other project branches
  public void deleteEntriesForProject(String project, InfoManager infoManager) throws SQLException {
    Collection<CatalogEntry> entriesOfProject = getEntriesOfProject(project);
    for (CatalogEntry anEntry : entriesOfProject.toArray(new CatalogEntry[0])) {
      deleteEntry(anEntry, infoManager);
    }
  }

  public void deleteEntry(int attrId, InfoManager infoManager) throws SQLException {
    CatalogEntry entry = getEntryByID(attrId);
    deleteEntry(entry, infoManager);
  }

  // deletes an entry and all its siblings
  public void deleteEntry(CatalogEntry anEntry, InfoManager infoManager) throws SQLException {
    infoManager.deleteInfosForEntry(anEntry);
    catalogAdapter.deleteEntry(anEntry.getAttrId());
    choiceAdapter.delete(anEntry.getAttrId());
    numDataAdapter.delete(anEntry.getAttrId());
    countAdapter.delete(anEntry.getAttrId());
    if (projectAndExtID2Entry.containsKey(anEntry.getProject())) {
      projectAndExtID2Entry.get(anEntry.getProject()).remove(anEntry.getExtID());
    }
    attrID2Entry.remove(anEntry.getAttrId());
    for (CatalogEntry aChild : anEntry.getChildren()) {
      deleteEntry(aChild, infoManager);
    }
    if (projectAndExtID2Entry.get(anEntry.getProject()) != null
            && projectAndExtID2Entry.get(anEntry.getProject()).isEmpty()) {
      projectAndExtID2Entry.remove(anEntry.getProject());
    }
    UniqueNameUtil.deleteUniqueNameFromKnownNames(anEntry.getUniqueName());
    commitAll();
  }

  public List<CatalogEntry> getEntriesByWordFilter(String word) {
    word = word.toLowerCase();
    List<CatalogEntry> result = new ArrayList<CatalogEntry>();
    if (word.isEmpty()) {
      return result;
    }
    return attrID2Entry.values().stream().filter(createFilterPredicate(word)).collect(Collectors.toList());
  }

  public void deleteAllEntries() throws SQLException {
    truncateCatalogTables();
    commitAll();
    initializeData();
  }

  public void deleteAllEntries(InfoManager infoManager) throws SQLException {
    deleteAllEntries();
    infoManager.deleteAllInfos();
  }

}
