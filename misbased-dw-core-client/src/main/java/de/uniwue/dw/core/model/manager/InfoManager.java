package de.uniwue.dw.core.model.manager;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.core.model.manager.adapter.IDWClientAdapterFactory;
import de.uniwue.dw.core.model.manager.adapter.IDeleteAdapter;
import de.uniwue.dw.core.model.manager.adapter.IInfoAdapter;
import de.uniwue.misc.sql.BulkInserter;
import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;
import de.uniwue.misc.util.FileUtilsUniWue;
import de.uniwue.misc.util.RegexUtil;
import de.uniwue.misc.util.TimeUtil;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * This manager class manages the facts from the DWInfo-table.
 */
public class InfoManager {

    public IInfoAdapter infoAdapter;

    public IDeleteAdapter deleteAdapter;

    protected CatalogManager catalogManager;

    protected InfoManager(CatalogManager aCatalogManager) {
        this.catalogManager = aCatalogManager;
    }

    public InfoManager(IDWClientAdapterFactory adapterFactory, File bulkInsertFolder) throws SQLException {
        initializeAdapters(adapterFactory, bulkInsertFolder);
    }

    protected InfoManager() {
    }

    public void initializeAdapters(IDWClientAdapterFactory adapterFactory, File bulkInsertFolder) throws SQLException {
        infoAdapter = adapterFactory.createInfoAdapter(this, bulkInsertFolder);
        if (DwClientConfiguration.getInstance().getSystemManager().getVersion() < 2) {
            deleteAdapter = adapterFactory.createDeleteAdapter(this);
        }
        catalogManager = DwClientConfiguration.getInstance().getCatalogManager();
    }

    public void dispose() {
        infoAdapter.dispose();
    }

    public void truncateInfoTables() throws SQLException {
        infoAdapter.truncateTable();
        if (DwClientConfiguration.getInstance().getSystemManager().getVersion() < 2) {
            deleteAdapter.truncateTable();
        }
    }

    public void commit() throws SQLException {
        infoAdapter.commit();
    }

    public ValueIterator readValues(int attrID) throws SQLException {
        return infoAdapter.readValues(attrID);
    }

    /*
     * Returns the List of String values of the given fact identifiers
     */
    public ValueIterator readValues(long pid, int attrID, long ref) throws SQLException {
        return infoAdapter.readValues(pid, attrID, ref);
    }

    /*
     * Returns the List of String values of the given fact identifiers
     */
    public ValueIterator readValuesForCase(long caseID, int attrID) throws SQLException {
        return infoAdapter.getValuesForCaseID(caseID, attrID);
    }

    public ValueIterator getDistincValuesOf(CatalogEntry catalogEntry) throws SQLException {
        return getDistincValuesOf(catalogEntry.getAttrId());
    }

    public ValueIterator getDistincValuesOf(int attrID) throws SQLException {
        return infoAdapter.getDistinctValues(attrID);
    }

    public List<String> getAllValuesOf(CatalogEntry catalogEntry) throws SQLException {
        return getAllValuesOf(catalogEntry.getAttrId());
    }

    public List<String> getAllValuesOf(int attrID) throws SQLException {
        return infoAdapter.getAllValues(attrID);
    }

    public double getAverageForAllValues(int attrID) throws SQLException {
        return infoAdapter.getAverageForAllValues(attrID);
    }

    public double getStandardDeviationForAllValues(int attrID) throws SQLException {
        return infoAdapter.getStandardDeviationForAllValues(attrID);
    }

    public double getMaximumForAllValues(int attrID) throws SQLException {
        return infoAdapter.getMaximumForAllValues(attrID);
    }

    public double getMinimumForAllValues(int attrID) throws SQLException {
        return infoAdapter.getMinimumForAllValues(attrID);
    }

    public long getInfoID(int attrID, long pid, Timestamp measureTime) throws SQLException {
        return infoAdapter.getInfoID(attrID, pid, measureTime);
    }

    public Information getInfoById(long infoid) throws SQLException {
        return infoAdapter.getInfoById(infoid);
    }

    public InfoIterator getInfosByAttrIDBetweenTime(int attrID, Timestamp min, Timestamp max) throws SQLException {
        return infoAdapter.getInfosByAttrIDBetweenTime(attrID, min, max);
    }

    public InfoIterator getInfos() throws SQLException {
        return infoAdapter.getInfos();
    }

    public List<Long> getPIDsAfterTime(Timestamp timestamp) throws SQLException {
        return infoAdapter.getPIDsAfterTime(timestamp);
    }

    public Set<Long> getAllPIDs() throws SQLException {
        return infoAdapter.getAllPIDs();
    }

    public Long getPIDByDocID(long docID) throws SQLException {
        return infoAdapter.getPIDByDocID(docID);
    }

    public Long getPIDByCaseID(long caseID) throws SQLException {
        return infoAdapter.getPIDByCaseID(caseID);
    }

    public Set<Long> getPIDsBeforeTime(Timestamp timestamp) throws SQLException {
        return infoAdapter.getPIDsBeforeMeasureTime(timestamp);
    }

    public List<Long> getPIDsForProject(String project) throws SQLException {
        return infoAdapter.getPIDsForProject(project);
    }

    public List<Long> getPIDsForAttrID(int attrId) throws SQLException {
        return infoAdapter.getPIDsForAttrID(attrId);
    }

    public Set<Integer> getAttrIDsForPIDs(Set<Long> pids) throws SQLException {
        return infoAdapter.getAttrIDsForPIDs(pids);
    }

    public Set<Integer> getAttrIDsForCaseID(Long caseID) throws SQLException {
        return infoAdapter.getAttrIDsForCaseID(caseID);
    }

    public List<Integer> getAttrIDsOfInfosAfterTime(Timestamp timestamp) throws SQLException {
        return infoAdapter.getAttrIDsOfInfosAfterTime(timestamp);
    }

    public List<Integer> getAttrIDsOfDeletedInfosAfterTime(Timestamp timestamp) throws SQLException {
        if (DwClientConfiguration.getInstance().getSystemManager().getVersion() < 2) {
            return deleteAdapter.getAttrIDsOfDeletedInfosAfterTime(timestamp);
        } else {
            return infoAdapter.getAttrIDsOfDeletedInfosAfterTime(timestamp);
        }
    }

    public List<Information> getIDsAfterTimeForDeletedInfos(Timestamp timestamp) throws SQLException {
        if (DwClientConfiguration.getInstance().getSystemManager().getVersion() < 2) {
            return deleteAdapter.getIDsAfterTimeForDeletedInfos(timestamp);
        } else {
            return infoAdapter.getIDsAfterTimeForDeletedInfos(timestamp);
        }
    }

    public InfoIterator getInfosByAttrID(int attrID, boolean getValue) throws SQLException {
        return infoAdapter.getInfosByAttrID(attrID, getValue);
    }

    public InfoIterator getInfosByEntry(CatalogEntry anEntry, boolean getValue) throws SQLException {
        return infoAdapter.getInfosByAttrID(anEntry.getAttrId(), getValue);
    }

    public InfoIterator getInfosByCaseAndDoc(long caseid, long docid) throws SQLException {
        return infoAdapter.getInfosByCaseAndDoc(caseid, docid);
    }

    public InfoIterator getInfosByCaseID(long caseID, boolean getValue) throws SQLException {
        return infoAdapter.getInfosByCaseID(caseID, getValue);
    }

    public InfoIterator getInfosByCaseIDAndEntry(long caseID, CatalogEntry anEntry, boolean getValue)
            throws SQLException {
        return infoAdapter.getInfosByCaseIDAndAttrID(caseID, anEntry.getAttrId(), getValue);
    }

    public InfoIterator getInfosByDocIDAndCatalogEntry(long docID, CatalogEntry anEntry, boolean getValue) throws SQLException {
        return infoAdapter.getInfosByDocIDAndAttrID(docID, anEntry.getAttrId(), getValue);
    }

    public InfoIterator getInfosByPIDAndCaseIDAndDocIDAndCatalogEntry(long pid, long caseID, long docID,
                                                                      CatalogEntry anEntry,
                                                                      boolean getValue) throws SQLException {
        return infoAdapter.getInfosByPIDAndCaseIDAndDocIDAndAttrID(pid, caseID, docID, anEntry.getAttrId(), getValue);
    }

    public InfoIterator getInfosByPIDAndCatalogEntry(long pid, CatalogEntry anEntry, boolean getValue)
            throws SQLException {
        return infoAdapter.getInfosByPIDAndAttrID(pid, anEntry.getAttrId(), getValue);
    }

    public InfoIterator getInfosByCaseIDAndEntryIncludingSiblings(long caseID, CatalogEntry anEntry, boolean getValue)
            throws SQLException {
        List<Integer> ids = new ArrayList<Integer>();
        ids.add(anEntry.getAttrId());
        for (CatalogEntry aSibling : anEntry.getDescendants()) {
            ids.add(aSibling.getAttrId());
        }
        return infoAdapter.getInfosByCaseIDAndAttrIDs(caseID, ids, getValue);
    }

    public InfoIterator getInfosByPID(long PID, boolean getValue) throws SQLException {
        return infoAdapter.getInfosByPID(PID, getValue);
    }

    public InfoIterator getInfosByPIDAfterTime(long PID, boolean getValue, Timestamp timestamp) throws SQLException {
        return infoAdapter.getInfosByPIDAfterTime(PID, getValue, timestamp);
    }

    public List<Long> getCaseIDsByPID(long PID) throws SQLException {
        return infoAdapter.getCaseIDsByPID(PID);
    }

    public Set<Long> getAllCaseIDs() throws SQLException {
        return infoAdapter.getAllCaseIDs();
    }

    public Set<Long> getCaseIDsBeforeTime(Timestamp timestamp) throws SQLException {
        return infoAdapter.getCaseIDsBeforeMeasureTime(timestamp);
    }

    public Set<Long> getAllRefs() throws SQLException {
        return infoAdapter.getAllRefs();
    }

    public Set<Long> getRefsBeforeTime(Timestamp timestamp) throws SQLException {
        return infoAdapter.getRefsBeforeMeasureTime(timestamp);
    }

    public InfoIterator getSpecificInfosByCaseID(long caseID, String project) throws SQLException {
        return getSpecificInfosByCaseID(caseID, project, true);
    }

    public InfoIterator getSpecificInfosByCaseID(long caseID, String project, boolean getValue) throws SQLException {
        return infoAdapter.getSpecificInfosByCaseID(caseID, project, getValue);
    }

    public List<Long> getRefsByAttrID(int attrID) throws SQLException {
        return infoAdapter.getRefsByAttrID(attrID);
    }

    public Map<Integer, Long> getCounts() throws SQLException {
        return infoAdapter.getCounts();
    }

    public Map<Integer, Long> getCounts(Collection<Integer> attrIDs) throws SQLException {
        return infoAdapter.getCounts(attrIDs);
    }

    public long getCount(Integer attrID) throws SQLException {
        return infoAdapter.getCount(attrID);
    }

    public long getCountForSingleChoiceValue(Integer attrID, String value) throws SQLException {
        return infoAdapter.getCountForSingleChoiceValue(attrID, value);
    }

    public long getCountBeforeTime(Integer attrID, Timestamp timestamp) throws SQLException {
        return infoAdapter.getCountBeforeMeasureTime(attrID, timestamp);
    }

    public void exportBulk(File aFile) throws SQLException, IOException {
        StringBuilder text = new StringBuilder();
        DecimalFormat df = BulkInserter.getDecimalFormat();
        InfoIterator iter = getInfos();
        DBType dbType = SQLPropertiesConfiguration.getInstance().getSQLConfig().dbType;
        BulkInserter bulkInserter = new BulkInserter();
        for (Information anInfo : iter) {
            String line = bulkInserter.getRow(BulkInserter.DEFAULT_FIELD_TERMINATOR, BulkInserter.DEFAULT_ROW_TERMINATOR, df,
                    dbType, anInfo.getInfoID(), anInfo.getAttrID(), anInfo.getPid(), anInfo.getMeasureTime(),
                    "2000-01-01 00:00:00.000000", anInfo.getCaseID(), anInfo.getDocID(), anInfo.getGroupID(), anInfo.getRef(),
                    anInfo.getValue(), anInfo.getValueShort(), anInfo.getValueDec(), anInfo.isStorno(),
                    anInfo.getUpdateTime());
            text.append(line + BulkInserter.DEFAULT_ROW_TERMINATOR);
        }
        FileUtilsUniWue.saveString2File(text.toString(), aFile);
    }

    public void importBulk(File aFile) throws SQLException, IOException {
        String text = FileUtilsUniWue.file2String(aFile, "UTF-8");
        if (!text.contains(BulkInserter.DEFAULT_FIELD_TERMINATOR)) {
            // I have no idea why this is needed on the test server but it works this way...
            text = FileUtilsUniWue.file2String(aFile, "Windows-1252");
        }
        String splitter = Pattern.quote(BulkInserter.DEFAULT_ROW_TERMINATOR);
        String[] lines = text.split(splitter);
        for (String aLine : lines) {
            String[] tokens = aLine.split(Pattern.quote(BulkInserter.DEFAULT_FIELD_TERMINATOR), -1);
            if (tokens.length < 12) {
                System.out.println(
                        "Errorneous line: " + tokens.length + " " + BulkInserter.DEFAULT_FIELD_TERMINATOR + " " + aLine);
            }
            if (SQLPropertiesConfiguration.getSQLBulkImportDir() != null) {
                infoAdapter.insertByBulk(tokens[0], tokens[1], tokens[2], tokens[3], tokens[4], tokens[5], tokens[8], tokens[6],
                        tokens[7], tokens[9], tokens[10], tokens[11], tokens[12], tokens[13]);
            } else {
                int attrID = Integer.valueOf(tokens[1]);
                long pid = Long.valueOf(tokens[2]);
                Timestamp measureTime = Timestamp.valueOf(tokens[3]);
                long caseID = Long.valueOf(tokens[5]);
                long refID = Long.valueOf(tokens[8]);
                long docID = Long.valueOf(tokens[6]);
                long groupID = Long.valueOf(tokens[7]);
                String value = tokens[9];
                double valueDecimal = Double.valueOf(tokens[11]);
                infoAdapter.insert(attrID, pid, valueDecimal, value, measureTime, caseID, docID, groupID, refID);
            }
        }
        commit();
    }

    //// INSERTS

    // insert date
    public long insert(CatalogEntry anEntry, long pid, Date value, Timestamp measureTime, long caseID, long docID,
                       long groupID) throws SQLException {
        String formatedDate = TimeUtil.getSdfWithTimeSQLTimestamp().format(value);
        return insert(anEntry, pid, formatedDate, measureTime, caseID, docID, groupID);
    }

    public long insert(CatalogEntry anEntry, long pid, Date value, Timestamp measureTime, long caseID, long docID)
            throws SQLException {
        return insert(anEntry, pid, value, measureTime, caseID, docID, 0);
    }

    public long insert(CatalogEntry anEntry, long pid, Date value, Timestamp measureTime, long caseID)
            throws SQLException {
        return insert(anEntry, pid, value, measureTime, caseID, 0);
    }

    // insert String
    public long insert(CatalogEntry anEntry, long pid, String value, Timestamp measureTime, long caseID, long docID)
            throws SQLException {
        return insert(anEntry, pid, value, measureTime, caseID, docID, 0);
    }

    public long insert(CatalogEntry anEntry, long pid, String value, Timestamp measureTime, long caseID, long docID,
                       long groupID) throws SQLException {
        if (value == null) {
            value = "";
        }
        double valueDecimal = RegexUtil.parseNumber(value, true);
        return insert(anEntry, pid, valueDecimal, value, measureTime, caseID, docID, groupID);
    }

    public long insert(CatalogEntry anEntry, long pid, double value, String valueStr, Timestamp measureTime, long caseID,
                       long docID) throws SQLException {
        return insert(anEntry, pid, value, valueStr, measureTime, caseID, docID, 0);
    }

    public long insert(CatalogEntry anEntry, long pid, double value, String valueStr, Timestamp measureTime, long caseID,
                       long docID, long groupID) throws SQLException {
        if (anEntry.getDataType() == CatalogEntryType.Structure) {
            throw new SQLException(
                    "Structure catalog entriy " + anEntry.getName() + ":" + anEntry.getProject() + " cannot contain data");
        } else if (anEntry.getDataType() == CatalogEntryType.SingleChoice) {
            if (!anEntry.getSingleChoiceChoice().contains(valueStr)) {
                catalogManager.addChoice(anEntry, valueStr);
            }
        }
        return infoAdapter.insert(anEntry.getAttrId(), pid, value, valueStr, measureTime, caseID, docID, groupID, docID);
    }

    public long insert(CatalogEntry anEntry, long pid, Timestamp measureTime, long caseID, long docID, long groupID)
            throws SQLException {
        return insert(anEntry, pid, "", measureTime, caseID, docID, groupID);
    }

    public long insert(CatalogEntry anEntry, long pid, Timestamp measureTime, long caseID, long docID)
            throws SQLException {
        return insert(anEntry, pid, "", measureTime, caseID, docID, 0);
    }

    public long insert(CatalogEntry anEntry, long pid, Timestamp measureTime, long caseID) throws SQLException {
        return insert(anEntry, pid, "", measureTime, caseID, 0);
    }

    // DELETES

    public void deleteInfo(CatalogEntry anEntry, long pid, long caseid, Timestamp measureTime) throws SQLException {
        if (DwClientConfiguration.getInstance().getSystemManager().getVersion() < 2) {
            InfoIterator infosByAttrIDAndPIDAndCaseIDAndMeasureTime = infoAdapter
                    .getInfosByAttrIDAndPIDAndCaseIDAndMeasureTime(anEntry.getAttrId(), pid, caseid, measureTime);
            deleteAdapter.markInfosDeleted(infosByAttrIDAndPIDAndCaseIDAndMeasureTime);
            try {
                infosByAttrIDAndPIDAndCaseIDAndMeasureTime.dispose();
            } catch (DWIterException e) {
                throw new SQLException(e);
            }
        }
        infoAdapter.deleteInfo(anEntry.getAttrId(), pid, measureTime);
    }

    public void deleteInfo(CatalogEntry anEntry, long pID, long caseID, long docID, long groupID, Timestamp measureTime)
            throws SQLException {
        infoAdapter.deleteInfo(anEntry.getAttrId(), pID, caseID, docID, groupID, measureTime);
    }

    public void deleteInfosForEntry(CatalogEntry anEntry) throws SQLException {
        if (DwClientConfiguration.getInstance().getSystemManager().getVersion() < 2) {
            InfoIterator infosByAttrID = infoAdapter.getInfosByAttrID(anEntry.getAttrId(), true);
            deleteAdapter.markInfosDeleted(infosByAttrID);
            try {
                infosByAttrID.dispose();
            } catch (DWIterException e) {
                throw new SQLException(e);
            }
        }
        infoAdapter.deleteInfosForAttrID(anEntry.getAttrId());
    }

    public void deleteInfosForProjectID(String project) throws SQLException {
        if (DwClientConfiguration.getInstance().getSystemManager().getVersion() < 2) {
            InfoIterator infosForProject = infoAdapter.getInfosForProject(project);
            deleteAdapter.markInfosDeleted(infosForProject);
            try {
                infosForProject.dispose();
            } catch (DWIterException e) {
                throw new SQLException(e);
            }
        }
        infoAdapter.deleteInfosForProjectID(project);
    }

    public void deleteInfosWithoutCatalogEntry() throws SQLException {
        infoAdapter.deleteInfosWithoutCatalogEntry();
    }

    public void deleteInfosForCaseID(long caseID) throws SQLException {
        infoAdapter.deleteInfosForCaseID(caseID);
    }

    public void deleteInfosForDocID(long docID) throws SQLException {
        infoAdapter.deleteInfosForDocID(docID);
    }

    public void deleteInfosForPID(long pId) throws SQLException {
        infoAdapter.deleteInfosForPID(pId);
    }

    public void deleteInfosForPIDCaseIDAndDocID(long pId, long caseID, long docID) throws SQLException {
        infoAdapter.deleteInfosForPIDCaseIDAndDocID(pId, caseID, docID);
    }

    public boolean deleteAllInfos() throws SQLException {
        if (DwClientConfiguration.getInstance().getSystemManager().getVersion() < 2) {
            deleteAdapter.truncateTable();
        }
        return infoAdapter.truncateTable();
    }

    public long getCountDistinctCases(String tableName) throws SQLException {
        return infoAdapter.getCountDistinctCases(tableName);
    }

    public Map<Long, Integer> getCountByCaseIDForAttrID(int attrID) throws SQLException {
        return infoAdapter.getCountByCaseIDForAttrID(attrID);
    }

    public long getMaxPID() throws SQLException {
        return infoAdapter.getMaxPID();
    }

    public long getMaxCaseID() throws SQLException {
        return infoAdapter.getMaxCaseID();
    }

    public long getMaxRef() throws SQLException {
        return infoAdapter.getMaxRef();
    }

    public long getNumOfPatients() throws SQLException {
        return infoAdapter.getNumOfPatients();
    }

    public long getNumOfPatientsBeforeTime(Timestamp timestamp) throws SQLException {
        return infoAdapter.getNumOfPatientsBeforeMeasureTime(timestamp);
    }

    public long getNumOfCases() throws SQLException {
        return infoAdapter.getNumOfCases();
    }

    public long getNumOfCasesBeforeTime(Timestamp timestamp) throws SQLException {
        return infoAdapter.getNumOfCasesBeforeMeasureTime(timestamp);
    }

    public long getNumOfRefs() throws SQLException {
        return infoAdapter.getNumOfRefs();
    }

    public long getNumOfRefsBeforeTime(Timestamp timestamp) throws SQLException {
        return infoAdapter.getNumOfRefsBeforeMeasureTime(timestamp);
    }

    public long getNumOfInfos() throws SQLException {
        return infoAdapter.getNumOfInfos();
    }

    public long getNumOfInfosBeforeTime(Timestamp timestamp) throws SQLException {
        return infoAdapter.getNumOfInfosBeforeMeasureTime(timestamp);
    }

    public InfoIterator getInfosByAttrIDAndPIDAndCaseIDAndMeasureTimeAndDocID(int attrID, long pid, long caseID,
                                                                              Timestamp measureTime, long docID, boolean getValue) throws SQLException {
        return infoAdapter
                .getInfosByAttrIDAndPIDAndCaseIDAndMeasureTimeAndDocID(attrID, pid, caseID, measureTime, docID, getValue);
    }

}
