package de.uniwue.dw.core.model.manager.adapter;

import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.core.model.manager.InfoIterator;
import de.uniwue.dw.core.model.manager.ValueIterator;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IInfoAdapter {

    ValueIterator readValues(long pid, int attrID, long ref) throws SQLException;

    ValueIterator readValues(int attrID) throws SQLException;

    ValueIterator getDistinctValues(int attrID) throws SQLException;

    List<String> getAllValues(int attrID) throws SQLException;

    double getAverageForAllValues(int attrID) throws SQLException;

    double getStandardDeviationForAllValues(int attrID) throws SQLException;

    double getMaximumForAllValues(int attrID) throws SQLException;

    double getMinimumForAllValues(int attrID) throws SQLException;

    ValueIterator getValuesForCaseID(long caseID, int attrID) throws SQLException;

    long getInfoID(int attrID, long pid, Timestamp measureTime) throws SQLException;

    Information getInfoById(long infoid) throws SQLException;

    boolean dropTable() throws SQLException;

    InfoIterator getInfosByAttrIDBetweenTime(int attrID, Timestamp min, Timestamp max)
            throws SQLException;

    InfoIterator getInfos() throws SQLException;

    /*
     * Return a list of PIDs retrieved from the underlying data source.
     */
    List<Long> getPIDsAfterTime(Timestamp timestamp) throws SQLException;

    Set<Long> getAllPIDs() throws SQLException;

    Long getPIDByDocID(long docID) throws SQLException;

    Long getPIDByCaseID(long caseID) throws SQLException;

    /*
     * see getPIDsAfterTime
     */
    List<Long> getPIDsForProject(String project) throws SQLException;

    /*
     * see getPIDsAfterTime
     */
    List<Long> getPIDsForAttrID(int attrid) throws SQLException;

    InfoIterator getInfosByAttrID(int attrID, boolean getValue) throws SQLException;

    InfoIterator getInfosByCaseAndDoc(long caseid, long docid) throws SQLException;

    InfoIterator getInfosByCaseID(long caseID, boolean getValue) throws SQLException;

    InfoIterator getInfosByCaseIDAndAttrID(long caseID, int attrID, boolean getValue)
            throws SQLException;

    InfoIterator getInfosByCaseIDAndAttrIDs(long caseID, List<Integer> attrIDs, boolean getValue)
            throws SQLException;

    InfoIterator getInfosByDocIDAndAttrID(long docID, int attrID, boolean getValue)
            throws SQLException;

    InfoIterator getInfosByPIDAndCaseIDAndDocIDAndAttrID(long pid, long caseID, long docID, int attrID, boolean getValue)
            throws SQLException;

    InfoIterator getInfosByPIDAndAttrID(long pid, int attrID, boolean getValue) throws SQLException;

    InfoIterator getInfosByAttrIDAndPIDAndCaseIDAndMeasureTime(int attrID, long pid, long caseID,
                                                               Timestamp measureTime) throws SQLException;

    InfoIterator getInfosByAttrIDAndPIDAndCaseIDAndMeasureTimeAndDocID(int attrID, long pid, long caseID,
                                                                       Timestamp measureTime, long docID, boolean getValue) throws SQLException;

    InfoIterator getInfosByPID(long PID, boolean getValue) throws SQLException;

    InfoIterator getInfosByPIDAfterTime(long PID, boolean getValue, Timestamp timestamp) throws SQLException;

    InfoIterator getInfosForProject(String project) throws SQLException;

    List<Long> getCaseIDsByPID(long PID) throws SQLException;

    InfoIterator getSpecificInfosByCaseID(long caseID, String project) throws SQLException;

    InfoIterator getSpecificInfosByCaseID(long caseID, String project, boolean getValue) throws SQLException;

    List<Long> getRefsByAttrID(int attrID) throws SQLException;

    List<Integer> getAttrIDsOfSingleChoicesWithDataAsWellAsTheirChildren() throws SQLException;

    Map<Integer, Long> getCounts() throws SQLException;

    Map<Integer, Long> getCounts(Collection<Integer> attrIDs) throws SQLException;

    long getCount(int attrID) throws SQLException;

    long getCountForSingleChoiceValue(int attrID, String value) throws SQLException;

    void insertByBulk(Object... objects) throws SQLException;

    void deleteInfo(int attrID, long pid, Timestamp measureTime) throws SQLException;

    void deleteInfo(int attrID, long pID, long caseID, long docID, long groupID, Timestamp measureTime)
            throws SQLException;

    void deleteInfosForAttrID(int attrId) throws SQLException;

    void deleteInfosForProjectID(String dwProject) throws SQLException;

    void deleteInfosWithoutCatalogEntry() throws SQLException;

    void deleteInfosForDocID(long docID) throws SQLException;

    void deleteInfosForCaseID(long caseID) throws SQLException;

    void deleteInfosForPID(long pId) throws SQLException;

    void deleteInfosForPIDCaseIDAndDocID(long pId, long caseID, long docID) throws SQLException;

    long getCountDistinctCases(String tableName) throws SQLException;

    long getMaxPID() throws SQLException;

    long getMaxCaseID() throws SQLException;

    long getMaxRef() throws SQLException;

    void setValueDecNull(long infoID) throws SQLException;

    void setValueDec(long infoID, double valueDec) throws SQLException;

    void doBulkImportTest() throws SQLException, IOException;

    void commit() throws SQLException;

    void dispose();

    boolean truncateTable() throws SQLException;

    long insert(int attrId, long pid, Double valueDecimal, String valueStr, Timestamp measureTime,
                long caseID, long docID, long groupID, long refID) throws SQLException;

    Set<Integer> getAttrIDsForPIDs(Set<Long> pids) throws SQLException;

    Set<Integer> getAttrIDsForCaseID(Long caseID) throws SQLException;

    /*
     * This method is used by the indexer to retrieve the catalog entries that have to be indexed.
     */
    List<Integer> getAttrIDsOfInfosAfterTime(Timestamp timestamp) throws SQLException;

    List<Integer> getAttrIDsOfDeletedInfosAfterTime(Timestamp timestamp) throws SQLException;

    List<Information> getIDsAfterTimeForDeletedInfos(Timestamp timestamp) throws SQLException;

    // the following methods are for debugging, quality assessment and controlling purposes

    long getNumOfPatients() throws SQLException;

    long getNumOfPatientsBeforeMeasureTime(Timestamp timestamp) throws SQLException;

    long getNumOfCases() throws SQLException;

    long getNumOfCasesBeforeMeasureTime(Timestamp timestamp) throws SQLException;

    long getNumOfRefs() throws SQLException;

    long getNumOfRefsBeforeMeasureTime(Timestamp timestamp) throws SQLException;

    long getNumOfInfos() throws SQLException;

    long getNumOfInfosBeforeMeasureTime(Timestamp timestamp) throws SQLException;

    long getCountBeforeMeasureTime(int attrID, Timestamp timestamp) throws SQLException;

    Set<Long> getAllCaseIDs() throws SQLException;

    Set<Long> getCaseIDsBeforeMeasureTime(Timestamp timestamp) throws SQLException;

    Set<Long> getAllRefs() throws SQLException;

    Set<Long> getRefsBeforeMeasureTime(Timestamp timestamp) throws SQLException;

    Set<Long> getPIDsBeforeMeasureTime(Timestamp timestamp) throws SQLException;

    Map<Long, Integer> getCountByCaseIDForAttrID(int attrID) throws SQLException;

    // end of debugging, quality assessment and controlling methods block

    boolean isParallelizeInsert();

    void setParallelizeInsert(boolean parallelizeInsert);

}
