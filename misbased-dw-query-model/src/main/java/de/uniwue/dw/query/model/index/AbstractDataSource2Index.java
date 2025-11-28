package de.uniwue.dw.query.model.index;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.CountType;
import de.uniwue.dw.core.client.authentication.group.AuthManager;
import de.uniwue.dw.core.client.authentication.group.Group;
import de.uniwue.dw.core.model.data.*;
import de.uniwue.dw.core.model.manager.*;
import de.uniwue.dw.core.sql.IDwSqlSchemaConstant;
import de.uniwue.dw.query.model.DWQueryConfig;
import de.uniwue.dw.query.model.manager.IndexLogManager;
import de.uniwue.misc.sql.IParamsAdapter;
import de.uniwue.misc.util.TimeUtil;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.util.*;

/**
 * An AbstractDataSource2Index can be inherited by any engine that likes to create an index on an
 * clinical Data Warehouse independent of the implementation of the index engine. The
 * AbstractDataSource2Index manages the iteration over patients that have to be indexed and those
 * that have to be deleted. It manages flags in the data source that are used for those purposes.
 */
public abstract class AbstractDataSource2Index {

  private CatalogManager catalogManager;

  private InfoManager infoManager;

  private IParamsAdapter indexParamAdapter;

  private AuthManager authManager;

  protected boolean isFirstIndexRun = false;

  protected AbstractDataSource2Index() throws IndexException {
    IndexLogManager.init();
    initialize();
  }

  private CatalogManager createCatalogManager() throws SQLException {
    return DwClientConfiguration.getInstance().getCatalogManager();
  }

  private InfoManager createInfoManager() throws SQLException {
    return DwClientConfiguration.getInstance().getInfoManager();
  }

  protected void initialize() throws IndexException {
    try {
      IndexLogManager.info("Initializing Indexer, creating managers.", getServerID());
      indexParamAdapter = DWQueryConfig.getInstance().getQueryAdapterFactory()
              .createParamsAdapter(getIndexParamStorageName());
      infoManager = createInfoManager();
      catalogManager = createCatalogManager();
      authManager = DwClientConfiguration.getInstance().getAuthManager();
      if (getLastDataUpdateDate() == null) {
        // cache the flag that this run is the first run for the creation of the index
        isFirstIndexRun = true;
      }
      IndexLogManager.info("Initializing Indexer finished", getServerID());
    } catch (SQLException e) {
      throw new IndexException(e);
    }
  }

  /*
   * With this method the index files (or any further means of individual data storage) of the
   * respective index server implementation is physically deleted
   */
  abstract protected void deleteIndex() throws IndexException;

  /*
   * This method has to be implemented with the code to index the information of this case of this
   * patient in the respective index server implementation. All facts in the list belong to the same
   * patient
   */
  abstract protected void indexData(Patient patient) throws IndexException;

  /*
   * The ServerID is a name for the particular index server. It is used to manage the parameters in
   * the database for this server.
   */
  abstract protected String getServerID();

  /*
   * If the index server implementation is transaction based, this call has to be connected to the
   * commit method of the respective server implementation
   */
  abstract protected void commit() throws IndexException;

  /*
   * Deletes these facts. All facts in the list belong to the same patient
   */
  abstract protected void deleteData(long pid, List<Information> infos) throws IndexException;

  /*
   * Calculate the counts of the catalog entry and write it to the respective table in the database
   */
  abstract protected long calculateCount(CatalogEntry anEntry, CountType countType) throws IndexException;

  /*
   * Index this catalogEntry. This method has to be implemented if the index draws its catalog
   * information from the index itself instead from the database
   */
  abstract protected void indexCatalogEntry(CatalogEntry anEntry) throws IndexException;

  private void indexCatalogEntryWithoutException(CatalogEntry anEntry) {
    try {
      indexCatalogEntry(anEntry);
    } catch (IndexException e) {
      throw new RuntimeException(e);
    }
  }

  /*
   * Free any resources that have been allocated by the indexing engine
   */
  public void dispose() {
    getCatalogManager().dispose();
    getInfoManager().dispose();
  }

  private void lockDW() throws IndexException {
    try {
      DwClientConfiguration.getInstance().getSystemManager().lock(getServerID());
    } catch (Exception e) {
      throw new IndexException(e);
    }
  }

  private void unlockDW() throws IndexException {
    try {
      DwClientConfiguration.getInstance().getSystemManager().unlock(getServerID());
    } catch (Exception e) {
      throw new IndexException(e);
    }
  }

  /*
   * Update indexes all cases added after the last successful updated call or all if it is the first
   * call for this server
   */
  public void update() throws IndexException {
    if (DWQueryConfig.lockDWForUpdate()) {
      lockDW();
    }
    try {
      initializeUpdate();
      updateDeletedData();
      updateNewData();
      calculateCatalogCounts();
      indexCatalog();
      if (DWQueryConfig.cleanCatalogIndex()) {
        cleanCatalogIndex();
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new IndexException(e);
    } finally {
      if (DWQueryConfig.lockDWForUpdate()) {
        unlockDW();
      }
    }
  }

  public abstract void cleanCatalogIndex() throws IndexException;

  private String getIndexParamStorageName() {
    return IDwSqlSchemaConstant.T_SYSTEM_PARAMS;
  }

  private void saveCurrentTimeForCatalogIndexTimestampParam(String timeString) throws IndexException {
    if (doNormalDataIndexing()) {
      // when not a specific project or attrID has been explicitly indexed, save the provided timestamp
      setParam(getServerMaxCatalogIndexDateParamName(), timeString);
    }
  }

  /*
   * This method does all initialization that has to be done before actual data or the catalog is
   * indexed. This can be the deletion of the index, creation of models in the respective index
   * implementations,...
   */
  protected void initializeUpdate() throws IndexException {
    if (DWQueryConfig.hasToDeleteIndex()) {
      deleteAllData();
    }
    if (DWQueryConfig.hasToIndexAllData()) {
      deleteMaxUpdateFlag();
      // when the index is recreated anyway, no deletions have to be done
      setParam(getServerMaxImportDeleteDateParamName(), TimeUtil.getDateFormat2().format(new Date()));
    }
  }

  private static <T> List<T> removeNextChunkFromList(List<T> ids, int chunkSize) {
    int toIndex = Math.min(ids.size(), chunkSize);
    List<T> chunkList = new ArrayList<>(ids.subList(0, toIndex));
    ids.removeAll(chunkList);
    return chunkList;
  }

  protected final void calculateCatalogCounts() throws IndexException {
    if (!DWQueryConfig.hasToCalculateCatalogCount()) {
      return;
    }
    Collection<CatalogEntry> catalogEntriesToIndex = getCatalogEntriesToIndex();
    IndexLogManager.info("Starting calculating catalog counts. Having to calculate counts of "
            + catalogEntriesToIndex.size() + " catalog entries", getServerID());
    if (DWQueryConfig.doParallelIndexing()) {
      processCatalogEntriesParallel(catalogEntriesToIndex, "counted");
    } else {
      calculateCounts(catalogEntriesToIndex);
    }
    commit();
    IndexLogManager.info("Finished calculating counts", getServerID());
  }

  private void calculateCounts(Collection<CatalogEntry> catalogEntriesToIndex) throws IndexException {
    int count = 0;
    for (CatalogEntry anEntry : catalogEntriesToIndex) {
      long distinctPID = calculateCount(anEntry, CountType.distinctPID);
      long distinctCaseID = calculateCount(anEntry, CountType.distinctCaseID);
      long absolute = 0; // calculateCount(anEntry, CountType.absolute);
      try {
        getCatalogManager().updateCounts(anEntry, distinctPID, distinctCaseID, absolute);
      } catch (SQLException e) {
        throw new IndexException(e);
      }
      count++;
      if (count % 1000 == 0) {
        IndexLogManager.info("Calculated counts for " + count + " catalog entries", getServerID());
      }
    }
  }

  private void processCatalogEntriesParallel(Collection<CatalogEntry> catalogEntriesToIndexSet, String process)
          throws IndexException {
    ArrayList<CatalogEntry> catalogEntriesToIndex = new ArrayList<>(catalogEntriesToIndexSet);
    int totalCount = catalogEntriesToIndex.size();
    long currentCount = 0;
    while (!catalogEntriesToIndex.isEmpty()) {
      List<CatalogEntry> chunkIDs = removeNextChunkFromList(catalogEntriesToIndex,
              DWQueryConfig.getIndexerCommitAfterDocs());
      if (process.equals("indexed"))
        chunkIDs.parallelStream().forEach(this::indexCatalogEntryWithoutException);
      else if (process.equals("counted"))
        chunkIDs.parallelStream().forEach(this::calcAndUpdateCount);
      currentCount += chunkIDs.size();
      double curPercentage = ((double) currentCount) / totalCount * 100;
      IndexLogManager.info(currentCount + "/" + totalCount + " (" + NumberFormat.getInstance().format(curPercentage)
              + "%) catalog entries " + process + " at " + TimeUtil.currentTime(), getServerID());
      commit();
    }
  }

  private void calcAndUpdateCount(CatalogEntry entry) {
    try {
      long distinctPID = calculateCount(entry, CountType.distinctPID);
      long distinctCaseID = calculateCount(entry, CountType.distinctCaseID);
      long absolute = 0; // calculateCount(anEntry, CountType.absolute);
      getCatalogManager().updateCounts(entry, distinctPID, distinctCaseID, absolute);
    } catch (SQLException | IndexException e) {
      throw new RuntimeException(e);
    }
  }

  /*
   * Index the catalog's data in the index
   */
  protected void indexCatalog() throws IndexException {
    if (!DWQueryConfig.hasToIndexCatalog() && !DWQueryConfig.hasToIndexEntireCatalog()) {
      return;
    }
    String currentTimeString = TimeUtil.getDateFormat2().format(new Timestamp(System.currentTimeMillis()));
    Collection<CatalogEntry> catalogEntriesToIndex = getCatalogEntriesToIndex();
    IndexLogManager.info(
            "Starting indexing catalog. " + catalogEntriesToIndex.size() + " catalogEntries to be indexed.",
            getServerID());
    if (DWQueryConfig.doParallelIndexing()) {
      processCatalogEntriesParallel(catalogEntriesToIndex, "indexed");
    } else {
      indexCatalog(catalogEntriesToIndex);
    }
    commit();
    saveCurrentTimeForCatalogIndexTimestampParam(currentTimeString);
    IndexLogManager.info("Finished indexing catalog", getServerID());
  }

  private void indexCatalog(Collection<CatalogEntry> catalogEntriesToIndex) throws IndexException {
    int count = 0;
    for (CatalogEntry anEntry : catalogEntriesToIndex) {
      indexCatalogEntry(anEntry);
      count++;
      if (count % 1000 == 0) {
        IndexLogManager.info("Indexed " + count + " catalog entries", getServerID());
      }
    }
  }

  private void setParam(String paramName, String paramValue) throws IndexException {
    try {
      indexParamAdapter.setParam(paramName, paramValue);
    } catch (SQLException e) {
      throw new IndexException(e);
    }
  }

  private boolean doNormalDataIndexing() {
    boolean result = DWQueryConfig.getProjectToIndex().isEmpty();
    if (DWQueryConfig.getAttrIdToIndex() != 0) {
      result = false;
    }
    if (DWQueryConfig.getPIDToIndex() != 0L) {
      result = false;
    }
    return result;
  }

  private long firstIndexTimestampTicks;

  private long indexedFactsInLastChunk = 0;

  private void logIndexingTime(long currentCount, long totalCount) {
    double curPercentage = ((double) currentCount) / totalCount * 100;
    long estimatedTimeOfTermination = firstIndexTimestampTicks
            + (long) ((System.currentTimeMillis() - firstIndexTimestampTicks) * (100 / curPercentage));

    String estimatedTimeString = TimeUtil.format(estimatedTimeOfTermination);
    IndexLogManager.info(
            currentCount + "/" + totalCount + " (" + NumberFormat.getInstance().format(curPercentage)
                    + "%) documents indexed at " + TimeUtil.currentTime() + " estimated time of termination: "
                    + estimatedTimeString + "; Last chunk contained " + indexedFactsInLastChunk + " facts",
            getServerID());
    indexedFactsInLastChunk = 0;
  }

  protected final void updateNewData() throws IndexException {
    if (!DWQueryConfig.hasToIndexData()) {
      return;
    }
    long start = System.currentTimeMillis();
    List<Long> pIDsToIndex = getPIDsToIndex();
    long totalCount = pIDsToIndex.size();
    firstIndexTimestampTicks = System.currentTimeMillis();
    Timestamp lastUpdateDate;
    if (DWQueryConfig.doParallelIndexing()) {
      lastUpdateDate = indexPIDsParallel(pIDsToIndex, totalCount);
    } else {
      lastUpdateDate = indexPIDs(pIDsToIndex, totalCount);
    }
    commit();
    updateLastUpdateTimestampIfNecessary(lastUpdateDate);
    IndexLogManager.info("All cases indexed at " + TimeUtil.currentTime(), getServerID());
    long end = System.currentTimeMillis();
    long duration = end - start;
    long durationSecs = duration / 100;
    System.out.println("Index duration: " + durationSecs + " secs.d");
  }

  private static class TimestampComparator implements Comparator<Timestamp> {

    @Override
    public int compare(Timestamp o1, Timestamp o2) {
      if (o1 == null) {
        if (o2 == null) {
          return 0;
        } else {
          return -1;
        }
      } else if (o2 == null) {
        return 1;
      } else {
        long delta = o1.getTime() - o2.getTime();
        return Long.signum(delta);
      }
    }

  }

  private void deletePIDsParallel(HashMap<Long, List<Information>> pid2infoMap) throws IndexException {
    List<Long> pIDsToDo = new ArrayList<>(pid2infoMap.keySet());
    long totalCount = pid2infoMap.size();
    long currentCount = 0;
    while (!pIDsToDo.isEmpty()) {
      List<Long> chunkIDs = removeNextChunkFromList(pIDsToDo, DWQueryConfig.getIndexerCommitAfterDocs());
      chunkIDs.parallelStream().forEach(n -> deleteDataWithoutException(n, pid2infoMap.get(n)));
      currentCount += chunkIDs.size();
      logIndexingTime(currentCount, totalCount);
      commit();
    }
  }

  private void deleteDataWithoutException(long aPID, List<Information> infos) {
    try {
      deleteData(aPID, infos);
    } catch (IndexException e) {
      throw new RuntimeException(e);
    }
  }

  private void deletePIDs(HashMap<Long, List<Information>> pid2infoMap) throws IndexException {
    long totalCount = pid2infoMap.size();
    long currentCount = 0;
    for (Long aPID : pid2infoMap.keySet()) {
      List<Information> infos = pid2infoMap.get(aPID);
      deleteData(aPID, infos);
      currentCount++;
      if (currentCount % DWQueryConfig.getIndexerCommitAfterDocs() == 0) {
        logIndexingTime(currentCount, totalCount);
        commit();
      }
    }
  }

  private Timestamp indexPIDsParallel(List<Long> pIDsToIndex, long totalCount) throws IndexException {
    List<Long> pIDsToDo = new ArrayList<>(pIDsToIndex);
    long currentCount = 0;
    Timestamp lastUpdateDate = null;
    TimestampComparator comp = new TimestampComparator();
    while (!pIDsToDo.isEmpty()) {
      List<Long> chunkIDs = removeNextChunkFromList(pIDsToDo, DWQueryConfig.getIndexerCommitAfterDocs());
      Optional<Timestamp> last = chunkIDs.parallelStream().map(this::indexDataWithoutException).max(comp);
      currentCount += chunkIDs.size();
      logIndexingTime(currentCount, totalCount);
      if (last.isPresent()) {
        lastUpdateDate = last.get();
        updateLastUpdateTimestampIfNecessary(lastUpdateDate);
      }
      commit();
    }
    return lastUpdateDate;
  }

  private Timestamp indexDataWithoutException(long patientID) {
    try {
      return indexData(patientID);
    } catch (IndexException e) {
      throw new RuntimeException(e);
    }
  }

  private Timestamp indexPIDs(List<Long> pIDsToIndex, long totalCount) throws IndexException {
    long currentCount = 0;
    Timestamp lastUpdateDate = null;
    for (Long aPID : pIDsToIndex) {
      // index the data for the respective PID
      lastUpdateDate = indexData(aPID);
      currentCount++;
      if (currentCount % DWQueryConfig.getIndexerCommitAfterDocs() == 0) {
        logIndexingTime(currentCount, totalCount);
        updateLastUpdateTimestampIfNecessary(lastUpdateDate);
        commit();
      }
    }
    logIndexingTime(currentCount, totalCount);
    updateLastUpdateTimestampIfNecessary(lastUpdateDate);
    commit();
    return lastUpdateDate;
  }

  private void updateLastUpdateTimestampIfNecessary(Timestamp lastUpdateDate) throws IndexException {
    if (doNormalDataIndexing() && (lastUpdateDate != null)) {
      // when not a specific project has been explicitly indexed, save the current timestamp
      setParam(getServerMaxUpdateDateParamName(), TimeUtil.getDateFormat2().format(lastUpdateDate));
    }
  }

  /*
   * Delete the data in the index. The update for the processed data is only done at the end of the
   * process as the facts retrieved by getDataIDsForDeletedInfo do not necessarily
   * have the correct order
   */
  protected final void updateDeletedData() throws IndexException {
    if (!DWQueryConfig.hasToIndexData()) {
      // if no data has to be indexed, also do not delete any data
      return;
    }
    if (DWQueryConfig.hasToDeleteIndex()) {
      // if the index has been deleted anyway, nothing has to be specifically deleted
      return;
    }
    if (!doNormalDataIndexing()) {
      // if a specific AttrID, PID or Project has to be indexed, don't do any deletions
      return;
    }
    if (!DWQueryConfig.processDeletedInfos()) {
      // if nothing should be deleted, then don't do it
      return;
    }
    if (DWQueryConfig.doIncrementalUpdate()) {
      // The incremental mode is experimental and does currently not support deletions (only as part of an update)
      return;
    }
    if (isFirstIndexRun) {
      // if the indexed is currently been created for the first time, there is nothing to be
      // deleted, so don't do anything
      return;
    }
    String currentTime = TimeUtil.getDateFormat2().format(new Date());
    // take all info objects that have been deleted. The objects do only bear the data PID, CaseID,
    // MeasureTime, ImportTime (which is the delete time), ref. The other fields are empty
    List<Information> deletedInfos = getDataIDsForDeletedInfo();
    // create a partition of the info objects concerning the PID
    HashMap<Long, List<Information>> pid2infoMap = new HashMap<>();
    for (Information anInfo : deletedInfos) {
      long pid = anInfo.getPid();
      if (!pid2infoMap.containsKey(pid)) {
        pid2infoMap.put(pid, new ArrayList<>());
      }
      List<Information> infos4pid = pid2infoMap.get(pid);
      infos4pid.add(anInfo);
    }
    IndexLogManager.info(pid2infoMap.size() + " IDs to be indexed due to deletion", getServerID());
    firstIndexTimestampTicks = System.currentTimeMillis();
    if (DWQueryConfig.doParallelIndexing()) {
      deletePIDsParallel(pid2infoMap);
    } else {
      deletePIDs(pid2infoMap);
    }
    commit();
    setParam(getServerMaxImportDeleteDateParamName(), currentTime);
    IndexLogManager.info("All cases with deleted infos indexed at " + TimeUtil.currentTime(), getServerID());
  }

  protected final Timestamp indexData(long dataID) throws IndexException {
    InfoIterator infoIter = getInfosByDataID(dataID);
    List<Information> infos;
    try {
      infos = infoIter.getInfos();
      infoIter.dispose();
    } catch (DWIterException e) {
      throw new IndexException(e);
    }
    indexedFactsInLastChunk += infos.size();
    Timestamp latestUpdateTimestamp = new Timestamp(0);
    if (infos.isEmpty()) {
      return latestUpdateTimestamp;
    }
    for (Information anInfo : infos.toArray(new Information[0])) {
      if (anInfo.isStorno()) {
        infos.remove(anInfo);
        continue;
      }
      if (anInfo.getUpdateTime() == null) {
        anInfo.setUpdateTime(anInfo.getImportTime());
      }
      if (anInfo.getUpdateTime().after(latestUpdateTimestamp)) {
        latestUpdateTimestamp = anInfo.getUpdateTime();
      }
      try {
        CatalogEntry anEntry = getCatalogManager().getEntryByID(anInfo.getAttrID());
        if (anEntry == null) {
          continue;
        }
        if (anEntry.getDataType() == CatalogEntryType.Structure) {
          // Structure data should not exist in the fact data. If it does, this is an import error
          infos.remove(anInfo);
        }
      } catch (SQLException e) {
        throw new IndexException(e);
      }
    }
    Patient patient = PatientStructureBuilder.buildPatient(dataID, infos);
    try {
      indexData(patient);
    } catch (Exception e) {
      IndexLogManager.info("Exception indexing id: " + dataID + " " + e, getServerID());
      throw e;
    }
    return latestUpdateTimestamp;
  }

  protected List<Group> getEntitledGroupsForDataID(long dataID) throws SQLException {
    List<Group> entitledGroups = new LinkedList<>();
    if (authManager != null) {
      for (Group group : authManager.getGroups()) {
        if (groupIsEntitledForCase(group, dataID)) {
          entitledGroups.add(group);
        }
      }
    }
    return entitledGroups;
  }

  private boolean groupIsEntitledForCase(Group group, long dataID) {
    String id = String.valueOf(dataID);
    if (group.caseWhiteListIsActive()) {
      return group.getCaseWhiteList().contains(id);
    } else {
      return !group.getCaseBlackList().contains(id);
    }
  }

  private void deleteIndexWithLogging() throws IndexException {
    IndexLogManager.info("Starting to delete the index", getServerID());
    deleteIndex();
    IndexLogManager.info("Index-deletion completed successfully", getServerID());
  }

  /*
   * Deletes the index and all its properties in the parameter table in the database
   */
  public void deleteAllData() throws IndexException {
    deleteIndexWithLogging();
    deleteMaxUpdateFlag();
    deleteMaxDeleteImportFlag();
    // when the index is recreated anyway, no deletions have to be done
    setParam(getServerMaxImportDeleteDateParamName(), TimeUtil.getDateFormat2().format(new Date()));
    isFirstIndexRun = true;
  }

  private void deleteMaxUpdateFlag() throws IndexException {
    String paramName = getServerMaxUpdateDateParamName();
    try {
      indexParamAdapter.deleteParam(paramName);
    } catch (SQLException e) {
      throw new IndexException(e);
    }
  }

  private void deleteMaxDeleteImportFlag() throws IndexException {
    String paramName = getServerMaxImportDeleteDateParamName();
    try {
      indexParamAdapter.deleteParam(paramName);
    } catch (SQLException e) {
      throw new IndexException(e);
    }
  }

  private String getServerMaxUpdateDateParamName() {
    String maxUpdateDateFlag = "Max_Update_Date";
    String serverID = getServerID();
    String indexPrefix = DWQueryConfig.getIndexPrefix();
    return indexPrefix + serverID + "_" + maxUpdateDateFlag;
  }

  private String getServerMaxCatalogIndexDateParamName() {
    String maxImportDateFlag = "Max_CatalogIndex_Date";
    String serverID = getServerID();
    String indexPrefix = DWQueryConfig.getIndexPrefix();
    return indexPrefix + serverID + "_" + maxImportDateFlag;
  }

  private String getServerMaxImportDeleteDateParamName() {
    String maxImportDateFlag = "Max_Import_Delete_Date";
    String serverID = getServerID();
    String indexPrefix = DWQueryConfig.getIndexPrefix();
    return indexPrefix + serverID + "_" + maxImportDateFlag;
  }

  private Collection<CatalogEntry> getCatalogEntriesToIndex() throws IndexException {
    Collection<CatalogEntry> result = new HashSet<>();
    try {
      if (!DWQueryConfig.getProjectToIndex().isEmpty()) {
        Collection<CatalogEntry> entriesOfProject = getCatalogManager()
                .getEntriesOfProject(DWQueryConfig.getProjectToIndex());
        for (CatalogEntry anEntry : entriesOfProject) {
          result.add(anEntry);
          result.addAll(anEntry.getAncestors());
        }
      } else if (DWQueryConfig.getAttrIdToIndex() != 0) {
        int attrID = DWQueryConfig.getAttrIdToIndex();
        CatalogEntry anEntry = getCatalogManager().getEntryByID(attrID);
        result.add(anEntry);
      } else if (DWQueryConfig.getPIDToIndex() != 0L) {
        InfoIterator iter = getInfoManager().getInfosByPID(DWQueryConfig.getPIDToIndex(), true);
        while (iter.hasNext()) {
          Information next = iter.next();
          CatalogEntry anEntry = getCatalogManager().getEntryByID(next.getAttrID());
          result.add(anEntry);
        }
        iter.dispose();
      } else if (DWQueryConfig.hasToIndexEntireCatalog() || DWQueryConfig.hasToDeleteIndex() || isFirstIndexRun) {
        result.addAll(getCatalogManager().getEntries());
      } else {
        // index only those catalogEntries that have data
        String serverMaxImportDateParam = getServerMaxCatalogIndexDateParamName();
        Timestamp timestamp = getTimeStampForParameter(serverMaxImportDateParam);
        List<Integer> ids = getInfoManager().getAttrIDsOfInfosAfterTime(timestamp);
        saveExistingEntries(result, ids);
        List<Integer> idsDeleted = getInfoManager().getAttrIDsOfDeletedInfosAfterTime(timestamp);
        saveExistingEntries(result, idsDeleted);
      }
    } catch (DWIterException | SQLException e) {
      throw new IndexException(e);
    }
    return result;
  }

  private void saveExistingEntries(Collection<CatalogEntry> result, List<Integer> ids) throws SQLException {
    for (Integer anID : ids) {
      CatalogEntry entryByID = getCatalogManager().getEntryByID(anID);
      if (entryByID != null) {
        result.add(entryByID);
        result.addAll(entryByID.getAncestors());
      }
    }
  }

  private Timestamp getLastDataUpdateDate() throws SQLException {
    String serverMaxUpdateDateParam = getServerMaxUpdateDateParamName();
    return getTimeStampForParameter(serverMaxUpdateDateParam);
  }

  private Timestamp getTimeStampForParameter(String serverMaxUpdateDateParam) throws SQLException {
    String timestampString = indexParamAdapter.getParam(serverMaxUpdateDateParam);
    Timestamp timestamp = null;
    if (timestampString != null) {
      timestamp = new Timestamp(Objects.requireNonNull(TimeUtil.parseDate(timestampString)).getTime());
    }
    return timestamp;
  }

  /*
   * Retrieve a list of all caseID which have to be indexed with their corresponding import times
   */
  private List<Long> getPIDsToIndex() throws IndexException {
    List<Long> result;
    try {
      if (!DWQueryConfig.getProjectToIndex().isEmpty()) {
        result = getInfoManager().getPIDsForProject(DWQueryConfig.getProjectToIndex());
      } else if (DWQueryConfig.getAttrIdToIndex() != 0) {
        result = getInfoManager().getPIDsForAttrID(DWQueryConfig.getAttrIdToIndex());
      } else if (DWQueryConfig.getPIDToIndex() != 0L) {
        result = new ArrayList<>();
        result.add(DWQueryConfig.getPIDToIndex());
      } else {
        Timestamp timestamp = getLastDataUpdateDate();
        result = getInfoManager().getPIDsAfterTime(timestamp);
      }
    } catch (SQLException e) {
      throw new IndexException(e);
    }
    String doIncrementalUpdate = DWQueryConfig.doIncrementalUpdate() ? " (using an incremental update)" : "";
    IndexLogManager.info(result.size() + " IDs to be indexed" + doIncrementalUpdate + ".", getServerID());
    return result;
  }

  /*
   * Retrieve a list of all PIDs which have to be re-indexed.
   * The patients have to be re-indexed because they either have been deleted or some of their data has
   * been deleted or has changed.
   */
  private List<Information> getDataIDsForDeletedInfo() throws IndexException {
    try {
      IndexLogManager.info("Calculating data IDs to delete", getServerID());
      String serverMaxImportDeleteDateParam = getServerMaxImportDeleteDateParamName();
      String timestampString = indexParamAdapter.getParam(serverMaxImportDeleteDateParam, new Timestamp(0).toString());
      Timestamp timestamp = null;
      if (timestampString != null) {
        timestamp = new Timestamp(Objects.requireNonNull(TimeUtil.parseDate(timestampString)).getTime());
      }
      return getInfoManager().getIDsAfterTimeForDeletedInfos(timestamp);
    } catch (SQLException e) {
      throw new IndexException(e);
    }
  }

  /*
   * Retrieve all data for the ID which has to be (re)indexed. Depending on the mode the index is
   * working with the dataID is either a caseID (when cases are the main data package to be indexed)
   * or a PID (when while patients are a complete data package)
   */
  private InfoIterator getInfosByDataID(long dataID) throws IndexException {
    if (dataID == 0) {
      return new FixedInfoIterator(new ArrayList<>());
    }
    try {
      if (DWQueryConfig.doIncrementalUpdate())
        return getInfoManager().getInfosByPIDAfterTime(dataID, true, getLastDataUpdateDate());
      else
        return getInfoManager().getInfosByPID(dataID, true);
    } catch (SQLException e) {
      throw new IndexException(e);
    }
  }

  protected InfoManager getInfoManager() {
    return infoManager;
  }

  protected CatalogManager getCatalogManager() {
    return catalogManager;
  }

}
