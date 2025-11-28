package de.uniwue.dw.core.sql;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.core.model.manager.InfoIterator;
import de.uniwue.dw.core.model.manager.ValueIterator;
import de.uniwue.dw.core.model.manager.adapter.IInfoAdapter;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.sql.SQLTypes;
import de.uniwue.misc.util.StringUtilsUniWue;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public abstract class SQLInfoAdapter extends DatabaseManager implements IDwSqlSchemaConstant, IInfoAdapter {

    private static final int COMMIT_AFTER_X_INFOS = 5000;

    public static final int VALUE_SHORT_LENGTH = 100;

    public abstract String getStornoNoSetQuery();

    public SQLInfoAdapter(SQLManager aSqlManager, File bulkInsertFolderIfNeeded) throws SQLException {
        super(aSqlManager);
        createSQLTables();
        setUseBulkInserts(bulkInsertFolderIfNeeded);
    }

    @Override
    public Set<String> getKeyColumnsInternal() {
        HashSet<String> keyColumns = new HashSet<String>(
                Arrays.asList(new String[]{"AttrID", "PID", "MeasureTime", "CaseID", "Ref", "DocID", "GroupID"}));
        return keyColumns;
    }

    @Override
    public Set<String> getNoUpdateColumnsInternal() {
        HashSet<String> keyColumns = new HashSet<String>(Arrays.asList(new String[]{"ImportTime"}));
        return keyColumns;
    }

    @Override
    public String getTableName() {
        return T_INFO;
    }

    @Override
    protected void readResult(ResultSet resultSet) {
    }

    private String getDeleteTrunc() {
        String command = "";
        if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
            command += "UPDATE " + getTableName() + " SET storno=1, updateTime=?";
        } else {
            command += "DELETE FROM " + getTableName();
        }
        command += " WHERE ";
        return command;
    }

    public static int setDeleteUpdateTime(PreparedStatement st, int paramOffset) throws SQLException {
        if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
            Timestamp updateDate = new Timestamp(System.currentTimeMillis());
            st.setTimestamp(paramOffset++, updateDate);
        }
        return paramOffset;
    }

    @Override
    public void deleteInfo(int attrID, long pid, Timestamp measureTime) throws SQLException {
        String command = getDeleteTrunc();
        command += "attrID=? AND pid=? AND measureTime=?";
        measureTime = SQLTypes.getMinTimestamp(measureTime);
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        paramOffset = setDeleteUpdateTime(st, paramOffset);
        st.setInt(paramOffset++, attrID);
        st.setLong(paramOffset++, pid);
        st.setString(paramOffset++, measureTime.toString());
        st.execute();
        st.close();
    }

    @Override
    public void deleteInfo(int attrID, long pID, long caseID, long docID, long groupID, Timestamp measureTime)
            throws SQLException {
        String command = getDeleteTrunc();
        command += "AttrID=? AND PID=? AND CaseID=? AND DocID=? AND GroupID=? AND measureTime=?";
        measureTime = SQLTypes.getMinTimestamp(measureTime);
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        paramOffset = setDeleteUpdateTime(st, paramOffset);
        st.setInt(paramOffset++, attrID);
        st.setLong(paramOffset++, pID);
        st.setLong(paramOffset++, caseID);
        st.setLong(paramOffset++, docID);
        st.setLong(paramOffset++, groupID);
        st.setString(paramOffset, measureTime.toString());
        st.execute();
        st.close();
    }

    public void physicallyDeleteInfo(int attrID, long pid, Timestamp measureTime) throws SQLException {
        String command = "DELETE FROM " + getTableName() + " WHERE attrID=? AND pid=? AND measureTime=?";
        measureTime = SQLTypes.getMinTimestamp(measureTime);
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setInt(paramOffset++, attrID);
        st.setLong(paramOffset++, pid);
        st.setString(paramOffset++, measureTime.toString());
        st.execute();
        st.close();
    }

    public void physicallyDeleteInfo(int attrID, long pID, long caseID, long docID, long groupID, Timestamp measureTime)
            throws SQLException {
        String command = "DELETE FROM " + getTableName() + " WHERE " +
                "AttrID=? AND PID=? AND CaseID=? AND DocID=? AND GroupID=? AND measureTime=?";
        measureTime = SQLTypes.getMinTimestamp(measureTime);
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setInt(paramOffset++, attrID);
        st.setLong(paramOffset++, pID);
        st.setLong(paramOffset++, caseID);
        st.setLong(paramOffset++, docID);
        st.setLong(paramOffset++, groupID);
        st.setString(paramOffset, measureTime.toString());
        st.execute();
        st.close();
    }

    public void physicallyDeleteInfosWithStornoFlagSet() throws SQLException {
        String command = "DELETE FROM " + getTableName() + " WHERE Storno=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        st.setInt(1, 1);
        st.execute();
        st.close();
    }

    public void deleteInfosWithoutCatalogEntry(String catalogTableName) throws SQLException {
        String command = getDeleteTrunc();
        command += "attrID NOT IN (SELECT attrID FROM " + catalogTableName + ")";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        setDeleteUpdateTime(st, 1);
        st.execute();
        st.close();
    }

    @Override
    public void deleteInfosForCaseID(long caseID) throws SQLException {
        String command = getDeleteTrunc();
        command += "caseID=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        paramOffset = setDeleteUpdateTime(st, paramOffset);
        st.setLong(paramOffset, caseID);
        st.execute();
        st.close();
    }

    @Override
    public void deleteInfosForDocID(long docID) throws SQLException {
        String command = getDeleteTrunc();
        command += "docID=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        paramOffset = setDeleteUpdateTime(st, paramOffset);
        st.setLong(paramOffset, docID);
        st.execute();
        st.close();
    }

    @Override
    public void deleteInfosForPID(long pId) throws SQLException {
        String command = getDeleteTrunc();
        command += "PID=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        paramOffset = setDeleteUpdateTime(st, paramOffset);
        st.setLong(paramOffset, pId);
        st.execute();
        st.close();
    }

    @Override
    public void deleteInfosForPIDCaseIDAndDocID(long pId, long caseID, long docID) throws SQLException {
        String command = getDeleteTrunc();
        command += "PID=? AND caseID=? AND docID=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        paramOffset = setDeleteUpdateTime(st, paramOffset);
        st.setLong(paramOffset++, pId);
        st.setLong(paramOffset++, caseID);
        st.setLong(paramOffset, docID);
        st.execute();
        st.close();
    }

    @Override
    public void deleteInfosForProjectID(String dwProject) throws SQLException {
        String command = getDeleteTrunc();
        command += "attrID IN (SELECT attrID FROM " + IDwSqlSchemaConstant.T_CATALOG + " WHERE project=?)";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        paramOffset = setDeleteUpdateTime(st, paramOffset);
        st.setString(paramOffset, dwProject);
        st.execute();
        st.close();
    }

    @Override
    public void deleteInfosWithoutCatalogEntry() throws SQLException {
        String command = getDeleteTrunc();
        command += "attrID NOT IN (SELECT attrID FROM " + IDwSqlSchemaConstant.T_CATALOG + ")";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        setDeleteUpdateTime(st, 1);
        st.execute();
        st.close();
    }

    @Override
    public void deleteInfosForAttrID(int attrId) throws SQLException {
        String command = getDeleteTrunc();
        command += "AttrID=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        paramOffset = setDeleteUpdateTime(st, paramOffset);
        st.setInt(paramOffset++, attrId);
        st.execute();
        st.close();
    }

    public void deleteInfosForProjectID(String dwProject, String catalogTableName) throws SQLException {
        String command = getDeleteTrunc();
        command += "attrID IN (SELECT attrID FROM " + catalogTableName + " WHERE project=?)";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        paramOffset = setDeleteUpdateTime(st, paramOffset);
        st.setString(paramOffset, dwProject);
        st.execute();
        st.close();
    }

    @Override
    public ValueIterator getValuesForCaseID(long caseID, int attrID) throws SQLException {
        String command = "SELECT value FROM " + getTableName() + " WHERE caseID=? AND attrID=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setLong(paramOffset++, caseID);
        st.setInt(paramOffset++, attrID);
        ResultSet resultSet = st.executeQuery();
        ValueIterator result = new SQLValueIterator(resultSet, st);
        return result;
    }

    @Override
    public ValueIterator readValues(long pid, int attrID, long ref) throws SQLException {
        String command = "SELECT value FROM " + getTableName() + " WHERE " + "pid=? AND attrID=? AND ref=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setLong(paramOffset++, pid);
        st.setLong(paramOffset++, attrID);
        st.setLong(paramOffset++, ref);
        ResultSet resultSet = st.executeQuery();
        ValueIterator result = new SQLValueIterator(resultSet, st);
        return result;
    }

    @Override
    public ValueIterator readValues(int attrID) throws SQLException {
        String command = "SELECT value FROM " + getTableName() + " WHERE attrID=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setLong(paramOffset++, attrID);
        ResultSet resultSet = st.executeQuery();
        ValueIterator result = new SQLValueIterator(resultSet, st);
        return result;
    }

    @Override
    public ValueIterator getDistinctValues(int attrID) throws SQLException {
        String command = "SELECT DISTINCT(value) FROM " + getTableName() + " WHERE attrID=?";
        command += " AND " + getStornoNoSetQuery();
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        st.setLong(1, attrID);
        ResultSet resultSet = st.executeQuery();
        ValueIterator result = new SQLValueIterator(resultSet, st);
        return result;
    }

    @Override
    public List<String> getAllValues(int attrID) throws SQLException {
        String command = "SELECT value FROM " + getTableName() + " WHERE attrID=?";
        command += " AND " + getStornoNoSetQuery();
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        st.setLong(1, attrID);
        ResultSet resultSet = st.executeQuery();
        List<String> result = new ArrayList<>();
        while (resultSet.next())
            result.add(resultSet.getString("value"));
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public double getAverageForAllValues(int attrID) throws SQLException {
        String command = "SELECT AVG(valueDec) AS 'avg' FROM " + getTableName() + " WHERE attrID=?";
        command += " AND " + getStornoNoSetQuery();
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        st.setLong(1, attrID);
        ResultSet resultSet = st.executeQuery();
        double result = 0;
        if (resultSet.next())
            result = resultSet.getDouble("avg");
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public double getMaximumForAllValues(int attrID) throws SQLException {
        String command = "SELECT MAX(valueDec) AS 'max' FROM " + getTableName() + " WHERE attrID=?";
        command += " AND " + getStornoNoSetQuery();
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        st.setLong(1, attrID);
        ResultSet resultSet = st.executeQuery();
        double result = 0;
        if (resultSet.next())
            result = resultSet.getDouble("max");
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public double getMinimumForAllValues(int attrID) throws SQLException {
        String command = "SELECT MIN(valueDec) AS 'min' FROM " + getTableName() + " WHERE attrID=?";
        command += " AND " + getStornoNoSetQuery();
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        st.setLong(1, attrID);
        ResultSet resultSet = st.executeQuery();
        double result = 0;
        if (resultSet.next())
            result = resultSet.getDouble("min");
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public Map<Integer, Long> getCounts() throws SQLException {
        Map<Integer, Long> result = new HashMap<Integer, Long>();
        String command = "SELECT AttrID, COUNT(*) c FROM " + getTableName() + " GROUP BY attrid";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        ResultSet resultSet = st.executeQuery();
        while (resultSet.next()) {
            Integer attrID = resultSet.getInt("AttrID");
            long count = resultSet.getLong("c");
            result.put(attrID, count);
        }
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public long getCount(int attrID) throws SQLException {
        return getCountBeforeMeasureTime(attrID, null);
    }

    @Override
    public long getCountBeforeMeasureTime(int attrID, Timestamp timestamp) throws SQLException {
        long result = 0L;
        String command = "SELECT COUNT(*) c FROM " + getTableName() + " WHERE attrID=?";
        command += " AND " + getStornoNoSetQuery();
        if (timestamp != null) {
            command += " AND MeasureTime < ?";
        }
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        st.setLong(1, attrID);
        if (timestamp != null) {
            st.setTimestamp(2, timestamp);
        }
        ResultSet resultSet = st.executeQuery();
        while (resultSet.next()) {
            result = resultSet.getLong("c");
        }
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public long getCountForSingleChoiceValue(int attrID, String value) throws SQLException {
        long result = 0L;
        String command = "SELECT COUNT(*) c FROM " + getTableName() + " WHERE attrID=? AND valueShort=?";
        command += " AND " + getStornoNoSetQuery();
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        st.setLong(1, attrID);
        st.setString(2, value);
        ResultSet resultSet = st.executeQuery();
        while (resultSet.next()) {
            result = resultSet.getLong("c");
        }
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public Map<Integer, Long> getCounts(Collection<Integer> attrIDs) throws SQLException {
        Map<Integer, Long> result = new HashMap<Integer, Long>();
        String command = "SELECT AttrID, COUNT(*) c FROM " + getTableName() + " WHERE attrID IN (";
        boolean first = true;
        for (Integer anAttrID : attrIDs) {
            if (first) {
                first = false;
            } else {
                command += ", ";
            }
            command += anAttrID;
        }
        command += ") GROUP BY attrid";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        ResultSet resultSet = st.executeQuery();
        while (resultSet.next()) {
            Integer attrID = resultSet.getInt("AttrID");
            long count = resultSet.getLong("c");
            result.put(attrID, count);
        }
        resultSet.close();
        st.close();
        return result;
    }

    public long getCountDistinctValues(int attrID) throws SQLException {
        long result = 0;
        String command = "SELECT COUNT(DISTINCT(valueShort)) FROM " + getTableName() + " WHERE attrID=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        st.setLong(1, attrID);
        ResultSet resultSet = st.executeQuery();
        while (resultSet.next()) {
            result = resultSet.getLong(1);
        }
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public long getCountDistinctCases(String tableName) throws SQLException {
        long result = 0;
        String command = "SELECT COUNT(DISTINCT(caseID)) FROM " + tableName;
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        ResultSet resultSet = st.executeQuery();
        while (resultSet.next()) {
            result = resultSet.getLong(1);
        }
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public long getInfoID(int attrID, long pid, Timestamp measureTime) throws SQLException {
        long result = 0;
        String command = "SELECT infoID FROM " + getTableName() + " WHERE " + "pid=? AND attrID=? AND MeasureTime=?";
        // restrict the measuretime time the earliest possible timestamp
        measureTime = SQLTypes.getMinTimestamp(measureTime);
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setLong(paramOffset++, pid);
        st.setLong(paramOffset++, attrID);
        st.setTimestamp(paramOffset++, measureTime);
        ResultSet resultSet = st.executeQuery();
        while (resultSet.next()) {
            result = resultSet.getLong("infoID");
        }
        resultSet.close();
        st.close();
        return result;
    }

    public static Information getInfo(ResultSet resultSet, boolean getValue) throws SQLException {
        Information result = new Information();
        result.setImportTime(resultSet.getTimestamp("ImportTime"));
        result.setMeasureTime(resultSet.getTimestamp("MeasureTime"));
        result.setPid(resultSet.getLong("PID"));
        result.setCaseID(resultSet.getLong("CaseID"));
        result.setInfoID(resultSet.getLong("InfoID"));
        result.setAttrID(resultSet.getInt("AttrID"));
        result.setRef(resultSet.getLong("Ref"));
        if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
            result.setDocID(resultSet.getLong("DocID"));
            result.setGroupID(resultSet.getLong("GroupID"));
            result.setStorno(resultSet.getBoolean("Storno"));
            result.setUpdateTime(resultSet.getTimestamp("UpdateTime"));
            if (result.getUpdateTime() == null) {
                // this should not be necessary, but still there are some facts in the database that have
                // null as updateTime
                result.setUpdateTime(result.getImportTime());
            }
        }
        if (getValue) {
            String value = resultSet.getString("value");
            if (value == null) {
                // the bulk inserter writes all empty strings as NULLs to the database. When there is a fact
                // it also has to have a value other than NULL. So setting that to an empty String is valid
                value = "";
            }
            result.setValue(value);
            String valueShort = getValueShort(value);
            if (valueShort == null) {
                valueShort = "";
            }
            result.setValueShort(valueShort);
            Double double1 = resultSet.getDouble("valueDec");
            if (resultSet.wasNull()) {
                double1 = null;
            }
            result.setValueDec(double1);
        }
        return result;
    }

    private static String getValueShort(String value) {
        if (value == null) {
            return null;
        }
        String valueShort = value.substring(0, Math.min(VALUE_SHORT_LENGTH - 50, value.length()));
        return valueShort;
    }

    @Override
    public Information getInfoById(long infoid) throws SQLException {
        Information result = null;
        String command = getDataSQL(true);
        command += " WHERE infoid = ?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setLong(paramOffset++, infoid);
        ResultSet resultSet = st.executeQuery();
        if (resultSet.next()) {
            result = getInfo(resultSet, true);
        }
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public InfoIterator getInfos() throws SQLException {
        String command = getDataSQL(true);
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        ResultSet resultSet = st.executeQuery();
        InfoIterator result = new SQLInfoIterator(resultSet, st);
        return result;
    }

    @Override
    public InfoIterator getInfosByAttrIDAndPIDAndCaseIDAndMeasureTime(int attrID, long pid, long caseID,
                                                                      Timestamp measureTime) throws SQLException {
        String command = getDataSQL(false);
        command += " WHERE attrID=? AND pid=? AND caseID=? AND measuretime=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setInt(paramOffset++, attrID);
        st.setLong(paramOffset++, pid);
        st.setLong(paramOffset++, caseID);
        st.setTimestamp(paramOffset++, measureTime);
        ResultSet resultSet = st.executeQuery();
        InfoIterator result = new SQLInfoIterator(resultSet, st, false);
        return result;
    }

    @Override
    public InfoIterator getInfosByAttrIDAndPIDAndCaseIDAndMeasureTimeAndDocID(int attrID, long pid, long caseID,
                                                                              Timestamp measureTime, long docID, boolean getValue) throws SQLException {
        String command = getDataSQL(getValue);
        command += " WHERE attrID=? AND pid=? AND caseID=? AND DATEADD(MICROSECOND, -DATEPART(MICROSECOND, measuretime), measuretime)=? AND docID=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setInt(paramOffset++, attrID);
        st.setLong(paramOffset++, pid);
        st.setLong(paramOffset++, caseID);
        st.setTimestamp(paramOffset++, measureTime);
        st.setLong(paramOffset++, docID);
        ResultSet resultSet = st.executeQuery();
        return new SQLInfoIterator(resultSet, st, getValue);
    }

    @Override
    public InfoIterator getInfosForProject(String project) throws SQLException {
        String command = getDataSQL(false);
        command += " WHERE attrID IN (SELECT attrID FROM " + IDwSqlSchemaConstant.T_CATALOG + " WHERE project=?)";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setString(paramOffset++, project);
        ResultSet resultSet = st.executeQuery();
        InfoIterator result = new SQLInfoIterator(resultSet, st);
        return result;
    }

    @Override
    public InfoIterator getInfosByAttrID(int attrID, boolean getValue) throws SQLException {
        String command = getDataSQL(getValue);
        command += " WHERE attrID=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setInt(paramOffset++, attrID);
        ResultSet resultSet = st.executeQuery();
        InfoIterator result = new SQLInfoIterator(resultSet, st, getValue);
        return result;
    }

    @Override
    public InfoIterator getInfosByAttrIDBetweenTime(int attrID, Timestamp min, Timestamp max) throws SQLException {
        String command = getDataSQL(true);
        command += " WHERE attrID=? AND MeasureTime>? and MeasureTime<?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setInt(paramOffset++, attrID);
        st.setTimestamp(paramOffset++, min);
        st.setTimestamp(paramOffset++, max);
        ResultSet resultSet = st.executeQuery();
        InfoIterator result = new SQLInfoIterator(resultSet, st);
        return result;
    }

    public List<Long> getInfoIDsByAttrID(int attrId) throws SQLException {
        List<Long> result = new ArrayList<Long>();
        String command = "SELECT infoID FROM " + getTableName() + " WHERE attrID=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setInt(paramOffset++, attrId);
        ResultSet resultSet = st.executeQuery();
        while (resultSet.next()) {
            long infoID = resultSet.getLong("infoID");
            result.add(infoID);
        }
        resultSet.close();
        st.close();
        return result;
    }

    private String getDataSQL(boolean getValue) throws SQLException {
        String command = "SELECT attrID, measureTime, ImportTime, pid, infoID, ref, caseID";
        if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
            command += ", docID, groupID, storno, updateTime";
        }
        if (getValue) {
            command += ", value, valueDec";
        }
        command += " FROM " + getTableName();
        return command;
    }

    @Override
    public InfoIterator getInfosByPID(long PID, boolean getValue) throws SQLException {
        return getInfosByPIDAfterTime(PID, getValue, null);
    }

    @Override
    public InfoIterator getInfosByPIDAfterTime(long PID, boolean getValue, Timestamp timestamp) throws SQLException {
        String command = getDataSQL(getValue);
        command += " WHERE PID=? AND " + getStornoNoSetQuery();
        if (timestamp != null) {
            command += " AND updateTime > ?";
        }
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setLong(paramOffset++, PID);
        if (timestamp != null) {
            st.setTimestamp(paramOffset, timestamp);
        }
        ResultSet resultSet = st.executeQuery();
        return new SQLInfoIterator(resultSet, st, getValue);
    }

    @Override
    public InfoIterator getInfosByCaseID(long caseID, boolean getValue) throws SQLException {
        String command = getDataSQL(getValue);
        command += " WHERE caseid=? AND " + getStornoNoSetQuery();
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setLong(paramOffset++, caseID);
        ResultSet resultSet = st.executeQuery();
        InfoIterator result = new SQLInfoIterator(resultSet, st, getValue);
        return result;
    }

    @Override
    public InfoIterator getInfosByCaseIDAndAttrID(long caseID, int attrID, boolean getValue) throws SQLException {
        String command = getDataSQL(getValue);
        command += " WHERE caseid=? AND attrID=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setLong(paramOffset++, caseID);
        st.setInt(paramOffset++, attrID);
        ResultSet resultSet = st.executeQuery();
        InfoIterator result = new SQLInfoIterator(resultSet, st);
        return result;
    }

    @Override
    public InfoIterator getInfosByDocIDAndAttrID(long docID, int attrID, boolean getValue) throws SQLException {
        String command = getDataSQL(getValue);
        command += " WHERE docID=? AND attrID=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setLong(paramOffset++, docID);
        st.setInt(paramOffset++, attrID);
        ResultSet resultSet = st.executeQuery();
        InfoIterator result = new SQLInfoIterator(resultSet, st);
        return result;
    }

    @Override
    public InfoIterator getInfosByPIDAndCaseIDAndDocIDAndAttrID(long pid, long caseID, long docID, int attrID,
                                                                boolean getValue) throws SQLException {
        String command = getDataSQL(getValue);
        command += " WHERE pid=? AND caseid=? AND docID=? AND attrID=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setLong(paramOffset++, pid);
        st.setLong(paramOffset++, caseID);
        st.setLong(paramOffset++, docID);
        st.setInt(paramOffset++, attrID);
        ResultSet resultSet = st.executeQuery();
        InfoIterator result = new SQLInfoIterator(resultSet, st);
        return result;
    }

    @Override
    public InfoIterator getInfosByPIDAndAttrID(long pid, int attrID, boolean getValue) throws SQLException {
        String command = getDataSQL(getValue);
        command += " WHERE pid=? AND attrID=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setLong(paramOffset++, pid);
        st.setInt(paramOffset++, attrID);
        ResultSet resultSet = st.executeQuery();
        InfoIterator result = new SQLInfoIterator(resultSet, st);
        return result;
    }

    @Override
    public InfoIterator getInfosByCaseIDAndAttrIDs(long caseID, List<Integer> attrIDs, boolean getValue)
            throws SQLException {
        String command = getDataSQL(getValue);
        command += " WHERE caseid=? AND attrID IN (";
        for (Integer anID : attrIDs) {
            command += anID + ", ";
        }
        command = command.replaceAll(", $", "");
        command += ")";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setLong(paramOffset++, caseID);
        ResultSet resultSet = st.executeQuery();
        InfoIterator result = new SQLInfoIterator(resultSet, st);
        return result;
    }

    @Override
    public List<Long> getCaseIDsByPID(long PID) throws SQLException {
        String command = "SELECT DISTINCT caseID FROM " + T_INFO + " WHERE pid=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setLong(paramOffset++, PID);
        ResultSet resultSet = st.executeQuery();
        List<Long> caseIDs = new ArrayList<>();
        while (resultSet.next()) {
            caseIDs.add(resultSet.getLong(1));
        }
        resultSet.close();
        st.close();
        return caseIDs;
    }

    @Override
    public Set<Long> getAllCaseIDs() throws SQLException {
        return getAllIDsBeforeMeasureTime(null, "caseID");
    }

    @Override
    public Set<Long> getCaseIDsBeforeMeasureTime(Timestamp timestamp) throws SQLException {
        return getAllIDsBeforeMeasureTime(timestamp, "caseID");
    }

    private Set<Long> getAllIDsBeforeMeasureTime(Timestamp timestamp, String idField) throws SQLException {
        String sql = "SELECT DISTINCT " + idField + " FROM " + IDwSqlSchemaConstant.T_INFO + " WHERE "
                + getStornoNoSetQuery();
        if (timestamp != null) {
            sql += " AND MeasureTime < ?";
        }
        PreparedStatement st = sqlManager.createPreparedStatement(sql);
        if (timestamp != null) {
            st.setTimestamp(1, timestamp);
        }
        ResultSet resultSet = st.executeQuery();
        Set<Long> result = new HashSet<Long>();
        while (resultSet.next()) {
            long id = resultSet.getLong(idField);
            result.add(id);
        }
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public Set<Long> getAllRefs() throws SQLException {
        return getAllIDsBeforeMeasureTime(null, "ref");
    }

    @Override
    public Set<Long> getRefsBeforeMeasureTime(Timestamp timestamp) throws SQLException {
        return getAllIDsBeforeMeasureTime(timestamp, "ref");
    }

    @Override
    public InfoIterator getSpecificInfosByCaseID(long caseID, String project) throws SQLException {
        return getSpecificInfosByCaseID(caseID, project, true);
    }

    @Override
    public InfoIterator getSpecificInfosByCaseID(long caseID, String project, boolean getValue) throws SQLException {
        String command = "SELECT " + T_CATALOG + ".attrID, measureTime, ImportTime, pid, infoID, ref, caseID";
        if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
            command += ", docID, groupID, storno, updateTime";
        }
        if (getValue) {
            command += ", value, valueShort, valueDec";
        }
        command += " FROM " + getTableName() + ", " + T_CATALOG + " WHERE " + getTableName() + ".attrID = " + T_CATALOG
                + ".attrID AND caseID=? AND project like ?;";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setLong(paramOffset++, caseID);
        st.setString(paramOffset++, project);
        ResultSet resultSet = st.executeQuery();
        InfoIterator result = new SQLInfoIterator(resultSet, st, getValue);
        return result;
    }

    @Override
    public Map<Long, Integer> getCountByCaseIDForAttrID(int attrID) throws SQLException {
        Map<Long, Integer> result = new HashMap<Long, Integer>();
        String command = "SELECT count(*) c, caseid FROM " + getTableName() + " WHERE attrID=? GROUP BY caseID";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setInt(paramOffset++, attrID);
        ResultSet resultSet = st.executeQuery();
        while (resultSet.next()) {
            long caseID = resultSet.getLong("caseid");
            int count = resultSet.getInt("c");
            result.put(caseID, count);
        }
        resultSet.close();
        st.close();
        return result;
    }

    public InfoIterator getInfosByAttrIDAndPID(int attrID, long pid) throws SQLException {
        String command = getDataSQL(true);
        command += " WHERE attrID=? AND PID=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setInt(paramOffset++, attrID);
        st.setLong(paramOffset++, pid);
        ResultSet resultSet = st.executeQuery();
        InfoIterator result = new SQLInfoIterator(resultSet, st);
        return result;
    }

    public InfoIterator getInfosByAttrIDsAndPID(Collection<Integer> attrIDs, long pid) throws SQLException {
        String command = getDataSQL(true);
        command += " WHERE PID=? AND attrID IN (" + StringUtilsUniWue.concatInts(attrIDs, ", ") + ")";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setLong(paramOffset++, pid);
        ResultSet resultSet = st.executeQuery();
        InfoIterator result = new SQLInfoIterator(resultSet, st);
        return result;
    }

    @Override
    public InfoIterator getInfosByCaseAndDoc(long caseid, long docid) throws SQLException {
        String command = getDataSQL(true);
        command += " WHERE CaseID=? and Ref=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        st.setLong(1, caseid);
        st.setLong(2, docid);
        ResultSet resultSet = st.executeQuery();
        InfoIterator result = new SQLInfoIterator(resultSet, st);
        return result;
    }

    @Override
    public List<Long> getRefsByAttrID(int attrID) throws SQLException {
        List<Long> result = new ArrayList<Long>();
        String command = "SELECT ref FROM " + getTableName() + " WHERE attrID=?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setInt(paramOffset++, attrID);
        ResultSet resultSet = st.executeQuery();
        while (resultSet.next()) {
            long aRef = resultSet.getLong("ref");
            result.add(aRef);
        }
        resultSet.close();
        st.close();
        return result;
    }

    private void checkInsertData(int attrID, long pid, String value, long ref, Timestamp measureTime)
            throws SQLException {
        if (measureTime == null) {
            throw new SQLException("measureTime has to be given");
        }
        if (attrID < 0) {
            throw new SQLException("negative AttrID invalid");
        }
    }

    private int counter = 0;

    @Override
    public long insert(int attrID, long pid, Double valueDecimal, String value, Timestamp measureTime, long caseID,
                       long docID, long groupID, long refID) throws SQLException {
        long result = 0;
        checkInsertData(attrID, pid, value, refID, measureTime);
        // restrict the measuretime time the earliest possible timestamp
        measureTime = SQLTypes.getMinTimestamp(measureTime);
        if (value == null) {
            value = "";
        }
        // because the field in the database can only take 14 digits before the comma, this is
        // restricted here
        if (valueDecimal > 100000000000000L) {
            valueDecimal = Double.NaN;
        }
        String valueShort = getValueShort(value);
        if (useBulkInserts() && !DwClientConfiguration.getInstance().doSQLUpdates()) {
            Timestamp importDate = new Timestamp(System.currentTimeMillis());
            Double valueDecObject = valueDecimal;
            if (Double.isNaN(valueDecimal)) {
                valueDecObject = null;
            }
            if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
                insertByBulk(0, attrID, pid, measureTime, importDate, caseID, refID, docID, groupID, value, valueShort,
                        valueDecObject, 0, importDate);
            } else {
                insertByBulk(0, attrID, pid, measureTime, importDate, caseID, refID, value, valueShort, valueDecObject);
            }
        } else {
            if (DwClientConfiguration.getInstance().doSQLUpdates()) {
                result = insertOrUpdateBySingleStatement(attrID, pid, value, valueShort, valueDecimal, refID, measureTime,
                        caseID, docID, groupID);
            } else {
                result = insertBySingleStatement(attrID, pid, value, valueShort, valueDecimal, refID, measureTime, caseID,
                        docID, groupID);
            }
        }
        counter++;
        if (counter % COMMIT_AFTER_X_INFOS == 0) {
            commit();
            System.out.print(",");
        }
        return result;
    }

    protected abstract long insertBySingleStatement(int attrID, long pid, String value, String valueShort,
                                                    double valueDecimal, long ref, Timestamp measureTime, long caseID, long docID, long groupID)
            throws SQLException;

    protected abstract long insertOrUpdateBySingleStatement(int attrID, long pid, String value, String valueShort,
                                                            double valueDecimal, long ref, Timestamp measureTime, long caseID, long docID, long groupID)
            throws SQLException;

    @Override
    public void setValueDecNull(long infoID) throws SQLException {
        String command = "UPDATE  " + getTableName() + " SET valueDec = ? WHERE infoID = ?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setNull(paramOffset++, Types.DOUBLE);
        st.setLong(paramOffset++, infoID);
        st.execute();
        st.close();
    }

    @Override
    public void setValueDec(long infoID, double valueDec) throws SQLException {
        String command = "UPDATE  " + getTableName() + " SET valueDec = ? WHERE infoID = ?";
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        int paramOffset = 1;
        st.setDouble(paramOffset++, valueDec);
        st.setLong(paramOffset++, infoID);
        st.execute();
        st.close();
    }

    public List<Long> getInfosByAttrIDs(List<Integer> attrIDsList) throws SQLException {
        List<Long> result = new LinkedList<Long>();
        String command = "SELECT InfoID FROM " + getTableName() + " WHERE ";
        int size = attrIDsList.size();
        for (int i = 0; i < size; i++) {
            if (i < size - 1) {
                command += ("AttrID=" + attrIDsList.get(i) + " OR ");
            } else {
                command += ("AttrID=" + attrIDsList.get(i) + ";");
            }
        }
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        ResultSet resultSet = st.executeQuery();
        while (resultSet.next()) {
            result.add(resultSet.getLong("InfoID"));
        }
        resultSet.close();
        st.close();
        return result;
    }

    /*
     * Insert a fact by bulk insert and immediately delete it afterwards TODO: This test does not work
     * properly. The tested info sometimes stays in the Info-table after the deletion attempt. I have
     * no clue why and how
     */
    @Override
    public void doBulkImportTest() throws SQLException, IOException {
        Timestamp importDate = new Timestamp(System.currentTimeMillis());
        if (DwClientConfiguration.getInstance().getSystemManager().getVersion() >= 2) {
            insertByBulk(0, 0, 0, importDate, importDate, 0, 0, 0, 0, "", "", 0, 0, importDate);
        } else {
            insertByBulk(0, 0, 0, importDate, importDate, 0, 0, "", "", 0);
        }
        commit();
        physicallyDeleteInfo(0, 0, importDate);
        commit();
    }

    @Override
    public Set<Integer> getAttrIDsForPIDs(Set<Long> pids) throws SQLException {
        Set<Integer> result = new HashSet<>();

        String tableName = "PIDsToExport_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
        String createTempTableCommand = "CREATE TABLE " + tableName;
        createTempTableCommand +=
                " (PID BIGINT NOT NULL, CONSTRAINT [" + tableName + "_PK] PRIMARY KEY CLUSTERED ([PID] ASC))";
        executeCommand(createTempTableCommand);

        String insertCommandStart = "INSERT INTO " + tableName + " VALUES ";
        int numPatientsPerInsertCommand = 500;
        for (int i = 0; i <= pids.size() / numPatientsPerInsertCommand; i++) {
            Set<Long> currentPIDs = pids.stream().skip(i * numPatientsPerInsertCommand).limit(numPatientsPerInsertCommand)
                    .collect(Collectors.toSet());
            if (currentPIDs.isEmpty()) {
                break;
            }
            String insertCommand = insertCommandStart;
            for (Long aPID : currentPIDs) {
                if (insertCommand.endsWith(")")) {
                    insertCommand += ", ";
                }
                insertCommand += "(" + aPID + ")";
            }
            executeCommand(insertCommand);
        }

        String sql = "SELECT distinct AttrID FROM " + IDwSqlSchemaConstant.T_INFO;
        sql += " WHERE PID IN (SELECT PID FROM " + tableName + ") AND " + getStornoNoSetQuery();
        PreparedStatement st = sqlManager.createPreparedStatement(sql);
        ResultSet resultSet = st.executeQuery();
        while (resultSet.next()) {
            result.add(resultSet.getInt("AttrID"));
        }
        resultSet.close();
        st.close();

        String dropTableCommand = "DROP TABLE " + tableName;
        executeCommand(dropTableCommand);
        return result;
    }

    @Override
    public Set<Integer> getAttrIDsForCaseID(Long caseID) throws SQLException {
        String sql = "SELECT distinct AttrID FROM " + IDwSqlSchemaConstant.T_INFO;
        sql += " WHERE CaseID = ? AND " + getStornoNoSetQuery();
        PreparedStatement st = sqlManager.createPreparedStatement(sql);
        st.setLong(1, caseID);
        ResultSet resultSet = st.executeQuery();
        Set<Integer> result = new HashSet<>();
        while (resultSet.next()) {
            result.add(resultSet.getInt("AttrID"));
        }
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public List<Integer> getAttrIDsOfInfosAfterTime(Timestamp timestamp) throws SQLException {
        List<Integer> result = new ArrayList<Integer>();
        String sql = "SELECT distinct attrID FROM " + IDwSqlSchemaConstant.T_INFO + " i ";
        sql += "WHERE " + getStornoNoSetQuery();
        if (timestamp != null) {
            sql += " AND i.updateTime >= ?";
        }
        PreparedStatement st = sqlManager.createPreparedStatement(sql);
        int paramOffset = 1;
        if (timestamp != null) {
            st.setTimestamp(paramOffset++, timestamp);
        }
        ResultSet resultSet = st.executeQuery();
        while (resultSet.next()) {
            int id = resultSet.getInt("attrID");
            result.add(id);
        }
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public List<Integer> getAttrIDsOfSingleChoicesWithDataAsWellAsTheirChildren() throws SQLException {
        List<Integer> result = new ArrayList<Integer>();
        String sql = "select a.a1 from (\n" + "select distinct(c1.attrid) a1 from dwinfo i1, dwcatalog c1 where \n"
                + "c1.DataType = 'singlechoice' and c1.AttrID = i1.AttrID\n" + ") a,\n"
                + "(select distinct(c1.attrid) b1 from dwinfo i2, dwcatalog c2, dwcatalog c1 where \n"
                + "c1.DataType = 'singlechoice' and c2.ParentID = c1.AttrID and c2.AttrID = i2.AttrID\n" + ") b\n"
                + "where a.a1 = b.b1";
        PreparedStatement st = sqlManager.createPreparedStatement(sql);
        ResultSet resultSet = st.executeQuery();
        while (resultSet.next()) {
            int id = resultSet.getInt("a1");
            result.add(id);
        }
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public List<Long> getPIDsAfterTime(Timestamp timestamp) throws SQLException {
        String sql = "SELECT pid FROM " + IDwSqlSchemaConstant.T_INFO + " i";
        sql += " WHERE " + getStornoNoSetQuery();
        if (timestamp != null) {
            sql += " AND i.updateTime > ?";
        }
        sql += " GROUP BY i.pid ORDER BY MAX(UpdateTime)";
        PreparedStatement st = sqlManager.createPreparedStatement(sql);
        int paramOffset = 1;
        if (timestamp != null) {
            st.setTimestamp(paramOffset++, timestamp);
        }
        ResultSet resultSet = st.executeQuery();
        List<Long> result = new ArrayList<Long>();
        while (resultSet.next()) {
            long pid = resultSet.getLong("pid");
            result.add(pid);
        }
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public Set<Long> getAllPIDs() throws SQLException {
        return getAllIDsBeforeMeasureTime(null, "pid");
    }

    @Override
    public Long getPIDByDocID(long docID) throws SQLException {
        String sql = "SELECT pid FROM " + IDwSqlSchemaConstant.T_INFO + " i WHERE i.docID = ?";
        PreparedStatement st = sqlManager.createPreparedStatement(sql);
        int paramOffset = 1;
        st.setLong(paramOffset++, docID);
        ResultSet resultSet = st.executeQuery();
        Long result = null;
        if (resultSet.next()) {
            result = resultSet.getLong("pid");
        }
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public Long getPIDByCaseID(long caseID) throws SQLException {
        String sql = "SELECT pid FROM " + IDwSqlSchemaConstant.T_INFO + " i WHERE i.caseID = ?";
        PreparedStatement st = sqlManager.createPreparedStatement(sql);
        int paramOffset = 1;
        st.setLong(paramOffset++, caseID);
        ResultSet resultSet = st.executeQuery();
        Long result = null;
        if (resultSet.next()) {
            result = resultSet.getLong("pid");
        }
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public Set<Long> getPIDsBeforeMeasureTime(Timestamp timestamp) throws SQLException {
        return getAllIDsBeforeMeasureTime(timestamp, "pid");
    }

    @Override
    public List<Long> getPIDsForProject(String project) throws SQLException {
        // this method is implemented with a "Statement" instead of a "PrepearedStatement" because the
        // MSSQL-Server somehow breaks the execution plan and the PreparedStatement is 1000x slower than
        // the Statement
        String sql = "SELECT pid FROM " + IDwSqlSchemaConstant.T_INFO + " i, " + IDwSqlSchemaConstant.T_CATALOG
                + " c WHERE i.attrID = c.attrID AND c.project = '" + project.replaceAll("'", "''") + "' GROUP BY i.pid";
        Statement st = sqlManager.createStatement();
        ResultSet resultSet = st.executeQuery(sql);
        List<Long> result = new ArrayList<Long>();
        while (resultSet.next()) {
            long pid = resultSet.getLong("pid");
            result.add(pid);
        }
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public List<Long> getPIDsForAttrID(int attrid) throws SQLException {
        String sql = "SELECT pid FROM " + IDwSqlSchemaConstant.T_INFO + " i, " + IDwSqlSchemaConstant.T_CATALOG
                + " c WHERE i.attrID = c.attrID AND c.attrid = ? GROUP BY i.pid";
        PreparedStatement st = sqlManager.createPreparedStatement(sql);
        int paramOffset = 1;
        st.setInt(paramOffset++, attrid);
        ResultSet resultSet = st.executeQuery();
        List<Long> result = new ArrayList<Long>();
        while (resultSet.next()) {
            long pid = resultSet.getLong("pid");
            result.add(pid);
        }
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public long getMaxPID() throws SQLException {
        return getMaxID("pid");
    }

    @Override
    public long getMaxCaseID() throws SQLException {
        return getMaxID("caseID");
    }

    @Override
    public long getMaxRef() throws SQLException {
        return getMaxID("ref");
    }

    private long getMaxID(String columnName) throws SQLException {
        long result = 0;
        String sql = "SELECT MAX(" + columnName + ") AS MAX_ID FROM " + IDwSqlSchemaConstant.T_INFO;
        PreparedStatement st = sqlManager.createPreparedStatement(sql);
        ResultSet resultSet = st.executeQuery();
        if (resultSet.next()) {
            result = resultSet.getLong("MAX_ID");
        }
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public long getNumOfPatients() throws SQLException {
        return getTotalCountBeforeMeasureTime(null, "pid");
    }

    @Override
    public long getNumOfPatientsBeforeMeasureTime(Timestamp timestamp) throws SQLException {
        return getTotalCountBeforeMeasureTime(timestamp, "pid");
    }

    @Override
    public long getNumOfCases() throws SQLException {
        return getTotalCountBeforeMeasureTime(null, "caseID");
    }

    @Override
    public long getNumOfCasesBeforeMeasureTime(Timestamp timestamp) throws SQLException {
        return getTotalCountBeforeMeasureTime(timestamp, "caseID");
    }

    @Override
    public long getNumOfRefs() throws SQLException {
        return getTotalCountBeforeMeasureTime(null, "ref");
    }

    @Override
    public long getNumOfRefsBeforeMeasureTime(Timestamp timestamp) throws SQLException {
        return getTotalCountBeforeMeasureTime(timestamp, "ref");
    }

    @Override
    public long getNumOfInfos() throws SQLException {
        return getTotalCountBeforeMeasureTime(null, null);
    }

    @Override
    public long getNumOfInfosBeforeMeasureTime(Timestamp timestamp) throws SQLException {
        return getTotalCountBeforeMeasureTime(timestamp, null);
    }

    private long getTotalCountBeforeMeasureTime(Timestamp timestamp, String columnName) throws SQLException {
        long result = 0;
        String countString = "*";
        if (columnName != null) {
            countString = "DISTINCT " + columnName;
        }
        String command = "SELECT COUNT(" + countString + ") FROM " + getTableName();
        command += " WHERE " + getStornoNoSetQuery();
        if (timestamp != null) {
            command += " AND MeasureTime < ?";
        }
        PreparedStatement st = sqlManager.createPreparedStatement(command);
        if (timestamp != null) {
            st.setTimestamp(1, timestamp);
        }
        ResultSet resultSet = st.executeQuery();
        while (resultSet.next()) {
            result = resultSet.getLong(1);
        }
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public List<Integer> getAttrIDsOfDeletedInfosAfterTime(Timestamp timestamp) throws SQLException {
        List<Integer> result = new ArrayList<Integer>();
        String sql = "SELECT DISTINCT attrID FROM " + getTableName() + " WHERE storno = ?";
        if (timestamp != null) {
            sql += " AND updateTime >= ?";
        }
        PreparedStatement st = sqlManager.createPreparedStatement(sql);
        int paramOffset = 1;
        st.setInt(paramOffset++, 1);
        if (timestamp != null) {
            st.setTimestamp(paramOffset++, timestamp);
        }
        ResultSet resultSet = st.executeQuery();
        while (resultSet.next()) {
            int id = resultSet.getInt("attrid");
            result.add(id);
        }
        resultSet.close();
        st.close();
        return result;
    }

    @Override
    public List<Information> getIDsAfterTimeForDeletedInfos(Timestamp timestamp) throws SQLException {
        String sql = "SELECT pid, caseid FROM " + getTableName() + " WHERE storno = ?";
        if (timestamp != null) {
            sql += " AND updateTime >= ?";
        }
        PreparedStatement st = sqlManager.createPreparedStatement(sql);
        int paramOffset = 1;
        st.setInt(paramOffset++, 1);
        if (timestamp != null) {
            st.setTimestamp(paramOffset++, timestamp);
        }
        ResultSet resultSet = st.executeQuery();
        List<Information> result = new ArrayList<Information>();
        while (resultSet.next()) {
            Information anInfo = new Information();
            anInfo.setPid(resultSet.getLong("PID"));
            anInfo.setCaseID(resultSet.getLong("CaseID"));
            result.add(anInfo);
        }
        resultSet.close();
        st.close();
        return result;
    }

}
