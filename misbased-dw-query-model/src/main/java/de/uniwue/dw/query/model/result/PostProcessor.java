package de.uniwue.dw.query.model.result;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.*;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;

import java.sql.Timestamp;
import java.util.*;

public class PostProcessor implements IPostProcessor {

  private static Calendar cal = Calendar.getInstance();

  private QueryRunnable queryRunnable;

  PostProcessor(QueryRunnable queryRunnable) {
    this.queryRunnable = queryRunnable;
  }

  @Override
  public List<List<ResultCellData>> doPostProcessing(List<ResultCellData> cellData)
          throws NumberFormatException, QueryException {
    // if (cellData.size() > 0) {
    // System.out.print(cellData.get(0).getValue().getPid() + " ");
    // }
    List<List<ResultCellData>> resultRows = new ArrayList<>();
    resultRows.add(cellData);
    checkMultipleRows(resultRows);
    checkAttributeConstraints(resultRows);
    checkIDFilterGroups(resultRows);
    checkTempOpsRel(resultRows);
    checkTempOpsAbs(resultRows);
    checkValueCompareOps(resultRows);
    checkDistincts(resultRows);
    checkAnys(resultRows);
    // if (cellData.size() > 0) {
    // System.out.println(resultRows.size() + " ");
    // }
    return resultRows;
  }

  /*
   * For every cell with multiple values but an attributes that wished for only one value ("ANY"),
   * leave all but the first one
   */
  private void checkAnys(List<List<ResultCellData>> resultRows) {
    for (List<ResultCellData> aRow : resultRows) {
      for (ResultCellData aData : aRow) {
        if ((aData.attribute.getReductionOperator() == ReductionOperator.ANY)
                && aData.hasMoreThanOneValue()) {
          boolean first = true;
          for (Information anInfo : aData.getValues().toArray(new Information[0])) {
            if (first) {
              first = false;
              continue;
            }
            aData.removeValue(anInfo);
          }
        }
      }
    }
  }

  private void checkAttributeConstraint(ResultCellData aData, Information anInfo,
          QueryAttribute attribute) throws NumberFormatException, QueryException {
    double desiredContentDouble = 0;
    double desiredContentUpperBound = 0;
    double valueDouble = 0;
    if (attribute.isOnlyDisplayExistence()) {
      // we cannot check the value because the underlying engine possibly did not return the actual
      // value
      return;
    }
    if (attribute.getContentOperator() == ContentOperator.EXISTS) {
      return;
    }
    if (attribute.getCatalogEntry().getDataType() == CatalogEntryType.Number) {
      if (attribute.getContentOperator() == ContentOperator.BETWEEN) {
        desiredContentDouble = attribute.getDesiredContentBetweenLowerBoundDouble();
        desiredContentUpperBound = attribute.getDesiredContentBetweenUpperBoundDouble();
      } else {
        desiredContentDouble = attribute.getDesiredContentDouble();
      }
      valueDouble = anInfo.getValueDec();
    } else if (attribute.getCatalogEntry().getDataType() == CatalogEntryType.DateTime) {
      if (attribute.getContentOperator() == ContentOperator.BETWEEN) {
        desiredContentDouble = attribute.getDesiredContentBetweenLowerBoundDate().getTime();
        desiredContentUpperBound = attribute.getDesiredContentBetweenUpperBoundDate().getTime();
      } else {
        desiredContentDouble = attribute.getDesiredContentDate().getTime();
      }
      valueDouble = anInfo.getValueDate().getTime();
    }
    if (attribute.getContentOperator() == ContentOperator.MORE) {
      if (valueDouble <= desiredContentDouble) {
        aData.removeValue(anInfo);
      }
    } else if (attribute.getContentOperator() == ContentOperator.MORE_OR_EQUAL) {
      if (valueDouble < desiredContentDouble) {
        aData.removeValue(anInfo);
      }
    } else if (attribute.getContentOperator() == ContentOperator.LESS) {
      if (valueDouble >= desiredContentDouble) {
        aData.removeValue(anInfo);
      }
    } else if (attribute.getContentOperator() == ContentOperator.LESS_OR_EQUAL) {
      if (valueDouble > desiredContentDouble) {
        aData.removeValue(anInfo);
      }
    } else if (attribute.getContentOperator() == ContentOperator.BETWEEN) {
      if ((valueDouble < desiredContentDouble) || (valueDouble > desiredContentUpperBound)) {
        aData.removeValue(anInfo);
      }
    } else if (attribute.getContentOperator() == ContentOperator.EQUALS) {
      if ((attribute.getCatalogEntry().getDataType() == CatalogEntryType.Number)
              || (attribute.getCatalogEntry().getDataType() == CatalogEntryType.DateTime)) {
        if (valueDouble != desiredContentDouble) {
          aData.removeValue(anInfo);
        }
      }
      // text isn't handled here !
    }
  }

  private void checkAttributeConstraints(List<List<ResultCellData>> resultRows)
          throws NumberFormatException, QueryException {
    List<List<ResultCellData>> result = new ArrayList<>();
    for (List<ResultCellData> aRow : resultRows) {
      boolean takeRow = true;
      for (ResultCellData aData : aRow) {
        QueryAttribute attribute = aData.attribute;
        Information[] valueArray = aData.getValues().toArray(new Information[0]);
        for (Information anInfo : valueArray) {
          checkAttributeConstraint(aData, anInfo, attribute);
        }
        if (aData.getValues().isEmpty() && !attribute.isOptional() &&
                !attribute.getParent().getClass().equals(QueryOr.class) &&
                !(attribute.getContentOperator() == ContentOperator.NOT_EXISTS) &&
                !(attribute.getContentOperator() == ContentOperator.CONTAINS_NOT) &&
                !(attribute.getContentOperator() == ContentOperator.CONTAINS_NOT_POSITIVE)) {
          takeRow = false;
        }
      }
      if (takeRow) {
        result.add(aRow);
      }
    }
    resultRows.clear();
    resultRows.addAll(result);
  }

  private void checkMultipleRows(List<List<ResultCellData>> originalRows) {
    List<List<ResultCellData>> result = new ArrayList<>(originalRows);
    for (QueryAttribute anAttr : getQueryRoot().getAttributesRecursive()) {
      List<List<ResultCellData>> tmpResult = new ArrayList<>();
      if (anAttr.hasToBeMadeMultipleRows()) {
        // take all rows
        for (List<ResultCellData> aRow : result) {
          HashMap<QueryAttribute, ResultCellData> attr2CellDataMap = getAttrMap(aRow);
          ResultCellData cellToMultiply = attr2CellDataMap.get(anAttr);
          if (cellToMultiply.getValues().size() == 0) {
            // if there are no values for the cell to multiply, still take that row
            tmpResult.add(aRow);
          } else {
            // copy all other CellDatas and distribute the one with the multipleRows
            for (Information anInfo : cellToMultiply.getValues()) {
              List<ResultCellData> newRow = new ArrayList<>();
              for (ResultCellData aData : aRow) {
                ResultCellData newData;
                if (!aData.attribute.equals(anAttr)) {
                  newData = new ResultCellData(aData);
                } else {
                  newData = new ResultCellData(anInfo, anAttr);
                }
                newRow.add(newData);
              }
              tmpResult.add(newRow);
            }
          }
        }
        result.clear();
        result.addAll(tmpResult);
      }
    }
    if (result.size() > 0) { // otherwise alle attribute were not set to mulitpleRows
      originalRows.clear();
      originalRows.addAll(result);
    }
  }

  private QueryRoot getQueryRoot() {
    return queryRunnable.getQueryRoot();
  }

  private HashMap<QueryAttribute, ResultCellData> getAttrMap(List<ResultCellData> cellData) {
    HashMap<QueryAttribute, ResultCellData> attr2CellDataMap = new HashMap<>();
    for (ResultCellData aData : cellData) {
      attr2CellDataMap.put(aData.attribute, aData);
    }
    return attr2CellDataMap;
  }

  private void checkValueCompareOps(List<List<ResultCellData>> originalRows) {
    List<List<ResultCellData>> result = new ArrayList<>();
    for (List<ResultCellData> aRow : originalRows) {
      boolean takeRow = getQueryRoot().getValueComparesRecursive().isEmpty();
      HashMap<QueryAttribute, ResultCellData> attr2CellDataMap = getAttrMap(aRow);
      for (QueryAttribute queryAttribute : getQueryRoot().getAttributesRecursive()) {
        for (QueryValueCompare aValueComp : queryAttribute.getValueCompares()) {
          boolean checkValueCompare = checkValueCompare(aValueComp, attr2CellDataMap);
          if (checkValueCompare) {
            takeRow = true;
            break;
          }
        }
      }
      if (takeRow) {
        result.add(aRow);
      }
    }
    originalRows.clear();
    originalRows.addAll(result);
  }

  private boolean checkValueCompare(QueryValueCompare aValueComp,
          HashMap<QueryAttribute, ResultCellData> attr2CellDataMap) {
    QueryAttribute myAttr = (QueryAttribute) aValueComp.getParent();
    ResultCellData myData = attr2CellDataMap.get(myAttr);
    ResultCellData refData = attr2CellDataMap.get(aValueComp.refElem);
    boolean result = false;
    Set<Integer> myDataIDsToKeep = new HashSet<>();
    Set<Integer> refDataIDsToKeep = new HashSet<>();
    for (int i = 0; i < myData.getValues().size(); i++) {
      Information myInfo = myData.getValues().get(i);
      for (int j = 0; j < refData.getValues().size(); j++) {
        Information refInfo = refData.getValues().get(j);
        if (aValueComp.getContentOperator() == ContentOperator.MORE) {
          if (myInfo.getValueDec() > refInfo.getValueDec()) {
            result = true;
            myDataIDsToKeep.add(i);
            refDataIDsToKeep.add(j);
          }
        } else if (aValueComp.getContentOperator() == ContentOperator.LESS) {
          if (myInfo.getValueDec() < refInfo.getValueDec()) {
            result = true;
            myDataIDsToKeep.add(i);
            refDataIDsToKeep.add(j);
          }
        } else if (aValueComp.getContentOperator() == ContentOperator.MORE_OR_EQUAL) {
          if (myInfo.getValueDec() >= refInfo.getValueDec()) {
            result = true;
            myDataIDsToKeep.add(i);
            refDataIDsToKeep.add(j);
          }
        } else if (aValueComp.getContentOperator() == ContentOperator.LESS_OR_EQUAL) {
          if (myInfo.getValueDec() <= refInfo.getValueDec()) {
            result = true;
            myDataIDsToKeep.add(i);
            refDataIDsToKeep.add(j);
          }
        } else if (aValueComp.getContentOperator() == ContentOperator.EQUALS) {
          if (myInfo.getValueDec().equals(refInfo.getValueDec())) {
            result = true;
            myDataIDsToKeep.add(i);
            refDataIDsToKeep.add(j);
          }
        }
      }
    }

    if (result) {
      deleteNotMatchingInfos(myData, myDataIDsToKeep);
      deleteNotMatchingInfos(refData, refDataIDsToKeep);
    }

    return result;
  }

  private void deleteNotMatchingInfos(ResultCellData resultCellData, Set<Integer> myDataIDsToKeep) {
    for (int i = resultCellData.getValues().size() - 1; i >= 0; i--) {
      if (!myDataIDsToKeep.contains(i)) {
        resultCellData.getValues().remove(i);
      }
    }
  }

  private void checkTempOpsAbs(List<List<ResultCellData>> resultRows) {
    List<List<ResultCellData>> result = new ArrayList<>();
    for (List<ResultCellData> aRow : resultRows) {
      boolean takeRow = getQueryRoot().getTempOpsAbsRecursive().isEmpty();
      HashMap<QueryAttribute, ResultCellData> attr2CellDataMap = getAttrMap(aRow);
      for (QueryAttribute queryAttribute : getQueryRoot().getAttributesRecursive()) {
        for (QueryTempOpAbs aTempOpAbs : queryAttribute.getTempOpsAbs()) {
          ResultCellData cellDataForAttr = attr2CellDataMap.get(queryAttribute);
          boolean checkTempOpAbs = checkTempOpAbs(aTempOpAbs, cellDataForAttr);
          if (checkTempOpAbs || queryAttribute.isOptional()) {
            takeRow = true;
            break;
          }
        }
      }
      if (takeRow) {
        result.add(aRow);
      }
    }
    resultRows.clear();
    resultRows.addAll(result);
  }

  private boolean checkTempOpAbs(QueryTempOpAbs aTempAbsOp, ResultCellData cellDataForAttr) {
    for (Information myInfo : cellDataForAttr.getValues()) {
      Timestamp myMeasureTime = myInfo.getMeasureTime();
      if (aTempAbsOp.absMinDate == null)
        return myMeasureTime.before(aTempAbsOp.absMaxDate);
      else if (aTempAbsOp.absMaxDate == null)
        return myMeasureTime.after(aTempAbsOp.absMinDate);
      else
        return myMeasureTime.after(aTempAbsOp.absMinDate) && myMeasureTime.before(aTempAbsOp.absMaxDate);
    }
    return false;
  }

  private void checkTempOpsRel(List<List<ResultCellData>> resultRows) {
    List<List<ResultCellData>> result = new ArrayList<>();
    for (List<ResultCellData> aRow : resultRows) {
      boolean takeRow = getQueryRoot().getTempOpsRelRecursive().isEmpty();
      HashMap<QueryAttribute, ResultCellData> attr2CellDataMap = getAttrMap(aRow);
      for (QueryAttribute queryAttribute : getQueryRoot().getAttributesRecursive()) {
        for (QueryTempOpRel aTempOpRel : queryAttribute.getTemporalOpsRel()) {
          boolean checkTempOpRel = checkTempOpRel(aTempOpRel, attr2CellDataMap);
          if (checkTempOpRel) {
            takeRow = true;
            break;
          }
        }
      }
      if (takeRow) {
        result.add(aRow);
      }
    }
    resultRows.clear();
    resultRows.addAll(result);
  }

  private boolean checkTempOpRel(QueryTempOpRel aTempRelOp,
          HashMap<QueryAttribute, ResultCellData> attr2CellDataMap) {
    QueryAttribute refElem = aTempRelOp.getRefElem();
    QueryAttribute queryElem = (QueryAttribute) aTempRelOp.getParent();
    ResultCellData myData = attr2CellDataMap.get(queryElem);
    ResultCellData relData = attr2CellDataMap.get(refElem);
    List<Information> myValues = myData.getValues();
    List<Information> relValues = relData.getValues();
    if ((myValues.size() == 0) || (relValues.size() == 0)) {
      // if there are no values on either side of the relation it could
      // mean that the side is
      // wrapped in an OR-clause. In this case there may be no result for
      // the side but the
      // relTemp-parent_shell is still full-filled.
      // This should be checked !
      return true;
    }
    Set<Integer> myValuesIDsToKeep = new HashSet<>();
    Set<Integer> relValuesIDsToKeep = new HashSet<>();
    boolean result = false;
    for (int i = 0; i < myValues.size(); i++) {
      Information myInfo = myValues.get(i);
      for (int j = 0; j < relValues.size(); j++) {
        Information relInfo = relValues.get(j);
        // if its the same fact instance, skip it
        if ((myInfo.getAttrID() == relInfo.getAttrID())
                && (myInfo.getCaseID() == relInfo.getCaseID())
                && (myInfo.getRef() == relInfo.getRef())
                && (myInfo.getMeasureTime().getTime() == relInfo.getMeasureTime().getTime())) {
          continue;
        }
        Timestamp myMeasureTime = myInfo.getMeasureTime();
        Timestamp relMeasureTime = relInfo.getMeasureTime();
        Date myDate = new Date(myMeasureTime.getTime());
        cal.setTimeInMillis(relMeasureTime.getTime());
        cal.add(Calendar.YEAR, aTempRelOp.getYearShiftMin());
        cal.add(Calendar.MONTH, aTempRelOp.getMonthShiftMin());
        cal.add(Calendar.DAY_OF_YEAR, aTempRelOp.getDayShiftMin());
        if (!aTempRelOp.hasMinBoundary()) {
          cal.add(Calendar.YEAR, -100);
        }
        Date minDate = cal.getTime();
        cal.setTimeInMillis(relMeasureTime.getTime());
        cal.add(Calendar.YEAR, aTempRelOp.getYearShiftMax());
        cal.add(Calendar.MONTH, aTempRelOp.getMonthShiftMax());
        cal.add(Calendar.DAY_OF_YEAR, aTempRelOp.getDayShiftMax());
        if (!aTempRelOp.hasMaxBoundary()) {
          cal.add(Calendar.YEAR, 100);
        }
        Date maxDate = cal.getTime();
        if ((myDate.after(minDate) || myDate.equals(minDate))
                && (myDate.before(maxDate) || myDate.equals(maxDate))) {
          result = true;
          myValuesIDsToKeep.add(i);
          relValuesIDsToKeep.add(j);
        }
      }
    }
    if (result) {
      deleteNotMatchingInfos(myData, myValuesIDsToKeep);
      deleteNotMatchingInfos(relData, relValuesIDsToKeep);
    }
    return result;
  }

  private void checkIDFilterGroups(List<List<ResultCellData>> resultRows) {
    List<List<ResultCellData>> result = new ArrayList<>();
    for (List<ResultCellData> aRow : resultRows) {
      List<List<ResultCellData>> currentRows = new ArrayList<>();
      currentRows.add(aRow);
      for (QueryIDFilter aFilter : getQueryRoot().getIDFilterRecursive()) {
        // TODO: process fixed IDs and times for IDFilter
        if (aFilter.getFilterIDType() == FilterIDType.PID) {
          // all ResultRows are yet anyway grouped by PID. This can
          // only be a filter using fixedIDS
          // or times
          continue;
        }
        if (aFilter.getFilterIDType() == FilterIDType.CaseID) {
          // TODO: fix this later!
          continue;
        }
        List<List<ResultCellData>> newRows = new ArrayList<>();
        for (List<ResultCellData> aCellDataRow : currentRows) {
          List<List<ResultCellData>> newRowsToAdd = getIDFilterGroups(aFilter, aCellDataRow);
          newRows.addAll(newRowsToAdd);
        }
        currentRows = newRows;
      }
      result.addAll(currentRows);
    }
    resultRows.clear();
    resultRows.addAll(result);
  }

  private List<List<ResultCellData>> createNewRowsFromPartitions(
          HashMap<Long, HashMap<QueryAttribute, ResultCellData>> resultSets,
          List<ResultCellData> cellData) {
    // create new resultRows for the partitions that have been created
    List<List<ResultCellData>> newRows = new ArrayList<>();
    for (HashMap<QueryAttribute, ResultCellData> aResultSet : resultSets.values()) {
      List<ResultCellData> newRow = new ArrayList<>();
      for (ResultCellData anOrigCellData : cellData) {
        QueryAttribute anAttr = anOrigCellData.attribute;
        ResultCellData aResultCell = aResultSet.get(anAttr);
        if (aResultCell == null) {
          aResultCell = anOrigCellData;
        }
        newRow.add(new ResultCellData(aResultCell));
      }
      newRows.add(newRow);
    }
    return newRows;
  }

  private void checkDistincts(List<List<ResultCellData>> newRows) {
    checkDistinct(null, newRows);
    for (QueryIDFilter aFilter : getQueryRoot().getIDFilterRecursive()) {
      checkDistinct(aFilter, newRows);
    }
  }

  private void checkDistinct(QueryIDFilter queryIDFilter, List<List<ResultCellData>> newRows) {
    if ((queryIDFilter == null && getQueryRoot().isDistinct()) ||
            (queryIDFilter != null && queryIDFilter.isDistinct())) {
      Map<Long, ResultCellData> idsToCellDataMap = new HashMap<>();
      Map<Long, Row> idsInProcessedRowsToRowMap = new HashMap<>();
      if (newRows.isEmpty() || newRows.get(0).isEmpty())
        return;
      QueryAttribute queryAttribute = newRows.get(0).get(0).attribute;
      for (Row aRow : queryRunnable.getResult().getRows()) {
        for (Cell aCell : aRow.getCells()) {
          if (aCell.getCellData().attribute == null)
            continue;
          if (aCell.getCellData().attribute.equals(queryAttribute)) {
            long id = getCurrentID(queryIDFilter, aCell.getCellData());
            if (id == 0)
              continue;
            idsToCellDataMap.put(id, aCell.getCellData());
            idsInProcessedRowsToRowMap.put(id, aRow);
            break;
          }
        }
      }
      for (List<ResultCellData> aNewRow : newRows.toArray(new List[0])) {
        ResultCellData value = null;
        for (ResultCellData data : aNewRow) {
          if (data.attribute.equals(queryAttribute)) {
            value = data;
            break;
          }
        }
        if (value == null)
          return;
        if (value.hasMoreThanOneValue()) {
          Information infoToKeep = null;
          for (Information anInfo : value.getValues()) {
            if (infoToKeep == null)
              infoToKeep = anInfo;
            else if (isNewValueSmaller(value.attribute.getCatalogEntry(), infoToKeep, value.attribute.getCatalogEntry(),
                    anInfo))
              infoToKeep = anInfo;
          }
          value.removeAllValues();
          value.addValue(infoToKeep);
        }
        long id = getCurrentID(queryIDFilter, value);
        if (id == 0)
          continue;
        if (!idsToCellDataMap.containsKey(id)) {
          idsToCellDataMap.put(id, value);
        } else if (isNewValueSmaller(idsToCellDataMap.get(id).attribute.getCatalogEntry(),
                idsToCellDataMap.get(id).getValue(), value.attribute.getCatalogEntry(), value.getValue())) {
          ResultCellData oldData = idsToCellDataMap.get(id);
          newRows.remove(oldData);
          idsToCellDataMap.put(id, value);
          if (idsInProcessedRowsToRowMap.containsKey(id)) {
            Row rowToRemove = idsInProcessedRowsToRowMap.remove(id);
            queryRunnable.getResult().removeRow(rowToRemove);
          }
        } else {
          newRows.remove(aNewRow);
        }
      }
    }
  }

  private long getCurrentID(QueryIDFilter queryIDFilter, ResultCellData resultCellData) {
    if (queryIDFilter == null || queryIDFilter.getFilterIDType().equals(FilterIDType.PID))
      return resultCellData.getValue().getPid();
    else if (queryIDFilter.getFilterIDType().equals(FilterIDType.CaseID))
      return resultCellData.getValue().getCaseID();
    else if (queryIDFilter.getFilterIDType().equals(FilterIDType.DocID))
      return resultCellData.getValue().getDocID();
    else
      return 0;
  }

  private boolean isNewValueSmaller(CatalogEntry oldEntry, Information oldInfo, CatalogEntry newEntry,
          Information newInfo) {
    if (!oldEntry.equals(newEntry)) {
      System.err.println("The compared attributes have different catalog entries");
      return false;
    }
    switch (oldEntry.getDataType()) {
      case SingleChoice:
      case Text:
        return newInfo.getValue().compareTo(oldInfo.getValue()) < 0;
      case Number:
        return newInfo.getValueDec() < oldInfo.getValueDec();
      case DateTime:
        return newInfo.getValueDate().before(oldInfo.getValueDate());
      default:
        return false;
    }
  }

  private List<List<ResultCellData>> getIDFilterGroups(QueryIDFilter idFilter,
          List<ResultCellData> cellData) {
    HashMap<QueryAttribute, ResultCellData> attr2CellDataMap = getAttrMap(cellData);
    List<QueryAttribute> attributesRecursive = idFilter.getAttributesRecursive();
    for (QueryAttribute anAttr : attr2CellDataMap.keySet().toArray(new QueryAttribute[0])) {
      if (!attributesRecursive.contains(anAttr)) {
        attr2CellDataMap.remove(anAttr);
      }
    }
    HashMap<Long, HashMap<QueryAttribute, ResultCellData>> resultSets = createPartitions(idFilter,
            attr2CellDataMap);
    filterPartitions(resultSets, attr2CellDataMap, idFilter.getFilterIDType());
    return createNewRowsFromPartitions(resultSets, cellData);
  }

  private HashMap<Long, HashMap<QueryAttribute, ResultCellData>> createPartitions(
          QueryIDFilter idFilter, HashMap<QueryAttribute, ResultCellData> attr2CellDataMap) {
    HashMap<Long, HashMap<QueryAttribute, ResultCellData>> resultSets = new HashMap<>();
    // create partitions for the IDs to filter on
    for (ResultCellData aCellData : attr2CellDataMap.values()) {
      for (Information anInfo : aCellData.getValues()) {
        long ref = getFilterID(idFilter.getFilterIDType(), anInfo);
        if (!resultSets.containsKey(ref)) {
          resultSets.put(ref, new HashMap<>());
        }
        HashMap<QueryAttribute, ResultCellData> resultSet = resultSets.get(ref);
        if (!resultSet.containsKey(aCellData.attribute)) {
          ResultCellData newCellData = new ResultCellData(aCellData.attribute);
          resultSet.put(aCellData.attribute, newCellData);
        }
        ResultCellData newCellData = resultSet.get(aCellData.attribute);
        newCellData.addValue(anInfo);
      }
    }
    return resultSets;
  }

  private long getFilterID(FilterIDType type, Information anInfo) {
    long ref = 0;
    if (type == FilterIDType.DocID) {
      ref = anInfo.getDocID();
    } else if (type == FilterIDType.CaseID) {
      ref = anInfo.getCaseID();
    } else if (type == FilterIDType.PID) {
      ref = anInfo.getPid();
    }
    return ref;
  }

  private void filterPartitions(HashMap<Long, HashMap<QueryAttribute, ResultCellData>> resultSets,
          HashMap<QueryAttribute, ResultCellData> attr2CellDataMap, FilterIDType type) {
    // filter the partitions from all IDs which have QueryAttributes that
    // are mandatory (not
    // optional) e.g. FilterID a DocID with two attributes for a patient for
    // which one document
    // contains both attributes and the other only one, the document with
    // only one attribute has to
    // be removed from the results
    for (long anID : resultSets.keySet().toArray(new Long[0])) {
      HashMap<QueryAttribute, ResultCellData> aResultSet = resultSets.get(anID);
      for (QueryAttribute anAttr : attr2CellDataMap.keySet()) {
        if (!aResultSet.containsKey(anAttr)) {
          ResultCellData cellData1 = attr2CellDataMap.get(anAttr);
          if (cellData1.getValues().size() == 0) {
            continue;
          }
          if (anAttr.isOptional()) {
            ResultCellData cellData2 = attr2CellDataMap.get(anAttr);
            for (Information anInfo : cellData2.getValues().toArray(new Information[0])) {
              long ref = getFilterID(type, anInfo);
              if (ref != anID) {
                cellData2.removeValue(anInfo);
              }
            }
            continue;
          }
          resultSets.remove(anID);
          break;
        }
      }
    }
  }

}
