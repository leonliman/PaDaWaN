package de.uniwue.dw.query.solr.export;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.core.model.hooks.IDwCatalogHooks;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryIDFilter;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;
import de.uniwue.dw.query.model.lang.ReductionOperator;
import de.uniwue.dw.query.model.result.Cell.CellType;
import de.uniwue.dw.query.model.result.DesiredContentCheckerVisitor;
import de.uniwue.dw.query.model.result.Highlighter;
import de.uniwue.dw.query.model.result.ResultCellData;
import de.uniwue.dw.query.solr.SolrConstants;
import de.uniwue.dw.query.solr.SolrUtil;
import de.uniwue.dw.query.solr.preprocess.PropagateIndexer2;
import de.uniwue.misc.util.TimeUtil;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.common.SolrDocument;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

public class SolrStreamingResponseCallback extends StreamingResponseCallback {

  static final Logger logger = LogManager.getLogger(SolrStreamingResponseCallback.class.getName());

  private final SolrPatientQueryRunnable patientQueryTask;

  private final Highlighter highlighter = new Highlighter();

  public SolrStreamingResponseCallback(SolrPatientQueryRunnable aPatientQueryTask) {
    patientQueryTask = aPatientQueryTask;
  }

  @Override
  public void streamDocListInfo(long numFound, long start, Float maxScore) {
    patientQueryTask.getMonitor().worked(SolrConstants.SENDING_QUERY_WORK);
    patientQueryTask.getMonitor().subTask("Dokumente werden empfangen");
  }

  public boolean checkAndHandleCaseIDAttribute(QueryAttribute anAttr, long patientID, long caseID,
          List<ResultCellData> cellDatas) {
    CatalogEntry anEntry = anAttr.getCatalogEntry();
    if (anEntry.getProject().equalsIgnoreCase(IDwCatalogHooks.PROJECT_HOOK_CASE_ID)
            && anEntry.getExtID().equalsIgnoreCase(IDwCatalogHooks.EXT_HOOK_CASE_ID)) {
      Information anInfo = new Information(0, anEntry.getAttrId(), patientID, caseID, null, null, null,
              Long.toString(caseID), Long.toString(caseID), (double) caseID, 0, 0, 0);
      ResultCellData cellData = new ResultCellData(anInfo, anAttr);
      cellDatas.add(cellData);
      return true;
    } else {
      return false;
    }
  }

  public boolean checkAndHandlePIDAttribute(QueryAttribute anAttr, long patientID, long caseID,
          List<ResultCellData> cellDatas) {
    CatalogEntry anEntry = anAttr.getCatalogEntry();
    if (anEntry.getProject().equalsIgnoreCase(IDwCatalogHooks.PROJECT_HOOK_PATIENT_ID)
            && anEntry.getExtID().equalsIgnoreCase(IDwCatalogHooks.EXT_HOOK_PATIENT_ID)) {
      Information anInfo = new Information(0, anEntry.getAttrId(), patientID, caseID, null, null, null,
              Long.toString(patientID), Long.toString(patientID), (double) patientID, 0, 0, 0);
      ResultCellData cellData = new ResultCellData(anInfo, anAttr);
      cellDatas.add(cellData);
      return true;
    } else {
      return false;
    }
  }

  private void addData(SolrDocument doc, QueryAttribute anAttr, CatalogEntry anEntry, boolean hasDocIDFilter,
          List<Object> values, List<Object> refIDs, List<Object> measureTimes, boolean isSubSearch) {
    if (anAttr.displayValue() || (anAttr.getValueCompares().size() > 0)
            || (anAttr.getReferencingValueRelOps().size() > 0)) {
      String fieldName;
      if (anAttr.displayCaseID() || anAttr.displayDocID() || anAttr.displayInfoDate() || hasDocIDFilter) {
        fieldName = SolrUtil.getSolrFieldName(anEntry);
      } else {
        fieldName = SolrUtil.getSolrDisplayFieldName(anAttr);
      }
      Object fieldValue = doc.getFieldValue(fieldName);
      if (fieldValue != null) {
        Collection<Object> newCol = (Collection) fieldValue;
        values.addAll(newCol);
      }
    }
    if (anAttr.displayDocID() || hasDocIDFilter) {
      String fieldName = SolrUtil.getSolrFieldName(anEntry, CellType.DocID);
      Object fieldValue = doc.getFieldValue(fieldName);
      if (fieldValue != null) {
        Collection<Object> newCol = (Collection) fieldValue;
        refIDs.addAll(newCol);
      }
    }
    // The measureTime is needed when it simply has to be displayed or when it is needed for
    // post processing comparisons by relative temporal operators
    if (anAttr.displayInfoDate() || (anAttr.getTempOpsAbs().size() > 0)
            || (anAttr.getReferencingTempRelOps().size() > 0) || (anAttr.getTemporalOpsRel().size() > 0)
            || (anAttr.getReductionOperator() == ReductionOperator.EARLIEST)
            || (anAttr.getReductionOperator() == ReductionOperator.LATEST)) {
      String fieldName = SolrUtil.getSolrFieldName(anEntry, CellType.MeasureTime);
      Object fieldValue = doc.getFieldValue(fieldName);
      if (fieldValue != null) {
        Collection<Object> newCol = (Collection) fieldValue;
        measureTimes.addAll(newCol);
      }
    }
  }

  private Object[] getTripel(List<Object> values, List<Object> refIDs, List<Object> measureTimes, int index) {
    Object[] result = new Object[3];
    Object value = null;
    Object refID = null;
    Object measureTime = null;
    if (values.size() > 0) {
      value = values.get(index);
    }
    if (refIDs.size() > 0) {
      refID = refIDs.get(index);
    }
    if (measureTimes.size() > 0) {
      measureTime = measureTimes.get(index);
    }
    result[0] = value;
    result[1] = refID;
    result[2] = measureTime;
    return result;
  }

  private void reduceData(QueryAttribute anAttr, List<Object> values, List<Object> refIDs, List<Object> measureTimes) {
    int indexOfLargest = 0;
    int indexOfSmallest = 0;
    int indexOfEarliest = 0;
    int indexOfLatest = 0;
    Date latest = new Date(0);
    Date earliest = new Date(3000, 0, 0);
    double smallest = Double.MAX_VALUE;
    double largest = Double.MIN_VALUE;
    int infoCount = Math.max(measureTimes.size(), Math.max(values.size(), refIDs.size()));
    for (int i = 0; i < infoCount; i++) {
      if (values.size() > 0) {
        if ((anAttr.getReductionOperator() == ReductionOperator.MAX)
                || (anAttr.getReductionOperator() == ReductionOperator.MIN)) {
          double valueDec = Double.valueOf(values.get(i).toString());
          if (smallest > valueDec) {
            smallest = valueDec;
            indexOfSmallest = i;
          }
          if (largest < valueDec) {
            largest = valueDec;
            indexOfLargest = i;
          }
        }
      }
      if (measureTimes.size() > 0) {
        Date date = (Date) measureTimes.get(i);
        if (date.after(latest)) {
          latest = date;
          indexOfLatest = i;
        }
        if (date.before(earliest)) {
          earliest = date;
          indexOfEarliest = i;
        }
      }
    }
    Object[] tripel = null;
    if (anAttr.getReductionOperator() == ReductionOperator.EARLIEST) {
      tripel = getTripel(values, refIDs, measureTimes, indexOfEarliest);
    } else if (anAttr.getReductionOperator() == ReductionOperator.LATEST) {
      tripel = getTripel(values, refIDs, measureTimes, indexOfLatest);
    } else if (anAttr.getReductionOperator() == ReductionOperator.MAX) {
      tripel = getTripel(values, refIDs, measureTimes, indexOfLargest);
    } else if (anAttr.getReductionOperator() == ReductionOperator.MIN) {
      tripel = getTripel(values, refIDs, measureTimes, indexOfSmallest);
    }
    values.clear();
    refIDs.clear();
    measureTimes.clear();
    if (tripel[0] != null) {
      values.add(tripel[0]);
    }
    if (tripel[1] != null) {
      refIDs.add(tripel[1]);
    }
    if (tripel[2] != null) {
      measureTimes.add(tripel[2]);
    }
  }

  private Object[][] getData(SolrDocument doc, QueryAttribute anAttr) {
    Object[][] result = new Object[3][];
    List<Object> values = new ArrayList<>();
    List<Object> refIDs = new ArrayList<>();
    List<Object> measureTimes = new ArrayList<>();
    boolean hasDocIDFilter = false;
    for (QueryIDFilter aFilter : anAttr.getAncestorIDFilters()) {
      if (aFilter.getFilterIDType() == FilterIDType.DocID) {
        hasDocIDFilter = true;
      }
    }
    if (anAttr.getCatalogEntry().getDataType() == CatalogEntryType.Bool
            || anAttr.getCatalogEntry().getDataType() == CatalogEntryType.Structure) {
      addData(doc, anAttr, anAttr.getCatalogEntry(), hasDocIDFilter, values, refIDs, measureTimes, false);
      if (anAttr.displayCaseID() || anAttr.displayDocID() || hasDocIDFilter || anAttr.displayInfoDate()) {
        for (CatalogEntry aSibling : anAttr.getCatalogEntry().getDescendants()) {
          addData(doc, anAttr, aSibling, hasDocIDFilter, values, refIDs, measureTimes, true);
        }
      }
    } else {
      addData(doc, anAttr, anAttr.getCatalogEntry(), hasDocIDFilter, values, refIDs, measureTimes, false);
    }
    if ((anAttr.getReductionOperator() != ReductionOperator.NONE)
            && (anAttr.getReductionOperator() != ReductionOperator.ANY)) {
      reduceData(anAttr, values, refIDs, measureTimes);
    }
    result[0] = values.toArray();
    result[1] = refIDs.toArray();
    result[2] = measureTimes.toArray();
    return result;
  }

  private ResultCellData processData(long patientID, long caseID, QueryAttribute anAttr, Object[] values,
          Object[] refIDs, Object[] measureTimes) {
    CatalogEntry anEntry = anAttr.getCatalogEntry();
    // take the length of one of the lists (some of them could be empty)
    int infoCount = Math.max(measureTimes.length, Math.max(values.length, refIDs.length));
    if ((infoCount == 0) && (caseID != 0) && anAttr.displayCaseID()) {
      infoCount = 1;
    }
    List<Information> infos = new ArrayList<>();
    for (int i = 0; i < infoCount; i++) {
      String value = null;
      double valueDec = 0;
      long refID = 0;
      Timestamp measureTime = null;
      if (values.length > i) {
        Object aValue = values[i];
        if (anEntry.getDataType() == CatalogEntryType.DateTime) {
          value = TimeUtil.format((Date) aValue);
        } else {
          value = aValue.toString();
        }
        if (anEntry.getDataType() == CatalogEntryType.Number) {
          valueDec = Double.valueOf(value);
        }
      }
      if (refIDs.length > i) {
        String refIDString = (String) refIDs[i];
        refID = Long.valueOf(refIDString);
      }
      if (measureTimes.length > i) {
        Date date = (Date) measureTimes[i];
        measureTime = new Timestamp(date.getTime());
      }
      Information anInfo = new Information(0, anEntry.getAttrId(), patientID, caseID, measureTime, null, null, value,
              value, valueDec, refID, 0, 0);
      boolean isHit = true;
      if (highlighter.hasToHighlight(anAttr)) {
        DesiredContentCheckerVisitor checkerVisitor = new DesiredContentCheckerVisitor();
        ParseTree desiredContentAsParseTree = anAttr.getDesiredContentAsParseTree();
        checkerVisitor.visit(desiredContentAsParseTree);
        isHit = highlighter.highlight(anAttr, anInfo);
      }
      if (isHit) {
        if (anAttr.isOnlyDisplayExistence()) {
          anInfo.setValue("x");
          anInfo.setValueShort("x");
        }
        infos.add(anInfo);
      }
    }
    ResultCellData cellData = new ResultCellData(infos, anAttr);
    return cellData;
  }

  @Override
  public void streamSolrDocument(SolrDocument doc) {
    try {
      long patientID = 0, caseID = 0;
      boolean isPatientQuery = patientQueryTask.getQueryRoot().getFilterIDTypeToUseForCount() == FilterIDType.PID;
      if (isPatientQuery) {
        patientID = PropagateIndexer2.solrDocumentID2PatientID(doc.getFieldValue("id").toString());
      } else {
        Object patientFieldValue = doc.getFieldValue("patient");
        if (patientFieldValue != null) {
          patientID = Long.valueOf(patientFieldValue.toString());
        }
        caseID = PropagateIndexer2.solrDocumentID2CaseID(doc.getFieldValue("id").toString());
      }
      List<ResultCellData> cellDatas = new ArrayList<>();
      for (QueryAttribute anAttr : patientQueryTask.attributes) {
        // The PID and CaseID have no additional fact data saved in the index so this has to be
        // simulated by individual methods
        if (checkAndHandlePIDAttribute(anAttr, patientID, caseID, cellDatas)) {
          continue;
        }
        if (checkAndHandleCaseIDAttribute(anAttr, patientID, caseID, cellDatas)) {
          continue;
        }
        Object[][] data = getData(doc, anAttr);
        Object[] values = data[0];
        Object[] refIDs = data[1];
        Object[] measureTimes = data[2];
        ResultCellData cellData = processData(patientID, caseID, anAttr, values, refIDs, measureTimes);
        cellDatas.add(cellData);
        // System.out.println("Celldata: patient " + patientID + " valuelength " + values.length);
      }
      if ((patientQueryTask.getQueryRoot().getLimitResult() > 0)
              && (patientQueryTask.getNumFound() >= patientQueryTask.getQueryRoot().getLimitResult())
              && !patientQueryTask.isCanceled()) {
        patientQueryTask.doCancel();
        return;
      }
      patientQueryTask.createResultRow(cellDatas, patientID);
    } catch (Exception e) {
      // as Solr does not allow to throw exceptions in "streamSolrDocument" we have to enforce an
      // exception that can always be thrown
      throw new RuntimeException(e);
    }
  }

}
