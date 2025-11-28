package de.uniwue.dw.query.model.result;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.client.IGUIClient;
import de.uniwue.dw.query.model.client.IQueryRunner;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.exception.QueryStructureException;
import de.uniwue.dw.query.model.exception.QueryStructureException.QueryStructureExceptionType;
import de.uniwue.dw.query.model.lang.DisplayStringVisitor;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.result.Cell.CellType;
import de.uniwue.dw.query.model.result.export.ExportConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class PatientQueryRunnable extends QueryRunnable {

  static final Logger logger = LogManager.getLogger(PatientQueryRunnable.class);

  // receivedDocs and numFound are only for the monitor to calculate the work done
  private long receivedDocs = 0;

  private long numFound;

  public int displayedWorkStatus = 0;

  // this is a cache of the recursive attributes of the queryRoot so that they do not have to be
  // retrieved all over again when they are needed
  public List<QueryAttribute> attributes;

  private PostProcessor postProcessor;

  // this is for optionaly further postprocessors. This feature does not work yet
  public List<IPostProcessor> postProcessors = new ArrayList<IPostProcessor>();

  public PatientQueryRunnable(int workTotal, QueryRoot queryRoot, User user, ExportConfiguration exportConfig,
          IGUIClient guiClient) throws QueryException {
    super(workTotal, queryRoot, user, exportConfig, guiClient);
    postProcessor = new PostProcessor(this);
  }

  abstract public long getOnlyCountNumber() throws QueryException;

  protected void createHeader() throws QueryException {
    List<String> columnNames = new ArrayList<>();
    if (getQueryRoot().isDisplayPID()) {
      columnNames.add("PID");
    }
    attributes = getQueryRoot().getAttributesRecursive();
    for (QueryAttribute anAttribute : attributes) {
      addColumns(columnNames, anAttribute);
    }
    outputHandler.setHeader(columnNames);
  }

  @Override
  protected void createOutputInternal() throws QueryException {
    createHeader();
    if (getQueryRoot().isOnlyCount()) {
      createOnlyCountOutput();
      return;
    } else {
      createOutput();
    }
  }

  protected void createOnlyCountOutput() throws QueryException {
    outputHandler.setHeader(Arrays.asList(new String[] { "Count" }));
    long numFound;
    try {
      if (getQueryRunner().hasToDoPostProcessing(getQueryRoot())) {
        if (getQueryRunner().canDoPostProcessing()) {
          getQueryRoot().setOnlyCount(false);
          getQueryRoot().setLimitResult(0);
          for (QueryAttribute anAttr : getQueryRoot().getAttributesRecursive()) {
            if (anAttr.isOptional()) {
              anAttr.setDisplayValue(false);
            }
          }
          IQueryRunner queryRunner = getQueryRunner();
          int createQuery = queryRunner.createQuery(getQueryRoot(), user);
          Result result = getGUIClient().getQueryRunner().runQueryBlocking(createQuery);
          numFound = result.getDocsFound();
        } else {
          throw new QueryStructureException(QueryStructureExceptionType.ENGINE_CANNOT_PERFORM_OPERATION, null,
                  "Postprocessing would be necessary but engine cannot post process");
        }
      } else {
        numFound = getOnlyCountNumber();
      }
    } catch (GUIClientException e) {
      throw new QueryException(e);
    }
    Row row = new Row();
    StatisticalCell newCell = row.createNewStatisticalCell();
    newCell.value = Long.toString(numFound);
    outputHandler.addRow(row);
  }

  public void updateMonitor() {
    // this whole calculation with the step sizes and when to increment is total crap !!!
    if (isFirstUpdate()) {
      calcOneWorkStepSize();
    }
    if (Math.floor(worked) < Math.floor(worked + oneWorkStepSize)) {
      getMonitor().worked(intOneWorkStepSize);
      getMonitor().setTaskName("Dokumente werden empfangen " + receivedDocs + " / "
              + Math.min(getQueryRoot().getLimitResult(), numFound));
    }
    worked += oneWorkStepSize;
  }

  private double oneWorkStepSize = 0;

  private double worked = 0;

  private int intOneWorkStepSize;

  private void calcOneWorkStepSize() {
    double sumWorked = getMonitor().getSumWorked();
    double totalWork = getMonitor().getTotalWork();
    double remainingWordk = totalWork - sumWorked;
    int limitResult = getQueryRoot().getLimitResult();
    double totalRespondRows = Math.min(limitResult, numFound);
    oneWorkStepSize = remainingWordk / totalRespondRows;
    intOneWorkStepSize = (int) Math.floor(oneWorkStepSize) + 1;
  }

  private boolean isFirstUpdate() {
    return receivedDocs == 1;
  }

  public long getNumFound() {
    return numFound;
  }

  public void setNumResultsFound(long numFound) {
    this.numFound = numFound;
    Result result = outputHandler.getResult();
    if (result != null) {
      result.setDocsFound(numFound);
    }
  }

  public void createResultRow(List<ResultCellData> cellData, long pid) throws QueryException {
    // Don't understand next row. Commented it out
    // if (getQueryRoot().getLimitResult() > 0 && getNumFound() >= getQueryRoot().getLimitResult())
    // {
    // return;
    // }
    logger.debug("Reciving document #" + receivedDocs);
    List<List<ResultCellData>> resultRows;
    int originalLimitResult = originalQueryRoot.getLimitResult();
    if (getQueryRunner().hasToDoPostProcessing(getQueryRoot())) {
      resultRows = postProcessor.doPostProcessing(cellData);
    } else {
      resultRows = new ArrayList<>();
      resultRows.add(cellData);
    }
    for (List<ResultCellData> aResultCellRow : resultRows) {
      Row row = new Row();
      initializeResponse(aResultCellRow, row, pid);
      for (ResultCellData aCellData : aResultCellRow) {
        processResponse(row, aCellData);
      }
      // either there is no limit or the limited count it not reached yet
      if ((originalLimitResult == 0) || (receivedDocs < originalLimitResult)) {
        outputHandler.addRow(row);
      } else {
        stopStreaming();
        break;
      }
    }
    receivedDocs += resultRows.size();
    if (getQueryRunner().postProcessingCanChangesResultCount(getQueryRoot())) {
      // the real number should not be computed because all documents would have to be
      // returned from the index engine.
      setNumResultsFound(receivedDocs);
    } else {
      if (originalLimitResult == 0) {
        if (getResult() != null) { // this can be the case for a streaming output handler
          setNumResultsFound(getResult().getRows().size());
        }
      }
    }
    updateMonitor();
  }

  private void processResponse(Row row, ResultCellData cellData) {
    Cell cell;
    if (cellData.attribute.displayValue()) {
      cell = row.createNewCell(cellData, CellType.Value);
    }
    if (cellData.attribute.displayCaseID()) {
      cell = row.createNewCell(cellData, CellType.CaseID);
    }
    if (cellData.attribute.displayDocID()) {
      cell = row.createNewCell(cellData, CellType.DocID);
    }
    if (cellData.attribute.displayInfoDate()) {
      cell = row.createNewCell(cellData, CellType.MeasureTime);
    }
  }

  private void initializeResponse(List<ResultCellData> cellData, Row row, long pid) {
    row.setPid(pid);
    if (getQueryRoot().isDisplayPID()) {
      row.createNewCell(new ResultCellData(
              new Information(0, 0, pid, 0, null, null, null, Long.toString(pid), Long.toString(pid), null, 0, 0, 0),
              null), CellType.PID);
    }
  }

  private void addColumns(List<String> columnNames, QueryAttribute a) throws QueryException {
    String columnName = a.accept(new DisplayStringVisitor());
    String columnNameCopy = columnName;
    int counter = 1;
    while (columnNames.contains(columnName)) {
      counter++;
      columnName = columnNameCopy + " (" + counter + ")";
    }
    if (a.displayValue()) {
      columnNames.add(columnName);
    }
    if (a.displayCaseID()) {
      columnNames.add(columnName + " - CaseID");
    }
    if (a.displayDocID()) {
      columnNames.add(columnName + " - DocID");
    }
    if (a.displayInfoDate()) {
      columnNames.add(columnName + " - MeasureTime");
    }
  }

  public List<QueryAttribute> getAttributes() {
    return attributes;
  }

}
