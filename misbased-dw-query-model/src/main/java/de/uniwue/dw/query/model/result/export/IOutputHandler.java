package de.uniwue.dw.query.model.result.export;

import java.util.List;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.result.Result;
import de.uniwue.dw.query.model.result.Row;

/**
 * The OutputHandlers handle the flow of all generated data during the search process. It grants
 * access to the generated Result and manages IO-access to written files and streams
 */
public interface IOutputHandler {

  public void setHeader(List<String> header) throws QueryException;

  public void addRow(Row row) throws QueryException;

  public void done() throws QueryException;

  /**
   * Returns the result if present.
   * 
   * @return result can be null.
   */
  public Result getResult();

  public void setResult(Result result) throws QueryException;

  public void close() throws QueryException;

  public void setDocsFound(long numFound) throws QueryException;

  public void setQueryTime(long queryTime) throws QueryException;

  public ExportConfiguration getExportConfiguration();

}
