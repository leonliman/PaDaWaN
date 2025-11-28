package de.uniwue.dw.imports;

public class ImportException extends Exception {

  private static final long serialVersionUID = 1L;

  public enum ImportExceptionType {
    UNKNOWN, INFO, IMPORT_PROCESS,
    //
    IMPORTER_LOAD_ERROR, IMPORT_DIR_NONE_EXISTANT, FILE_FORMAT_UNKNOWN, FILE_NOT_FOUND, FILE_WRONG_ENCODING, FILE_ALREADY_BACKED_UP_OR_NOT_FOUND, FILE_IMPORT_SUCCESS, FILE_IMPORT_ERROR, FILE_IMPORT_SKIP,
    // data processing errors
    DATA_PARSING_ERROR, DATA_MALFORMED, DATA_FIELD_EMPTY, DATA_MISSING, SQL_ERROR, IO_ERROR, DATA_MISMATCH,
    // NO data entries found
    NO_CASE, NO_CATALOG_ENTRY, NO_DOC, NO_PID,
    // XML file errors
    XML_ELEMENT_NOT_FOUND, XML_INVALID, XML_PARSING,
    // CSV file errors
    CSV_HEADER_ROW_LENGTH_MISMATCH, CSV_COLUMNS_MISSING, CSV_COLUMN_NON_EXISTANT,
    // OTHER STUFF
    NULLPOINTER, PID_FILTER
  };

  public enum ImportExceptionLevel {
    warning, error, info
  }

  private ImportExceptionType _type;

  private Exception _externalException;

  private long _line;

  private String _project;

  private IDataElem _file;

  private ImportExceptionLevel level = ImportExceptionLevel.error;

  public String getProject() {
    return _project;
  }

  @Override
  public String getMessage() {
    String result = super.getMessage();
    if (getProject() != null) {
      result = result + "; Project: " + getProject();
    }
    if ((getFile() != null) && (getFile().getName() != null)) {
      result = result + "; File: " + getFile().getName();
    }
    if (getLine() != 0) {
      result = result + "; Line: " + getLine();
    }
    if (getType() != null) {
      result = result + "; Type: " + getType();
    }
    if (_externalException != null) {
      result += ", " + _externalException.getMessage();
    }
    return result;
  }

  public void setProject(String project) {
    this._project = project;
  }

  public IDataElem getFile() {
    return _file;
  }

  public void setFile(IDataElem file) {
    this._file = file;
  }

  public ImportException(ImportExceptionType type, String message, Exception e) {
    this(type, message, null, e);
  }

  public ImportException(ImportExceptionType type, String message) {
    this(type, message, null, null);
  }

  public ImportException(ImportExceptionType type, String message, ImportExceptionLevel aLevel) {
    this(type, message, null, null, aLevel, 0);
  }

  public ImportException(ImportExceptionType type, Exception e) {
    this(type, e.getMessage(), null, e);
  }

  public ImportException(ImportExceptionType type, String message, String project) {
    this(type, message, project, null);
  }

  public ImportException(ImportExceptionType type, String message, String project, long rowCount) {
    this(type, message, project, null, ImportExceptionLevel.error, rowCount);
  }

  public ImportException(ImportExceptionType type, String message, String project, Exception e) {
    this(type, message, project, e, ImportExceptionLevel.error, 0);
  }

  public ImportException(ImportExceptionType type, String message, String project, Exception e,
          ImportExceptionLevel aLevel, long rowCount) {
    super(message);
    _type = type;
    _line = rowCount;
    _externalException = e;
    _project = project;
    level = aLevel;
  }

  public void setLine(long line) {
    _line = line;
  }

  public long getLine() {
    return _line;
  }

  public ImportExceptionType getType() {
    return _type;
  }

  public Exception getExtException() {
    return _externalException;
  }

  public ImportExceptionLevel getLevel() {
    return level;
  }

  public void setLevel(ImportExceptionLevel level) {
    this.level = level;
  }

}
