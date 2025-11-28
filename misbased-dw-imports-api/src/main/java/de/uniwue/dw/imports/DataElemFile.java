package de.uniwue.dw.imports;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.configured.data.ConfigDataSource;
import de.uniwue.dw.imports.configured.data.ConfigDataSourceFilesystem;
import de.uniwue.misc.util.FileUtilsUniWue;

public class DataElemFile extends DataElem {

  protected File file;

  protected long cachedTimestamp;

  public DataElemFile(File aFile, ConfigDataSource aDataSource) {
    super(aDataSource);
    file = aFile;
  }

  public File getFile() {
    return file;
  }

  @Override
  public long getTimestamp() {
    if (cachedTimestamp == 0) {
      cachedTimestamp = file.lastModified();
    }
    return cachedTimestamp;
  }

  @Override
  public String getName() {
    return file.getName();
  }

  protected ConfigDataSourceFilesystem getDataSource() {
    return (ConfigDataSourceFilesystem) dataSource;
  }

  @Override
  public String getContent() throws ImportException {
    String text;
    try {
      text = FileUtilsUniWue.file2String(file, getDataSource().encoding);
    } catch (IOException e) {
      throw new ImportException(ImportExceptionType.IO_ERROR, e);
    }
    return text;
  }

  @Override
  public InputStreamReader getInputStreamReader() throws ImportException {
    try {
      FileInputStream inStream = new FileInputStream(file);
      InputStreamReader in = new InputStreamReader(inStream, getDataSource().encoding);
      return in;
    } catch (FileNotFoundException e) {
      throw new ImportException(ImportExceptionType.DATA_PARSING_ERROR, " error " + e.getMessage());
    } catch (UnsupportedEncodingException e) {
      throw new ImportException(ImportExceptionType.DATA_PARSING_ERROR, " error " + e.getMessage());
    }
  }

  @Override
  public void logLatestRowNumber() {
  }

}
