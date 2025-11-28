package de.uniwue.dw.imports;

import java.io.InputStreamReader;

import de.uniwue.dw.imports.configured.data.ConfigDataSourceDatabase;

public class WrappedDBDataElemIterator extends DataElem implements IDataElemIterator {

  private DBDataElemIterator iter;

  private boolean nextHasBeenCalled = false;

  public WrappedDBDataElemIterator(ConfigDataSourceDatabase aDataSource) throws ImportException {
    super(aDataSource);
    iter = new DBDataElemIterator(aDataSource);
  }

  @Override
  public boolean hasNext() {
    throw new RuntimeException("does not implement");
  }

  @Override
  public DataElem next() {
    if (nextHasBeenCalled) {
      return null;
    } else {
      nextHasBeenCalled = true;
      return iter;
    }
  }

  @Override
  public long getTimestamp() {
    return iter.getTimestamp();
  }

  @Override
  public String getName() {
    return iter.getName();
  }

  @Override
  public String getContent() throws ImportException {
    return iter.getContent();
  }

  @Override
  public InputStreamReader getInputStreamReader() throws ImportException {
    return iter.getInputStreamReader();
  }

  @Override
  public void dispose() throws ImportException {
    iter.dispose();
  }

  @Override
  public void logLatestRowNumber() {
    iter.logLatestRowNumber();
  }

}
