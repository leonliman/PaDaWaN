package de.uniwue.dw.imports;

import java.util.Collection;
import java.util.Iterator;

public class FileElemIterator implements IDataElemIterator {

  private Iterator<DataElem> fileIter;

  public FileElemIterator(Collection<DataElem> files) {
    fileIter = files.iterator();
  }

  @Override
  public boolean hasNext() {
    return fileIter.hasNext();
  }

  @Override
  public DataElem next() {
    if (hasNext()) {
      return fileIter.next();
    } else {
      return null;
    }
  }

  @Override
  public void dispose() throws ImportException {
  }

}
