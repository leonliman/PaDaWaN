package de.uniwue.dw.imports;

public class DummyElemIterator implements IDataElemIterator {

  public DummyElemIterator() {
  }


  @Override
  public boolean hasNext() {
    return false;
  }


  @Override
  public DataElem next() {
    return null;
  }


  @Override
  public void dispose() throws ImportException {
  }

}
