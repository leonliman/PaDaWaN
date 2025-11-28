package de.uniwue.dw.imports;

import java.util.Iterator;

public interface IDataElemIterator extends Iterator<DataElem> {

  void dispose() throws ImportException;
  
}
