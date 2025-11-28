package de.uniwue.dw.imports;

import java.io.InputStreamReader;

public interface IDataElem {

  long getTimestamp();

  String getName();

  String getContent() throws ImportException;

  InputStreamReader getInputStreamReader() throws ImportException;

  void logLatestRowNumber();
  
}