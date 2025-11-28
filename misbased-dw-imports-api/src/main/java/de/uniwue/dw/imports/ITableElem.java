package de.uniwue.dw.imports;

import java.util.List;
import java.util.Set;

public interface ITableElem extends IDataElem {

  Set<String> getHeaderColumns() throws ImportException;

  List<String> getHeaderColumnsAsList();

  Integer getColumnIndex(String key) throws ImportException;

  String getItem(String key) throws ImportException;

  Boolean moveToNextLine() throws ImportException;

  Integer getTokenLength();

  Integer getHeaderColAmount() throws ImportException;

  boolean initHeaderLine() throws ImportException;

  void checkRequiredHeaders(String[] necessaryColumnHeaders) throws ImportException;

  void checkRequiredHeaders(List<String> necessaryColumnHeaders) throws ImportException;

  void close() throws ImportException;

  String[] getCurrentLineTokens();

  boolean isCurrentLineWellFormed() throws ImportException;

  Long getRowCounter();

}