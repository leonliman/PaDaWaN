package de.uniwue.dw.imports.configured.data;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.csv.QuoteMode;

import de.uniwue.dw.imports.CsvFile;
import de.uniwue.dw.imports.DataElem;
import de.uniwue.dw.imports.FileElemIterator;
import de.uniwue.dw.imports.IDataElemIterator;
import de.uniwue.dw.imports.ImportException;

public class ConfigDataSourceCSVFile extends ConfigDataSourceCSVDir {

  private File file;

  Collection<DataElem> result = new ArrayList<DataElem>();

  public ConfigDataSourceCSVFile(File aFile, String anEncoding) throws ImportException {
    this(aFile, ConfigDataSourceCSVDir.default_delimiter, ConfigDataSourceCSVDir.default_quoteMode,
            ConfigDataSourceCSVDir.default_escapeChar, anEncoding);
  }

  public ConfigDataSourceCSVFile(File aFile, char aDelimiter, QuoteMode aQuoteMode,
          char anEscapeChar, String anEncoding) throws ImportException {
    super(aFile.getParentFile(), aDelimiter, aQuoteMode, anEscapeChar, anEncoding, true, true, null);
    setFile(aFile);
  }

  public ConfigDataSourceCSVFile(File aFile, char aDelimiter, QuoteMode aQuoteMode,
          char anEscapeChar, String anEncoding, boolean firstIsHeader, boolean getFilesRecursive, String[] manualHeader)
          throws ImportException {
    super(aFile.getParentFile(), aDelimiter, aQuoteMode, anEscapeChar, anEncoding, firstIsHeader,
            getFilesRecursive, manualHeader);
    setFile(aFile);
  }

  public ConfigDataSourceCSVFile(ConfigStructureElem aParent) {
    super(aParent);
  }

  public IDataElemIterator getDataElemsToProcess(String project, boolean doSort)
          throws ImportException {
    return new FileElemIterator(result);
  }

  public void addDataElemsToProcess(String project, File afile) throws ImportException {
    result.add(new CsvFile(afile, this));

  }

  public File getFile() {
    return file;
  }

  public void setFile(File file) throws ImportException {
    this.file = file;
    result.add(new CsvFile(file, this));
  }

}
