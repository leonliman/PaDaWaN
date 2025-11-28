package de.uniwue.dw.imports.configured.data;

import java.io.File;
import java.util.ArrayList;

import org.apache.commons.csv.QuoteMode;

import de.uniwue.dw.imports.CsvFile;
import de.uniwue.dw.imports.DataElem;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.manager.ImportLogManager;

public class ConfigDataSourceCSVDir extends ConfigDataSourceFilesystem {

  protected static char default_delimiter = '\t';

  protected static QuoteMode default_quoteMode = QuoteMode.NONE;

  protected static char default_quoteChar = '\u0000';

  protected static char default_escapeChar = '\\';

  public char delimiter = '\t';

  public QuoteMode quoteMode = QuoteMode.NONE;

  public char quoteChar = '\u0000';

  public char escapeChar = '\\';
  
  public boolean firstIsHeader = true;
  
  public boolean getFilesRecursive = true;
  
  public String[] manheader;

  public ConfigDataSourceCSVDir(File importDir, String anEncoding) {
    this(importDir, ConfigDataSourceCSVDir.default_delimiter,
            ConfigDataSourceCSVDir.default_quoteMode, ConfigDataSourceCSVDir.default_escapeChar,
            anEncoding, true, true, null);
  }
  public ConfigDataSourceCSVDir(File importDir, String anEncoding, boolean firstIsHeader, boolean getFilesRecursive, String[] manualHeader) {
    this(importDir, ConfigDataSourceCSVDir.default_delimiter,
            ConfigDataSourceCSVDir.default_quoteMode, ConfigDataSourceCSVDir.default_escapeChar,
            anEncoding, firstIsHeader, getFilesRecursive, manualHeader);
  }

  public ConfigDataSourceCSVDir(File importDir) {
    this(importDir, null);
  }

  public ConfigDataSourceCSVDir(File importDir, char aDelimiter, QuoteMode aQuoteMode,
          char anEscapeChar) {
    this(importDir, aDelimiter, aQuoteMode, anEscapeChar,
            null, true, true, null);
  }

  public ConfigDataSourceCSVDir(File importDir, char aDelimiter, QuoteMode aQuoteMode,
          char anEscapeChar, String anEncoding, boolean firstIsHeader, boolean getFilesRecursive, String[] manualHeader) {
    super(null, importDir, anEncoding);
    delimiter = aDelimiter;
    quoteMode = aQuoteMode;
    escapeChar = anEscapeChar;
    this.firstIsHeader = firstIsHeader;
    this.getFilesRecursive = getFilesRecursive;
    this.manheader = manualHeader;
  }

  public ConfigDataSourceCSVDir(ConfigStructureElem aParent) {
    super(aParent);
  }


  @Override
  protected void getFilenamesRecursive(File dir, ArrayList<DataElem> result, String project)
          throws ImportException {
    String[] list = dir.list();
    if (list == null) { // this can happen when access on the directory is
      // denied
      ImportLogManager.info("Directory " + dir.getAbsolutePath() + " in project " + project
              + " could not be accessed ");
      return;
    }
    for (String aFilename : list) {
      File aFile = new File(dir, aFilename);
      if (!aFilename.contains(".")) {
        if (this.getFilesRecursive && aFile.isDirectory()) {
          getFilenamesRecursive(aFile, result, project);
        }
      } else {
        result.add(new CsvFile(aFile, this));
      }
    }
  }
}
