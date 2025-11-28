package de.uniwue.dw.imports.data;

import java.io.File;

public class ImportedFileData {

  private String filename;

  private String project;

  private long lastModifyTime;

  public ImportedFileData() {
  }

  public ImportedFileData(File aFile, long aLastModDate, String aProject) {
    filename = aFile.getName();
    lastModifyTime = aLastModDate;
    project = aProject;
  }

  public String getFilename() {
    return filename;
  }

  public String getProject() {
    return project;
  }

  public long getLastModTime() {
    return lastModifyTime;
  }

}