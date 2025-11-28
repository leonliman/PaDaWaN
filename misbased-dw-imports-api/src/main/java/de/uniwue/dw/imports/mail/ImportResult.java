package de.uniwue.dw.imports.mail;

import java.util.ArrayList;
import java.util.List;

class ImportResult {
  public String project = "";

  public List<ImportDetail> detail = new ArrayList<ImportDetail>();

  public List<ImportDetail> detailErroneous = new ArrayList<ImportDetail>();

  public String state = "";

  public String detailedState = "";

  public List<Integer> prevCounts = new ArrayList<Integer>();

  public Integer yesterdayCount = 0;

  public double mean = 0;

  public double variance = 0;

  public double stdev = 0;

  public boolean yesterdayWarn = false;

  public long lastImportSinceDays = 0;

  public boolean lastImportError = false;

  public Integer lastImportCount = 0;

  public boolean lastImportCountWarn = false;

  public boolean stateError = false;

  public boolean detailedStateError = false;

}
