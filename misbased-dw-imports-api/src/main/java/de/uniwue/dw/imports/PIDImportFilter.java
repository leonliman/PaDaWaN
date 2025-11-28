package de.uniwue.dw.imports;

import de.uniwue.dw.imports.api.DWImportsConfig;
import de.uniwue.misc.util.FileUtilsUniWue;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class PIDImportFilter {

  private static PIDImportFilter singleton;

  public static PIDImportFilter getInstance() {
    if (singleton == null) {
      singleton = new PIDImportFilter();
    }
    return singleton;
  }

  private Set<Long> pids;

  private Set<Long> getPIDs() {
    if (pids == null) {
      File pidFilterFile = DWImportsConfig.getPIDFilterFile();
      if (pidFilterFile != null) {
        pids = new HashSet<Long>();
        try {
          String file2String = FileUtilsUniWue.file2String(pidFilterFile);
          String[] pidLines = file2String.split("\n");
          for (String aPIDString : pidLines) {
            Long aPID = Long.valueOf(aPIDString.trim());
            pids.add(aPID);
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return pids;
  }

  public boolean checkPID(long pid) {
    Set<Long> pids = getPIDs();
    if (pids == null) {
      return true; // there is not filter
    } else {
      return pids.contains(pid);
    }
  }

  public void clear() {
    if (pids != null) {
      pids.clear();
      pids = null;
    }
  }

}
