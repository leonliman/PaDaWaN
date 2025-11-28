package de.uniwue.dw.imports.mail;

import java.util.Comparator;

public class ImportDetailComparator implements Comparator<ImportDetail> {
  public int compare(ImportDetail det1, ImportDetail det2) {
    return det1.date.compareTo(det2.date);
  }
}
