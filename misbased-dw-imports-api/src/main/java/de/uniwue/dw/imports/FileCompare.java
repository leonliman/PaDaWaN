package de.uniwue.dw.imports;

import java.io.File;

public class FileCompare implements Comparable {
  public File f;
  public long lastModified;

  public FileCompare(File file) {
    f = file;
    lastModified = f.lastModified();
  }

  public long getTimeFromFile() throws Exception {
    /*
     * URI pu = f.toURI(); Path fn = Paths.get(pu); BasicFileAttributes attr =
     * Files.readAttributes(fn, BasicFileAttributes.class); return
     * attr.lastModifiedTime().toMillis();
     */

    // last modify date seems to be the only timestamp that is not changed after moving files
    return lastModified;
  }

  @Override
  public int compareTo(Object o) {
    try {
      long u = ((FileCompare) o).getTimeFromFile();
      long t = getTimeFromFile();
      return t < u ? -1 : t == u ? 0 : 1;
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    return 0;
  }
};
