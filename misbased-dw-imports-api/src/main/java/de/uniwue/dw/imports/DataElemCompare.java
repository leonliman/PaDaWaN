package de.uniwue.dw.imports;

public class DataElemCompare implements Comparable<DataElemCompare> {
  
  public DataElem f;

  public long lastModified;

  public DataElemCompare(DataElem dataElem) {
    f = dataElem;
    lastModified = f.getTimestamp();
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
  public int compareTo(DataElemCompare o) {
    try {
      long u = o.getTimeFromFile();
      long t = getTimeFromFile();
      int tieBreak = 0;
      if (u == t) {
        String name = o.f.getName();
        String myName = f.getName();
        tieBreak = name.compareTo(myName);
      }
      return t < u ? -1 : t == u ? tieBreak : 1;
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    return 0;
  }
};
