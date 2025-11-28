package de.uniwue.dw.core.model.manager;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class ValueIterator implements Iterator<String>, Iterable<String> {

  boolean isDisposed = false;
  
  public void dispose() throws DWIterException {
    isDisposed = true;
  }
  
  public List<String> getInfos() throws DWIterException {
    List<String> result = new ArrayList<String>();
    for (String anInfo : this) {
      result.add(anInfo);
    }
    dispose();
    return result;
  }
  
  public Iterator<String> iterator() {
    return this;
  }
  
}
