package de.uniwue.dw.core.model.manager;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import de.uniwue.dw.core.model.data.Information;

public abstract class InfoIterator implements Iterator<Information>, Iterable<Information> {

  boolean isDisposed = false;
  
  public void dispose() throws DWIterException {
    isDisposed = true;
  }
  
  public List<Information> getInfos() throws DWIterException {
    List<Information> result = new ArrayList<Information>();
    for (Information anInfo : this) {
      result.add(anInfo);
    }
    dispose();
    return result;
  }
  
  public Iterator<Information> iterator() {
    return this;
  }
  
}
