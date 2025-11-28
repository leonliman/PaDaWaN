package de.uniwue.dw.core.model.manager;

import java.util.Iterator;
import java.util.List;

import de.uniwue.dw.core.model.data.Information;

public class FixedInfoIterator extends InfoIterator {

  private Iterator<Information> iter;
  
  public FixedInfoIterator(List<Information> someInfos) {
    iter = someInfos.iterator();
  }
  
  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public Information next() {
    return iter.next();
  }

}
