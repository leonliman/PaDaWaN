package de.uniwue.dw.core.model.manager;

public abstract class AbstractCatalogClientManager implements ICatalogClientManager {

  private boolean isDisposed = false;
  
  @Override
  public boolean isDisposed() {
    return isDisposed;
  }

  @Override
  public void dispose() {
    isDisposed = true;
  }

}
