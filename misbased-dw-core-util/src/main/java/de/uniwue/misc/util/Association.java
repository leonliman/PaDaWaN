package de.uniwue.misc.util;

public class Association<T, S> {

  public T key;

  public S value;

  public Association(T aKey, S aValue) {
    key = aKey;
    value = aValue;
  }

  @Override
  public String toString() {
    return key.toString() + " -> " + value.toString();
  }
  
}
