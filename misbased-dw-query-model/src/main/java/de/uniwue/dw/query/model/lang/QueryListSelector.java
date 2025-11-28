package de.uniwue.dw.query.model.lang;

public class QueryListSelector {

  public int topN;

  public QuerySortOrder sortOrder = QuerySortOrder.highToLow;

  public static enum QuerySortOrder {
    lowToHigh, highToLow
  }

  public QueryOrderAttribute orderAttribute = QueryOrderAttribute.time;

  public static enum QueryOrderAttribute {
    time, value
  }

  public int beginWithNth;

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + beginWithNth;
    result = prime * result + ((orderAttribute == null) ? 0 : orderAttribute.hashCode());
    result = prime * result + ((sortOrder == null) ? 0 : sortOrder.hashCode());
    result = prime * result + topN;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    QueryListSelector other = (QueryListSelector) obj;
    if (beginWithNth != other.beginWithNth)
      return false;
    if (orderAttribute != other.orderAttribute)
      return false;
    if (sortOrder != other.sortOrder)
      return false;
    if (topN != other.topN)
      return false;
    return true;
  }

}
