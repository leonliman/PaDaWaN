package de.uniwue.dw.query.model.result;

import de.uniwue.dw.core.model.data.Information;
import de.uniwue.dw.query.model.lang.QueryAttribute;

import java.util.ArrayList;
import java.util.List;

public class ResultCellData {

  private List<Information> values = new ArrayList<>();

  public QueryAttribute attribute;

  public ResultCellData(ResultCellData aCellToCopy) {
    for (Information anInfo : aCellToCopy.getValues()) {
      values.add(new Information(anInfo));
    }
    attribute = aCellToCopy.attribute;
  }

  public ResultCellData(Information anInfo, QueryAttribute anAttribute) {
    values.add(anInfo);
    attribute = anAttribute;
  }

  public ResultCellData(List<Information> infos, QueryAttribute anAttribute) {
    if (infos != null) {
      values.addAll(infos);
    }
    attribute = anAttribute;
  }

  public ResultCellData(QueryAttribute anAttribute) {
    this(new ArrayList<Information>(), anAttribute);
  }

  public boolean hasMoreThanOneValue() {
    return values.size() > 1;
  }

  public Information getValue() {
    if (!values.isEmpty()) {
      return values.get(0);
    } else {
      return null;
    }
  }

  public List<Information> getValues() {
    return values;
  }

  public void clearValues() {
    values.clear();
  }

  public void removeValue(Information anInfo) {
    values.remove(anInfo);
  }

  public void removeAllValues() {
    values.clear();
  }

  public void addValue(Information anInfo) {
    values.add(anInfo);
  }

  @Override
  public String toString() {
    if (hasMoreThanOneValue()) {
      StringBuilder builder = new StringBuilder();
      for (Information anInfo : values) {
        String value = anInfo.getValueShort();
        if (value == null) {
          value = Double.toString(anInfo.getValueDec());
        }
        builder.append(value + "; ");
      }
      return builder.toString();
    } else {
      Information singleInfo = getValue();
      if (singleInfo == null) {
        return "null-value";
      } else {
        String value = singleInfo.getValueShort();
        if (value == null) {
          value = Double.toString(singleInfo.getValueDec());
        }
        return value;
      }
    }
  }

}
