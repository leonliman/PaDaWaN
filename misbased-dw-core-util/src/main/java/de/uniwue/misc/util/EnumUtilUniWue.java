package de.uniwue.misc.util;

import java.util.ArrayList;
import java.util.List;

public class EnumUtilUniWue {

  public static <T extends Enum<T>> String[] convertEnumsToStrings(Enum<T>[] values) {
    String[] list = new String[values.length];
    for (int i = 0; i < values.length; i++) {
      list[i] = values[i].toString();
    }
    return list;
  }

  public static <T extends Enum<T>> List<String> convertEnumsToStringList(Enum<T>[] values) {
    String[] strings = convertEnumsToStrings(values);
    List<String> result = new ArrayList<String>();
    for (String aString : strings) {
      result.add(aString);
    }
    return result;
  }

}
