package de.uniwue.misc.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class StringUtilsUniWue {

  public static void lowerCaseCollection(Collection<String> aCol) {
    for (String aColumn : aCol.toArray(new String[0])) {
      aCol.remove(aColumn);
      aCol.add(aColumn.toLowerCase());
    }
  }
  
  public static String cleanStringFromSpecialCharacters(String extID) {
    String result = extID;
    result = result.replaceAll("[^a-zA-Z0-9_]", "_");
    return result;
  }
  
  public static String concat(String[] someStrings, String separator) {
    if (someStrings == null)
      return null;
    StringBuilder builder = new StringBuilder();
    for (String aString : someStrings) {
      builder.append(aString);
      builder.append(separator);
    }
    if (builder.length() > 0) {
      builder.delete(builder.length() - separator.length(), builder.length());
    }
    return builder.toString();
  }

  public static String concatLongs(Collection<Long> someLongs, String separator) {
    List<String> stringList = new ArrayList<String>();
    for (Long aLong : someLongs) {
      stringList.add(Long.toString(aLong));
    }
    return concat(stringList, separator);
  }

  public static String concatInts(Collection<Integer> someInts, String separator) {
    List<String> stringList = new ArrayList<String>();
    for (Integer anInt : someInts) {
      stringList.add(Integer.toString(anInt));
    }
    return concat(stringList, separator);
  }

  public static String concatObjects(Collection<Object> objects, String separator) {
    List<String> stringList = new ArrayList<String>();
    for (Object object : objects) {
      if (object == null)
        object = "";
      stringList.add(object.toString());
    }
    return concat(stringList, separator);
  }

  public static String concat(Collection<String> someStrings, String separator) {
    return concat(someStrings.toArray(new String[0]), separator);
  }

  public static String arffName(String aName) {
    String result = aName.replaceAll("ä", "ae");
    result = result.replaceAll("ö", "oe");
    result = result.replaceAll("ü", "ue");
    result = result.replaceAll("ß", "ss");
    result = result.replaceAll("[^\\w]", "_");
    // ( |-|/|:|=|%|\\[\\]|\\(|\\)|\\{|\\}|,|\\[|\\|°|'])
    result = result.replaceAll("^_*", "");
    result = result.replaceAll("_+", "_");
    result = result.replaceAll("_*$", "");
    return result;
  }

}
