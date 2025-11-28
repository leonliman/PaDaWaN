package de.uniwue.misc.util;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TimeUtil {

  public static String sdf_withTimeString_SQL_Timestamp = "yyyy-MM-dd HH:mm:ss";

  public static String sdf_withTimeString_withoutDots = "yyyyMMdd HHmmss";

  public static String sdf_withTimeString_withoutDots_withoutSeconds = "yyyyMMdd HHmm";

  public static String sdf_withTimeString = "dd.MM.yyyy HH:mm:ss";

  public static String sdf_withTimeWithMillisecondsString = "yyyy-MM-dd HH:mm:ss.SSSSSS";

  public static String sdf_withoutTimeString = "dd.MM.yyyy";

  public static String sdf_onlyTimeString = "HH:mm:ss";

  public static String sdf_withoutTimeStringRegex = "^\\d{2}\\.\\d{2}\\.\\d{4}$";

  public static String sdf_withTimeStringRegex = "^\\d{2}\\.\\d{2}\\.\\d{4} \\d{2}:\\d{2}:\\d{2}$$";

  @Deprecated
  public static SimpleDateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  @Deprecated
  public static SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

  @Deprecated
  public static SimpleDateFormat dateFormat3 = new SimpleDateFormat("dd.MM.yyyy");

  @Deprecated
  public static SimpleDateFormat dateFormat4 = new SimpleDateFormat("dd.MM.yyyy HH:mm");

  @Deprecated
  public static SimpleDateFormat dateFormat5 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

  @Deprecated
  public static SimpleDateFormat dateFormat6 = new SimpleDateFormat("yyyy-MM-dd HH:mm");

  @Deprecated
  public static SimpleDateFormat dateFormat7 = new SimpleDateFormat("yyyy-MM-dd");

  @Deprecated
  public static SimpleDateFormat dateFormat8 = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");

  @Deprecated
  public static SimpleDateFormat dateFormat9 = new SimpleDateFormat("yyyy");

  @Deprecated
  public static SimpleDateFormat dateFormat10 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  @Deprecated
  public static SimpleDateFormat dateFormat11 = new SimpleDateFormat(sdf_withTimeWithMillisecondsString);

  @Deprecated
  public static SimpleDateFormat dateFormat12 = new SimpleDateFormat("MM/dd/yy hh:mm:ss a");

  @Deprecated
  public static SimpleDateFormat sdf_withTime_SQL_Timestamp = new SimpleDateFormat(sdf_withTimeString_SQL_Timestamp);

  @Deprecated
  public static SimpleDateFormat sdf_withTime_withoutDots = new SimpleDateFormat(sdf_withTimeString_withoutDots);

  @Deprecated
  public static SimpleDateFormat sdf_withTime_withoutDots_withoutSeconds = new SimpleDateFormat(
          sdf_withTimeString_withoutDots_withoutSeconds);

  @Deprecated
  public static SimpleDateFormat sdf_withTime = new SimpleDateFormat(sdf_withTimeString);

  @Deprecated
  public static SimpleDateFormat sdf_withoutTime = new SimpleDateFormat(sdf_withoutTimeString);

  // public static Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+1"));
  @Deprecated
  public static Calendar cal = Calendar.getInstance();

  private static SimpleDateFormat sdfWithTimeSQLTimestamp;

  static {
    dateFormat5.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  public static String getTimeString(long aTime) {
    GregorianCalendar calendar = new GregorianCalendar();
    calendar.setTimeInMillis(aTime);
    return getSdfWithTime().format(calendar.getTime());
  }

  public static Timestamp parseTimestamp(SimpleDateFormat format, String value) throws ParseException {
    Date date = format.parse(value);
    return new Timestamp(date.getTime());
  }

  public static String currentTime() {
    return getDateFormat10().format(new Date(System.currentTimeMillis()));
  }

  public static String format(long time) {
    return format(new Date(time));
  }

  public static String format(Date d) {
    // The following replaceAll is a workaround for a change in Java 20 and later (see https://bugs.openjdk.org/browse/JDK-8304925)
    return getDateFormat().format(d).replaceAll("\u202F", " ");
  }

  public static String format2GermanFormat(Date date) {
    return getDateFormat3().format(date);
  }

  public static List<Date> parseDates(String[] values) throws ParseException {
    List<Date> result = new ArrayList<>();
    for (String value : values) {
      Date date = parseDate(value);
      if (date == null) {
        throw new ParseException("Argument hat no valid date format. Argument: " + value, 0);
      }
      result.add(date);
    }
    return result;
  }

  public static Date parseDate(String value) {
    value = value.trim();
    try {
      if (value.matches("\\d{4}-\\d{1,2}-\\d{1,2}T\\d{2}:\\d{2}:\\d{2}Z")) {
        return getDateFormat5().parse(value);
      } else if (value.matches("\\d{8}")) {
        return getDateFormat3RevertedAndWithoutDots().parse(value);
      } else if (value.matches("\\d{1,2}.\\d{1,2}.\\d{4}")) {
        return getDateFormat3().parse(value);
      } else if (value.matches("\\d{1,2}.\\d{1,2}.\\d{4} \\d{1,2}:\\d{1,2}")) {
        return getDateFormat4().parse(value);
      } else if (value.matches("\\d{1,2}.\\d{1,2}.\\d{4} \\d{1,2}:\\d{1,2}:\\d{1,2}")) {
        return getDateFormat8().parse(value);
      } else if (value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")) {
        return getDateFormat1().parse(value);
      } else if (value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}")) {
        return getDateFormat6().parse(value);
      } else if (value.matches("\\d{2}-\\d{2}-\\d{2} \\d{2}:\\d{2}")) {
        return getDateFormat6ShortYear().parse(value);
      } else if (value.matches("\\d{4}-\\d{2}-\\d{2}")) {
        return getDateFormat7().parse(value);
      } else if (value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.0")) {
        return getDateFormat2().parse(value);
      } else if (value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{1,3}")) {
        return getDateFormat10().parse(value);
      } else if (value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{6}")) {
        return getDateFormat11().parse(value);
      } else if (value.matches("\\d{4}")) {
        return getDateFormat9().parse(value);
      } else if (value.matches("\\d{1,2}.\\d{1,2}.\\d{2},? \\d{2}:\\d{2}:\\d{2}")) {
        return getDateFormat().parse(value);
      } else if (value.matches("\\d{1,2}/\\d{1,2}/\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2} [AP]M")) {
        return getDateFormat12().parse(value);
      } else if (value.matches("\\d{1,2}/\\d{1,2}/\\d{1,2} \\d{1,2}:\\d{1,2}")) {
        return getDateFormat13().parse(value);
      } else if (value.matches("\\d{1,2}/\\d{1,2}/\\d{1,2}")) {
        return getDateFormat13WithoutTime().parse(value);
      } else if (value.matches("\\d{1,2}/\\d{1,2}/\\d{1,2}, \\d{1,2}:\\d{1,2}:\\d{1,2} [AP]M")) {
        try {
          return getDateFormat(Locale.ENGLISH).parse(value);
        } catch (ParseException e) {
          // The following replaceAll is a workaround for a change in Java 20 and later (see https://bugs.openjdk.org/browse/JDK-8304925)
          String valueForJava20AndLater = value.replaceAll(" AM", "\u202FAM").replaceAll(" PM", "\u202FPM");
          return getDateFormat(Locale.ENGLISH).parse(valueForJava20AndLater);
        }
      } else if (value.matches("\\d{8} \\d{4}")) {
        return getSdfWithTimeWithoutDotsWithoutSeconds().parse(value);
      }
    } catch (ParseException e) {
      System.err.println("input: " + value);
      e.printStackTrace();
    }
    return null;
  }

  public static Calendar parseDate2Calendar(String value) {
    Date date = parseDate(value);
    Calendar cal = new GregorianCalendar();
    cal.setTime(date);
    return cal;
  }

  public static Long getUnixTime(Date date) {
    if (date != null)
      return date.getTime() / 1000;
    return null;
  }

  public static DateFormat getDateFormat() {
    return DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM);
  }

  public static DateFormat getDateFormat(Locale locale) {
    return DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM, locale);
  }

  public static SimpleDateFormat getDateFormat1() {
    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  }

  public static SimpleDateFormat getDateFormat2() {
    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
  }

  public static SimpleDateFormat getDateFormat3() {
    return new SimpleDateFormat("dd.MM.yyyy");
  }

  public static SimpleDateFormat getDateFormat3RevertedAndWithoutDots() {
    return new SimpleDateFormat("yyyyMMdd");
  }

  public static SimpleDateFormat getDateFormat4() {
    return new SimpleDateFormat("dd.MM.yyyy HH:mm");
  }

  public static SimpleDateFormat getDateFormat5() {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    return simpleDateFormat;
  }

  public static SimpleDateFormat getDateFormat6() {
    return new SimpleDateFormat("yyyy-MM-dd HH:mm");
  }

  public static SimpleDateFormat getDateFormat6ShortYear() {
    return new SimpleDateFormat("yy-MM-dd HH:mm");
  }

  public static SimpleDateFormat getDateFormat7() {
    return new SimpleDateFormat("yyyy-MM-dd");
  }

  public static SimpleDateFormat getDateFormat8() {
    return new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
  }

  public static SimpleDateFormat getDateFormat9() {
    return new SimpleDateFormat("yyyy");
  }

  public static SimpleDateFormat getDateFormat10() {
    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  }

  public static SimpleDateFormat getDateFormat11() {
    return new SimpleDateFormat(sdf_withTimeWithMillisecondsString);
  }

  public static SimpleDateFormat getDateFormat12() {
    return new SimpleDateFormat("MM/dd/yy hh:mm:ss a");
  }

  public static SimpleDateFormat getDateFormat13() {
    return new SimpleDateFormat("MM/dd/yy HH:mm");
  }

  public static SimpleDateFormat getDateFormat13WithoutTime() {
    return new SimpleDateFormat("MM/dd/yy");
  }

  public static SimpleDateFormat getSdfWithTimeSQLTimestamp() {
    if (sdfWithTimeSQLTimestamp == null) {
      sdfWithTimeSQLTimestamp = new SimpleDateFormat(sdf_withTimeString_SQL_Timestamp);
    }
    return sdfWithTimeSQLTimestamp;
  }

  public static SimpleDateFormat getSdfWithTimeWithoutDots() {
    return new SimpleDateFormat(sdf_withTimeString_withoutDots);
  }

  public static SimpleDateFormat getSdfWithTimeWithoutDotsWithoutSeconds() {
    return new SimpleDateFormat(sdf_withTimeString_withoutDots_withoutSeconds);
  }

  public static SimpleDateFormat getSdfWithTime() {
    return new SimpleDateFormat(sdf_withTimeString);
  }

  public static SimpleDateFormat getSdfWithoutTime() {
    return new SimpleDateFormat(sdf_withoutTimeString);
  }
}
