package de.uniwue.dw.exchangeFormat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public interface EFDateTimeFormat {

  public static String dottedDateString = "dd.MM.yyyy";

  public static String dottedDateRegexString = "\\d{2}\\.\\d{2}\\.\\d{4}";

  public static String dashedDateString = "yyyy-MM-dd";

  public static String dashedDateRegexString = "\\d{4}-\\d{2}-\\d{2}";

  public static String hoursAndMinutesString = " HH:mm";

  public static String hoursAndMinutesRegexString = " \\d{2}:\\d{2}";

  public static String secondsString = ":ss";

  public static String secondsRegexString = ":\\d{2}";

  public static String millisecondsString = ".S";

  public static String millisecondsRegexString = "\\.\\d{1,3}";

  public static SimpleDateFormat dottedDateOnlyFormat = new SimpleDateFormat(dottedDateString);

  public static String dottedDateOnlyRegexString = dottedDateRegexString;

  public static SimpleDateFormat dottedDateWithHoursAndMinutesFormat = new SimpleDateFormat(
          dottedDateString + hoursAndMinutesString);

  public static String dottedDateWithHoursAndMinutesRegexString = dottedDateRegexString
          + hoursAndMinutesRegexString;

  public static SimpleDateFormat dottedDateWithHoursAndMinutesAndSecondsFormat = new SimpleDateFormat(
          dottedDateString + hoursAndMinutesString + secondsString);

  public static String dottedDateWithHoursAndMinutesAndSecondsRegexString = dottedDateRegexString
          + hoursAndMinutesRegexString + secondsRegexString;

  public static SimpleDateFormat dottedCompleteFormat = new SimpleDateFormat(
          dottedDateString + hoursAndMinutesString + secondsString + millisecondsString);

  public static String dottedCompleteFormatRegexString = dottedDateRegexString
          + hoursAndMinutesRegexString + secondsRegexString + millisecondsRegexString;

  public static SimpleDateFormat dashedDateOnlyFormat = new SimpleDateFormat(dashedDateString);

  public static String dashedDateOnlyRegexString = dashedDateRegexString;

  public static SimpleDateFormat dashedDateWithHoursAndMinutesFormat = new SimpleDateFormat(
          dashedDateString + hoursAndMinutesString);

  public static String dashedDateWithHoursAndMinutesRegexString = dashedDateRegexString
          + hoursAndMinutesRegexString;

  public static SimpleDateFormat dashedDateWithHoursAndMinutesAndSecondsFormat = new SimpleDateFormat(
          dashedDateString + hoursAndMinutesString + secondsString);

  public static String dashedDateWithHoursAndMinutesAndSecondsRegexString = dashedDateRegexString
          + hoursAndMinutesRegexString + secondsRegexString;

  public static SimpleDateFormat dashedCompleteFormat = new SimpleDateFormat(
          dashedDateString + hoursAndMinutesString + secondsString + millisecondsString);

  public static String dashedCompleteFormatRegexString = dashedDateRegexString
          + hoursAndMinutesRegexString + secondsRegexString + millisecondsRegexString;

  public static SimpleDateFormat defaultFormat = dottedCompleteFormat;

  public default Date parseDateString(String stringToParse) throws ParseException {
    return parseDateString(stringToParse, null);
  }

  public default Date parseDateString(String stringToParse,
          SimpleDateFormat additionalPossibleFormat) throws ParseException {
    String stringToWorkWith = stringToParse.trim();
    if (additionalPossibleFormat != null && !additionalPossibleFormat.equals(defaultFormat)) {
      try {
        return additionalPossibleFormat.parse(stringToWorkWith);
      } catch (Exception e) {
        // Do nothing and try default formats
      }
    }
    if (stringToWorkWith.matches(dottedCompleteFormatRegexString)) {
      return dottedCompleteFormat.parse(stringToWorkWith);
    } else if (stringToWorkWith.matches(dottedDateWithHoursAndMinutesAndSecondsRegexString)) {
      return dottedDateWithHoursAndMinutesAndSecondsFormat.parse(stringToWorkWith);
    } else if (stringToWorkWith.matches(dottedDateWithHoursAndMinutesRegexString)) {
      return dottedDateWithHoursAndMinutesFormat.parse(stringToWorkWith);
    } else if (stringToWorkWith.matches(dottedDateOnlyRegexString)) {
      return dottedDateOnlyFormat.parse(stringToWorkWith);
    } else if (stringToWorkWith.matches(dashedCompleteFormatRegexString)) {
      return dashedCompleteFormat.parse(stringToWorkWith);
    } else if (stringToWorkWith.matches(dashedDateWithHoursAndMinutesAndSecondsRegexString)) {
      return dashedDateWithHoursAndMinutesAndSecondsFormat.parse(stringToWorkWith);
    } else if (stringToWorkWith.matches(dashedDateWithHoursAndMinutesRegexString)) {
      return dashedDateWithHoursAndMinutesFormat.parse(stringToWorkWith);
    } else if (stringToWorkWith.matches(dashedDateOnlyRegexString)) {
      return dashedDateOnlyFormat.parse(stringToWorkWith);
    } else {
      throw new ParseException("The format of the supplied DateTime-String is not supported", 0);
    }
  }

  public default String getFormattedDateAsString(Date dateToFormat) {
    return getFormattedDateAsString(dateToFormat, null);
  }

  public default String getFormattedDateAsString(Date dateToFormat, SimpleDateFormat formatToUse) {
    if (formatToUse != null) {
      return formatToUse.format(dateToFormat);
    } else {
      return defaultFormat.format(dateToFormat);
    }
  }

}
