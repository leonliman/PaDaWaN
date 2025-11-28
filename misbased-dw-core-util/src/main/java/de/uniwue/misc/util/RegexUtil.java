package de.uniwue.misc.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexUtil {

  public static String numberReplacement = "XX";

  public static String numbersRegexString = "\\d+([\\.,]\\d+)?";

  public static String numbersRegexForRegex = "\\\\d+([\\\\.,]\\\\d+)?";

  public static String allLexicalOrNumChars = "\\d\\p{L}";

  public static Pattern allNumbersRegex = Pattern.compile("^\\d+([\\.,]\\d+)?$", Pattern.MULTILINE);

  public static Pattern containsNumbersRegex = Pattern.compile(".*\\d+([\\.,]\\d+)?.*",
          Pattern.MULTILINE);

  public static Pattern numbersRegex = Pattern.compile("\\d+([\\.,]\\d+)?", Pattern.MULTILINE);

  public static Pattern escapeStrings = Pattern.compile(".*[\\(\\)\\[\\]\\.\\?\\*\\s].*",
          Pattern.MULTILINE);

  public static Pattern allLexicalChardsPattern = Pattern.compile("^[" + allLexicalOrNumChars
          + "]*$", Pattern.MULTILINE);

  public static String startingLexicalCharAndEndingWithOne = "^[" + allLexicalOrNumChars + "].*["
          + allLexicalOrNumChars + "]$";

  public static Pattern umlautDetectPattern = Pattern.compile(".*[ÜÖÄ].*");

  public static String replaceNumbersWithXX(String aText) {
    return aText.replaceAll(numbersRegexString, numberReplacement);
  }

  public static String makeXXRegexCapable(String aText) {
    return aText.replaceAll(numberReplacement, numbersRegexForRegex);
  }

  public static boolean isNumber(String aPossibleStringNumber) {
    return numbersRegex.matcher(aPossibleStringNumber).matches();
  }
  
  public static String replaceUmlaute(String aText) {
    if (umlautDetectPattern.matcher(aText).find()) {
      aText = aText.replace("Ö", "ö");
      aText = aText.replace("Ü", "ü");
      aText = aText.replace("Ä", "ä");
    }
    return aText;
  }

  public static Matcher matchText(String aWordToDetect, String textToSearchIn) {
    if (escapeStrings.matcher(aWordToDetect).find()) {
      aWordToDetect = aWordToDetect.replace("*", "\\*");
      aWordToDetect = aWordToDetect.replace("?", "\\?");
      aWordToDetect = aWordToDetect.replace(".", "\\.");
      aWordToDetect = aWordToDetect.replace("(", "\\(");
      aWordToDetect = aWordToDetect.replace(")", "\\)");
      aWordToDetect = aWordToDetect.replace("[", "\\[");
      aWordToDetect = aWordToDetect.replace("]", "\\]");
    }
    aWordToDetect = "(^|[^" + allLexicalOrNumChars + "])(" + aWordToDetect;
    aWordToDetect = aWordToDetect + ")([^" + allLexicalOrNumChars + "]|$)";
    Pattern regex = Pattern.compile(aWordToDetect, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    Matcher matches = regex.matcher(textToSearchIn);
    return matches;
  }

  public static String trimNumberString(String value) {
    String strippedValue = value.replaceAll("[\r\n]", "");
    if (strippedValue.matches(".*\\d.*")) {
      strippedValue = strippedValue.replaceAll("\\(.*\\)", "");
      strippedValue = strippedValue.replaceAll(",", ".");
      if (strippedValue.matches("\\d+(\\.\\d*)?-\\d+(\\.\\d*)?")) {
        // bei Zahlen die ein Interval angeben, wird der Mittelwert genommen.
        // sehr willkuerlich, aber ich weiss mir nicht besser zu helfen...
        String first = strippedValue.replaceAll("-\\d+(\\.\\d*)?", "");
        String second = strippedValue.replaceAll("\\d+(\\.\\d*)?-", "");
        strippedValue = Double
                .toString((Double.parseDouble(first) + Double.parseDouble(second)) / 2.0);
      } else {
        strippedValue = strippedValue.replaceAll("[^\\d\\.]", "").trim();
        // trailing , and . are removed
        strippedValue = strippedValue.replaceAll("[,\\.]$", "");
      }
    }
    return strippedValue;
  }
  
  public static double parseNumber(String numberString, boolean clean) {
    double result = Double.NaN;
    if (clean) {
      numberString = cleanNumber(numberString);
    }
    if (numberString.matches(RegexUtil.numbersRegexString)) {
      try {
        result = Double.parseDouble(numberString);
      } catch (NumberFormatException e) {
      }
    }    
    return result;
  }

  public static String cleanNumber(String aNumberString) {
    String result = aNumberString;
    result = result.replaceAll("[^0-9,\\.\\-]", "");
    result = result.replaceAll(",", ".");
    return result;
  }

  
}
