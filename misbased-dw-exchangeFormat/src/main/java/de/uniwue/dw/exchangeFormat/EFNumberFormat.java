package de.uniwue.dw.exchangeFormat;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;

public interface EFNumberFormat {

  public default Double parseNumberString(String stringToParse)
          throws ParseException, NumberFormatException {
    String stringToWorkWith = stringToParse.trim();
    if (Character.isDigit(stringToWorkWith.charAt(stringToWorkWith.length() - 1))) {
      if (!Locale.getDefault().equals(Locale.GERMANY) && (stringToWorkWith.contains(",")
              || stringToWorkWith.contains("."))) {
        stringToWorkWith = stringToWorkWith.replace(",", "#MARC,#");
        stringToWorkWith = stringToWorkWith.replace(".", ",");
        stringToWorkWith = stringToWorkWith.replace("#MARC,#", ".");
      }
      return NumberFormat.getInstance().parse(stringToWorkWith).doubleValue();
    } else {
      throw new NumberFormatException("The String has to end with a numeric character");
    }
  }

}
