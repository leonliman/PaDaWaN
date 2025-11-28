package de.uniwue.dw.core.client.authentication;

/**
 * The value of this enum indicates if the semantics of the catalogViews "count" indication for the
 * catalogEntries
 */
public enum CountType {

  //@formatter:off
  absolute("Anzahl Fakten"), 
  distinctPID("Anzahl Patienten"), 
  distinctCaseID("Anzahl Fälle");  
  //@formatter:on

  private String displayText;

  private CountType(String displayText) {
    this.displayText = displayText;
  }

  public String getDisplayText() {
    return displayText;
  }

  public static CountType parse(String countType) {
    if (countType == null)
      throw new IllegalArgumentException("Argument must not be null");
    for (CountType type : values()) {
      if (type.name().equalsIgnoreCase(countType) || type.displayText.equalsIgnoreCase(countType))
        return type;
    }
    throw new IllegalArgumentException("Unparsable argument: " + countType);
  }

}
