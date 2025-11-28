package de.uniwue.dw.core.model.data;

public enum CatalogEntryType {

  //@formatter:off
  SingleChoice("SingleChoice"), 
  Text("Text"), 
  Number("Nummer","number"), 
  Bool("Bool(Wahr/Falsch)","bool","boolean"), 
  Structure("Struktur","structure"), 
  isA("isA"), 
  DateTime("Datum","date");
//@formatter:on

  private String displayString;

  private String[] alternativeNames = {};

  private CatalogEntryType(String displayString) {
    this.displayString = displayString;
  }

  private CatalogEntryType(String displayString, String... alternativeNames) {
    this.displayString = displayString;
    this.alternativeNames = alternativeNames;
  }

  public String getDisplayString() {
    return displayString;
  }

  public static CatalogEntryType parse(String s) {
    for (CatalogEntryType type : CatalogEntryType.values()) {
      if (type.toString().equalsIgnoreCase(s) || type.getDisplayString().equalsIgnoreCase(s))
        return type;
      else {
        for (String alternativeName : type.alternativeNames) {
          if (alternativeName.equalsIgnoreCase(s))
            return type;
        }
      }
    }
    throw new IllegalArgumentException("CatalogEntryType unknown for arguement: " + s);
  }

}
