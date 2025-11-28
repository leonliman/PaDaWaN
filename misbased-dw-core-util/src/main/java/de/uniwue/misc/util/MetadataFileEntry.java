package de.uniwue.misc.util;

public class MetadataFileEntry {

  private String feldbezeichner;

  private String feldname;

  private String zusatzinformation;

  private boolean ignore;

  private String datentyp;

  private int laenge;

  private String synonym;

  private String einheit;

  public String getFeldbezeichner() {
    return feldbezeichner;
  }

  public void setFeldbezeichner(String feldbezeichner) {
    this.feldbezeichner = feldbezeichner;
  }

  public String getFeldname() {
    return feldname;
  }

  public void setFeldname(String feldname) {
    this.feldname = feldname;
  }

  public String getZusatzinformation() {
    return zusatzinformation;
  }

  public void setZusatzinformation(String zusatzinformation) {
    this.zusatzinformation = zusatzinformation;
  }

  public boolean isIgnore() {
    return ignore;
  }

  public void setIgnore(boolean ignore) {
    this.ignore = ignore;
  }

  public String getDatentyp() {
    return datentyp;
  }

  public void setDatentyp(String datentyp) {
    this.datentyp = datentyp;
  }

  public int getLaenge() {
    return laenge;
  }

  public void setLaenge(int laenge) {
    this.laenge = laenge;
  }

  public String getSynonym() {
    return synonym;
  }

  public void setSynonym(String synonym) {
    this.synonym = synonym;
  }

  public String getEinheit() {
    return einheit;
  }

  public void setEinheit(String einheit) {
    this.einheit = einheit;
  }

  @Override
  public String toString() {
    return "MetadataFileEntry [feldbezeichner=" + feldbezeichner + ", feldname=" + feldname
            + ", ignore=" + ignore + "]";
  }

}
