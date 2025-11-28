package de.uniwue.misc.util;

public class ConfigException extends Exception {

  private static final long serialVersionUID = 8243517271738803931L;

  public String parameter;
  
  public ConfigException(String aParamter) {
    parameter = aParamter;
  }
  
  public ConfigException(Exception e) {
    super(e);
  }
  
  public ConfigException(String message, Throwable e) {
    super(message,e);
    parameter = message;
  }
  
}
