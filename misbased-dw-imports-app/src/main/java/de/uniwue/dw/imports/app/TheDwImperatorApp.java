package de.uniwue.dw.imports.app;

import java.io.File;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.imports.manager.ImporterManager;

public class TheDwImperatorApp {

  public ImporterManager importManager;

  public static void main(String[] args) {
    ArgumentsParser parser = new ArgumentsParser();
    boolean argsOk = parser.parse(args);
    if (!argsOk) {
      System.err.println("invalid arguments");
      return;
    }
    try {
      TheDwImperatorApp imperator = new TheDwImperatorApp();
      File configFile = new File(parser.getConfigPath());
      imperator.doImport(configFile);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void doImport(File configFile)           {
    try {
      DwClientConfiguration.loadProperties(configFile);
      importManager = new ImporterManager();
      importManager.doImport();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
