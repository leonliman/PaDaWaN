package de.uniwue.dw.exchangeFormat;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.misc.util.ConfigException;

public class EFExample {

  public static void main(String[] args)
          throws SQLException, IOException, NumberFormatException, ParseException, ConfigException {
    String configFilePath = "<Path to the configuration file>";
    DwClientConfiguration.loadProperties(new File(configFilePath));

    String catalogPath = "<Optional Path to catalog.csv>";
    String factsPath = "<Path to facts.csv>";
    String metadataPath = "<Optional Path to metadata.csv>";

    String defaultProjectName = "<Optional default project for catalog entries without project>";

    boolean printResultToConsole = true;

    // Used to export an existing PaDaWaN database
    EFExport.doExport(catalogPath, factsPath, printResultToConsole);

    // Used to import csv-Files into a PaDaWaN database (overwrites the complete database)
    EFImport.doImport(catalogPath, factsPath, metadataPath, defaultProjectName,
            printResultToConsole);
  }

}
