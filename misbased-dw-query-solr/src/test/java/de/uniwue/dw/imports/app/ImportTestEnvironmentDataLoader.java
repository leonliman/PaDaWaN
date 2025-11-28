package de.uniwue.dw.imports.app;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.query.model.TestEnvironmentDataLoader;

public abstract class ImportTestEnvironmentDataLoader extends TestEnvironmentDataLoader {

  private static Logger logger = LogManager.getLogger(ImportTestEnvironmentDataLoader.class);

  public void importData() throws IOException, URISyntaxException {
    File configFile = getConfigFile();
    logger.info("using config file: " + configFile);
    new TheDwImperatorApp().doImport(configFile);
  }

}
