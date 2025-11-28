package de.uniwue.dw.imports.app;

import java.io.File;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

@SuppressWarnings("all")
public class ArgumentsParser {

  private static final String ARG_CONFIG = "config";

  private static final String OPT_KEY_CONFIG = "config";

  private static final String OPT_KEY_BOOT = "b";

  private static final String OPT_KEY_INFOS = "i";

  private static final String ARG_BOOT = "true/false";

  private static final String ARG_INFOS = "true/false";

  private Options options;

  private String configPath;

  private Boolean bootOverrideSetting;

  private Boolean infosOverrideSetting;

  private String filter;

  public ArgumentsParser() {
    initOptions();
  }

  private void initOptions() {
    options = new Options();
    //
    {
      Option opt = OptionBuilder.withArgName(ARG_CONFIG).hasArg().isRequired(true)
              .withDescription("path to a configuration properties file").withType(File.class)
              .create(OPT_KEY_CONFIG);
      options.addOption(opt);
    }
    {
      Option opt = OptionBuilder.withArgName(ARG_BOOT).hasArg().isRequired(false)
              .withDescription("force/prevent bootstrap step (overrides config setting)")
              .create(OPT_KEY_BOOT);
      options.addOption(opt);
    }
    {
      Option opt = OptionBuilder.withArgName(ARG_INFOS).hasArg().isRequired(false)
              .withDescription("force/prevent info imports (overrides config setting)")
              .create(OPT_KEY_INFOS);
      options.addOption(opt);
    }
  }

  private void printUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("TheDwImperatorApp", options);
  }

  /**
   * 
   * @param args
   * @return true, iff application should be run (valid options, no help requested)
   */
  public boolean parse(String[] args) {
    if (args.length == 0) {
      printUsage(options);
      return false;
    }
    CommandLineParser parser = new BasicParser();
    try {
      CommandLine cmd = parser.parse(options, args);
      { // input:
        File dir = (File) cmd.getParsedOptionValue(OPT_KEY_CONFIG);
        if (dir != null) {
          configPath = dir.toPath().toAbsolutePath().normalize().toString();
        } else {
          throw new ParseException("config file path not valid");
        }
      }
      { // force/prevent properties settings:
        String bootOv = cmd.getOptionValue(OPT_KEY_BOOT);
        bootOverrideSetting = bootOv == null ? null : Boolean.parseBoolean((String) bootOv);
        Object infosOv = cmd.getOptionValue(OPT_KEY_INFOS);
        infosOverrideSetting = infosOv == null ? null : Boolean.parseBoolean((String) infosOv);
      }
    } catch (ParseException e) {
      System.out.println();
      System.out.printf("%s%n", e.getMessage());
      System.out.println("Please follow the instructions below.");
      System.out.println("");
      printUsage(options);
      return false;
    }
    return true;
  }

  public String getConfigPath() {
    return configPath;
  }

  public String getImporterFilter() {
    return filter;
  }
}
