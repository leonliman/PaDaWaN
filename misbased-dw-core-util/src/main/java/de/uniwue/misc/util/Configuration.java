package de.uniwue.misc.util;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Configuration {

  protected static Configuration instance = null;

  public static List<Configuration> singletons = new ArrayList<>();

  public static final String ENV_DIR_CONFIGURATION_FILES = "MISBASED_DW_CONFIG_DIR";

  private static final Properties props = new Properties() {
    @Override
    public synchronized Enumeration<Object> keys() {
      return Collections.enumeration(new TreeSet<>(super.keySet()));
    }
  };

  public static synchronized Configuration getInstance() {
    if (instance == null) {
      instance = new Configuration();
      instance.getProposalsFromEnvironment();
      singletons.add(instance);
    }
    return instance;
  }

  private static void loadIncludedProperties() throws IOException, ConfigException {
    List<String> includesToLoad = new ArrayList<>();
    String includeFile = props.getProperty("properties.include");
    if (includeFile != null) {
      includesToLoad.add(includeFile);
      props.remove("properties.include");
    }
    for (int index = 2; index < 10; index++) {
      includeFile = props.getProperty("properties.include" + index);
      if (includeFile != null) {
        includesToLoad.add(includeFile);
        props.remove("properties.include" + index);
      }
    }
    for (String fileToInclude : includesToLoad) {
      loadProperties(new File(fileToInclude));
    }
  }

  public static void mergeProperties(Properties propsToMerge) {
    // additionally, merge all new props into this props
    for (Entry<Object, Object> x : propsToMerge.entrySet()) {
      String value = propsToMerge.getProperty(x.getKey().toString());
      props.put(x.getKey(), value);
    }
  }

  public void setProperty(String aPropName, String aValue) {
    props.setProperty(aPropName, aValue);
  }

  public static void loadProperties(File file) throws IOException, ConfigException {
    if (file == null || !file.exists() || !file.isFile()) {
      throw new FileNotFoundException("cannot find properties file @ " + file);
    }
    Properties newProps = new Properties();
    try (FileInputStream is = new FileInputStream(file)) {
      Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
      newProps.load(reader);
    }
    mergeProperties(newProps);
    loadIncludedProperties();
  }

  public Properties getProps() {
    return props;
  }

  @Override
  public String toString() {
    return props.toString();
  }

  public String getParameter(String key, String defaultValue) {
    String param = props.getProperty(key, defaultValue);
    if (param != null) {
      param = param.trim();
    }
    return param;
  }

  public String getParameter(String key) {
    String param = props.getProperty(key);
    if (param != null) {
      param = param.trim();
    }
    return param;
  }

  public String getParameter(String key, boolean throwExceptionIfMissing) throws ConfigException {
    String result = getParameter(key);
    if (result == null) {
      if (throwExceptionIfMissing)
        throw new ConfigException(key);
      else
        return null;
    }
    return result;
  }

  public boolean getBooleanParameter(String key) {
    return Boolean.parseBoolean(getParameter(key));
  }

  public boolean getBooleanParameter(String key, boolean defaultValue) {
    return props.containsKey(key) ? getBooleanParameter(key) : defaultValue;
  }

  public Boolean getBooleanParameterWithPossibleException(String key,
          boolean throwExceptionIfMissing) throws ConfigException {
    String result = getParameter(key);
    if (result == null) {
      if (throwExceptionIfMissing)
        throw new ConfigException(key);
      else
        return null;
    }
    return Boolean.parseBoolean(result);
  }

  public int getIntegerParameter(String key) {
    return Integer.parseInt(getParameter(key));
  }

  public double getDoubleParameter(String key) {
    return Double.parseDouble(getParameter(key));
  }

  public Integer getIntegerParameter(String key, boolean throwExceptionIfMissing)
          throws ConfigException {
    String result = getParameter(key);
    if (result == null) {
      if (throwExceptionIfMissing)
        throw new ConfigException(key);
      else
        return null;
    }
    return Integer.parseInt(result);
  }

  public int getIntegerParameter(String key, int defaultValue) {
    return props.containsKey(key) ? getIntegerParameter(key) : defaultValue;
  }

  public long getLongParameter(String key, long defaultValue) {
    return props.containsKey(key) ? getLongParameter(key) : defaultValue;
  }

  public long getLongParameter(String key) {
    return Long.parseLong(getParameter(key));
  }

  public double getDoubleParameter(String key, double defaultValue) {
    return props.containsKey(key) ? getDoubleParameter(key) : defaultValue;
  }

  public File getFileParameter(String key) {
    String pathString = getParameter(key);
    if (pathString != null) {
      if (pathString.startsWith("classpath:")) {
        pathString = pathString.substring("classpath:".length());
        URL resource = getClass().getClassLoader().getResource(pathString);
        File file;
        try {
          file = Paths.get(resource.toURI()).toFile();
          return file;
        } catch (URISyntaxException e) {
          e.printStackTrace();
        }
      } else
        return new File(pathString);
    }
    return null;
  }

  public List<String> getArrayParameter(String key) {
    List<String> theList = new ArrayList<>();
    String tmp = props.getProperty(key);
    if (tmp != null && tmp.trim().length() > 0) {
      tmp = tmp.trim();
      Matcher m = Pattern.compile("(?:\\s*(?:\"([^\"]*)\"|([^,]+))\\s*,?|(?<=,),?)+?")
              .matcher(tmp);
      while (m.find())
        theList.add(m.group(1).replace("\"", ""));
    }
    return theList;
  }

  public static void clearAllConfigurations() throws ConfigException {
    for (Configuration aConfig : singletons) {
      aConfig.clear();
    }
  }

  public void clear() throws ConfigException {
    props.clear();
  }

  /**
   * @return <code>null</code> if environment variable <code>MISBASED_DW_CONFIG_DIR</code> has not
   * been set, else paths of "*.properties" files in the given directory.
   */
  public Path[] getProposalsFromEnvironment() {
    String env = System.getenv(Configuration.ENV_DIR_CONFIGURATION_FILES);
    if (env == null) {
      return new Path[0];
    }
    Path dir = Paths.get(env);
    String[] list = dir.toFile().list((dir1, name) -> name.endsWith(".properties"));
    Path[] out = new Path[list.length];
    for (int i = 0; i < list.length; i++) {
      String fn = list[i];
      out[i] = Paths.get(env, fn);
    }
    return out;
  }

  private Path getPathFromEnvironment(String name) {
    Path[] paths = getProposalsFromEnvironment();
    for (Path path : paths) {
      if (path.getFileName().toString().equals(name)) {
        return path;
      }
    }
    return null;
  }

  /**
   * singleton instance initialized with properties from config $name available through system environment.
   */
  public void loadFromEnvironment(String name) throws IOException, ConfigException {
    Path path = getPathFromEnvironment(name);
    if (path != null) {
      File file = path.toAbsolutePath().normalize().toFile();
      loadProperties(file);
    }
  }

}
