package de.uniwue.dw.core.client.authentication;

import java.util.Properties;

public class UserSettings {

  public static final int CATALOG_MIN_OCCURRENCE_THRESHOLD_DEFAULT = 0;

  public static final CountType CATALOG_COUNT_TYPE_DEFAULT = CountType.distinctCaseID;

  public static final int CATALOG_SEARCH_MAX_ITEMS_DEFAULT = 500;

  public static final boolean CATALOG_SHOW_METADATA_DEFAULT = false;

  private static final String CATALOG_MIN_OCCURRENCE_THRESHOLD_KEY = "catalog_threshold";

  private static final String CATALOG_COUNT_TYPE_KEY = "catalog_count_type";

  private static final String CATALOG_SEARCH_MAX_ITEMS_KEY = "catalog_search_max_items";

  private static final String CATALOG_SHOW_METADATA_KEY = "catalog_show_metadata";

  private static final String USER_INTERFACE_TYPE_KEY = "user_interface_type";

  private static final String USER_INTERFACE_LANGUAGE_KEY = "user_interface_language";

  private static final String USER_INTERFACE_SESSION_STORAGE_KEY = "user_interface_session_storage";

  private static final String USER_INTERFACE_USE_PLUGINS_KEY = "user_interface_use_plugins";

  public static UserInterfaceType USER_INTERFACE_TYPE_DEFAULT = UserInterfaceType.light;

  public static UserInterfaceLanguage USER_INTERFACE_LANGUAGE_DEFAULT = UserInterfaceLanguage.german;

  public static boolean USER_INTERFACE_SESSION_STORAGE_DEFAULT = false;

  public static boolean USER_INTERFACE_USE_PLUGINS_DEFAULT = false;

  private final Properties props;

  public UserSettings(int catalogMinOccurrenceThreshold, CountType catalogCountType) {
    this(catalogMinOccurrenceThreshold, catalogCountType, CATALOG_SEARCH_MAX_ITEMS_DEFAULT,
            CATALOG_SHOW_METADATA_DEFAULT, USER_INTERFACE_TYPE_DEFAULT, USER_INTERFACE_LANGUAGE_DEFAULT,
            USER_INTERFACE_SESSION_STORAGE_DEFAULT, USER_INTERFACE_USE_PLUGINS_DEFAULT);
  }

  public UserSettings(int catalogMinOccurrenceThreshold, CountType catalogCountType, int catalogSearchMaxItems,
          boolean catalogShowMetadata, UserInterfaceType userInterfaceType, UserInterfaceLanguage userInterfaceLanguage,
          boolean userInterfaceUseSessionStorage, boolean userInterfaceUsePlugins) {
    this.props = new Properties();
    props.setProperty(CATALOG_MIN_OCCURRENCE_THRESHOLD_KEY, String.valueOf(catalogMinOccurrenceThreshold));
    props.setProperty(CATALOG_COUNT_TYPE_KEY, catalogCountType.name());
    props.setProperty(CATALOG_SEARCH_MAX_ITEMS_KEY, String.valueOf(catalogSearchMaxItems));
    props.setProperty(CATALOG_SHOW_METADATA_KEY, String.valueOf(catalogShowMetadata));
    props.setProperty(USER_INTERFACE_TYPE_KEY, userInterfaceType.name());
    props.setProperty(USER_INTERFACE_LANGUAGE_KEY, userInterfaceLanguage.name());
    props.setProperty(USER_INTERFACE_SESSION_STORAGE_KEY, String.valueOf(userInterfaceUseSessionStorage));
    props.setProperty(USER_INTERFACE_USE_PLUGINS_KEY, String.valueOf(userInterfaceUsePlugins));
  }

  public UserSettings(Properties props) {
    this.props = props;
  }

  public static UserSettings createDefaultUserSettings() {
    return new UserSettings(CATALOG_MIN_OCCURRENCE_THRESHOLD_DEFAULT, CATALOG_COUNT_TYPE_DEFAULT,
            CATALOG_SEARCH_MAX_ITEMS_DEFAULT, CATALOG_SHOW_METADATA_DEFAULT,
            USER_INTERFACE_TYPE_DEFAULT, USER_INTERFACE_LANGUAGE_DEFAULT, USER_INTERFACE_SESSION_STORAGE_DEFAULT,
            USER_INTERFACE_USE_PLUGINS_DEFAULT);
  }

  public static UserSettings createDefaultUserSettings(boolean userIsAdmin) {
    return new UserSettings(CATALOG_MIN_OCCURRENCE_THRESHOLD_DEFAULT, CATALOG_COUNT_TYPE_DEFAULT,
            CATALOG_SEARCH_MAX_ITEMS_DEFAULT, userIsAdmin || CATALOG_SHOW_METADATA_DEFAULT,
            USER_INTERFACE_TYPE_DEFAULT, USER_INTERFACE_LANGUAGE_DEFAULT, USER_INTERFACE_SESSION_STORAGE_DEFAULT,
            USER_INTERFACE_USE_PLUGINS_DEFAULT);
  }

  public Properties getInternalProperties() {
    return props;
  }

  public int getCatalogMinOccurrenceThreshold() {
    return Integer.parseInt(props.getProperty(CATALOG_MIN_OCCURRENCE_THRESHOLD_KEY));
  }

  public UserSettings setCatalogMinOccurrenceThreshold(int catalogMinOccurrenceThreshold) {
    props.setProperty(CATALOG_MIN_OCCURRENCE_THRESHOLD_KEY, String.valueOf(catalogMinOccurrenceThreshold));
    return this;
  }

  public CountType getCatalogCountType() {
    return CountType.valueOf(props.getProperty(CATALOG_COUNT_TYPE_KEY));
  }

  public UserSettings setCatalogCountType(CountType catalogCountType) {
    props.setProperty(CATALOG_COUNT_TYPE_KEY, catalogCountType.name());
    return this;
  }

  public int getCatalogSearchMaxItems() {
    return Integer.parseInt(props.getProperty(CATALOG_SEARCH_MAX_ITEMS_KEY));
  }

  public UserSettings setCatalogSearchMaxItems(int catalogSearchMaxItems) {
    props.setProperty(CATALOG_SEARCH_MAX_ITEMS_KEY, String.valueOf(catalogSearchMaxItems));
    return this;
  }

  public boolean getCatalogShowMetadata() {
    return Boolean.parseBoolean(props.getProperty(CATALOG_SHOW_METADATA_KEY));
  }

  public UserSettings setCatalogShowMetadata(boolean catalogShowMetadata) {
    props.setProperty(CATALOG_SHOW_METADATA_KEY, String.valueOf(catalogShowMetadata));
    return this;
  }

  public UserInterfaceType getUserInterfaceType() {
    if (!props.containsKey(USER_INTERFACE_TYPE_KEY))
      return null;
    else
      return UserInterfaceType.valueOf(props.getProperty(USER_INTERFACE_TYPE_KEY));
  }

  public UserSettings setUserInterfaceType(UserInterfaceType userInterfaceType) {
    props.setProperty(USER_INTERFACE_TYPE_KEY, userInterfaceType.name());
    return this;
  }

  public UserInterfaceLanguage getUserInterfaceLanguage() {
    if (!props.containsKey(USER_INTERFACE_LANGUAGE_KEY))
      return null;
    else
      return UserInterfaceLanguage.valueOf(props.getProperty(USER_INTERFACE_LANGUAGE_KEY));
  }

  public UserSettings setUserInterfaceLanguage(UserInterfaceLanguage userInterfaceLanguage) {
    props.setProperty(USER_INTERFACE_LANGUAGE_KEY, userInterfaceLanguage.name());
    return this;
  }

  public Boolean getUserInterfaceUseSessionStorage() {
    if (!props.containsKey(USER_INTERFACE_SESSION_STORAGE_KEY))
      return null;
    else
      return Boolean.parseBoolean(props.getProperty(USER_INTERFACE_SESSION_STORAGE_KEY));
  }

  public UserSettings setUserInterfaceUseSessionStorage(boolean userInterfaceUseSessionStorage) {
    props.setProperty(USER_INTERFACE_SESSION_STORAGE_KEY, String.valueOf(userInterfaceUseSessionStorage));
    return this;
  }

  public Boolean getUserInterfaceUsePlugins() {
    if (!props.containsKey(USER_INTERFACE_USE_PLUGINS_KEY))
      return null;
    else
      return Boolean.parseBoolean(props.getProperty(USER_INTERFACE_USE_PLUGINS_KEY));
  }

  public UserSettings setUserInterfaceUsePlugins(boolean userInterfaceUsePlugins) {
    props.setProperty(USER_INTERFACE_USE_PLUGINS_KEY, String.valueOf(userInterfaceUsePlugins));
    return this;
  }

}
