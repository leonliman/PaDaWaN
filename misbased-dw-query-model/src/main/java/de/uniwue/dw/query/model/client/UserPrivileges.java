package de.uniwue.dw.query.model.client;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.query.model.data.RawQuery;
import de.uniwue.dw.query.model.data.StoredQueryTreeEntry;

public class UserPrivileges {

  @Deprecated
  private static final List<String> ADMIN_CATALOG_ENTRY_NAMES = Arrays.asList(new String[] {
      "MetaDaten", "Studienmarkierung", "Herzinsuffizienz", "Meona", "AbdataMeds" });

  @Deprecated
  private static final List<String> SUPER_USER_CATALOG_ENTRY_NAMES = Arrays
          .asList(new String[] { "Studien", "EntlassDiagnose" });

  public static Predicate<RawQuery> entitledForQuery(User user) {
    return entry -> {
      if (user.isAdmin())
        return true;
      else
        return (entry.getName().toLowerCase().contains(user.getUsername().toLowerCase()));
    };
  }

  public static Predicate<StoredQueryTreeEntry> entitledForStoredQuery(User user) {
    return entry -> {
      if (user.isAdmin())
        return true;
      else
        return (entry.getPath().toLowerCase().contains(user.getUsername().toLowerCase()));
    };

  }

  @Deprecated
  public static Predicate<CatalogEntry> entitledForCatalogEntry(User user) {
    return entry -> {
      return entitledForCatalogEntry(entry, user);
    };
  }

  @Deprecated
  public static boolean entitledForCatalogEntry(CatalogEntry entry, User user) {
    if (isAdminEntry(entry) && !user.isAdmin())
      return false;
    if (isSuperUserEntry(entry) && !user.isSuperuser())
      return false;
    return true;
  }

  @Deprecated
  public static boolean isAdminEntry(CatalogEntry catalogEntry) {
    return ADMIN_CATALOG_ENTRY_NAMES.parallelStream()
            .filter(n -> n.equalsIgnoreCase(catalogEntry.getName())).findAny().isPresent();
  }

  @Deprecated
  public static boolean isSuperUserEntry(CatalogEntry catalogEntry) {
    return SUPER_USER_CATALOG_ENTRY_NAMES.parallelStream()
            .filter(n -> n.equalsIgnoreCase(catalogEntry.getName())).findAny().isPresent();
  }

  @Deprecated
  public static boolean entryOrAncestorIsAdminEntry(CatalogEntry catalogEntry) {
    List<CatalogEntry> toTest = catalogEntry.getAncestors();
    toTest.add(catalogEntry);
    return toTest.parallelStream().filter(UserPrivileges::isAdminEntry).findAny().isPresent();
  }

  @Deprecated
  public static boolean entryOrAncestorIsSuperUserEntry(CatalogEntry catalogEntry) {
    List<CatalogEntry> toTest = catalogEntry.getAncestors();
    toTest.add(catalogEntry);
    return toTest.parallelStream().filter(UserPrivileges::isSuperUserEntry).findAny().isPresent();
  }
}
