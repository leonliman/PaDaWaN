package de.uniwue.dw.core.model.manager;

import de.uniwue.dw.core.model.data.CatalogEntry;

import java.util.HashSet;
import java.util.Set;

public class UniqueNameUtil {

  /*
   * The entries' uniqueName is parameterized by these three constants that determine how long the
   * individual parts of their concatenated members are to form the uniqueName
   */
  private static final int UNIQUE_NAME_MAX_PROJECT_CHARS = 3;

  private static final int UNIQUE_NAME_MAX_PARENT_CHARS = 20;

  private static final int UNIQUE_NAME_MAX_NAME_CHARS = 30;

  // This set contains all names that have yet been given to catalog entries. It is used to check if
  // a name already exists
  public static Set<String> uniqueNames = new HashSet<>();

  public static void deleteUniqueNameFromKnownNames(String uniqueName) {
    if (uniqueName != null && !uniqueName.isEmpty())
      uniqueNames.remove(uniqueName);
  }

  public static void clearKnownUniqueNames() {
    uniqueNames.clear();
  }

  public static String createOrRepairUniqueNameIfNecessary(CatalogEntry entry) {
    String result = createOrRepairUniqueNameIfNecessary(entry.getName(), entry.getProject(),
            entry.getParent(), entry.getUniqueName());
    uniqueNames.add(result);
    return result;
  }

  public static String createOrRepairUniqueNameIfNecessary(String name, String project,
          CatalogEntry parent, String uniqueName) {
    if (uniqueName == null || uniqueName.isEmpty())
      return getUniqueName(name, project, parent);
    else if (!isValidUniqueName(uniqueName)) {
      return repairUniqueName(uniqueName);
    } else
      return uniqueName;
  }

  private static String repairUniqueName(String uniqueName) {
    return getUniqueName(uniqueName);
  }

  public static boolean isValidUniqueName(String uniqueName) {
    if (uniqueName == null || uniqueName.isEmpty())
      return false;
    else
      return (uniqueName.equals(formatUniqueNameToAllowedSyntax(uniqueName)));
  }

  public static String getUniqueName(String name, String project) {
    return getUniqueName(name, project, "");
  }

  public static String getUniqueName(String name, String project, CatalogEntry parent) {
    if ((parent != null) && !parent.isRoot()) {
      return getUniqueName(name, project, parent.getName());
    } else {
      return getUniqueName(name, project);
    }
  }

  /*
   * Returns a (pseudo-)unique name created by the concatenation of the project, the parent and the
   * name of a catalogEntry. The uniqueName contains no "_", "(", ")" or ":."
   */
  public static String getUniqueName(String entryName, String projectName, String parentName) {
    if ((projectName != null) && !projectName.isEmpty()) {
      projectName = projectName.substring(0,
              Math.min(projectName.length(), UNIQUE_NAME_MAX_PROJECT_CHARS));
    } else {
      projectName = "";
    }
    if ((parentName != null) && !parentName.isEmpty()) {
      parentName = parentName.substring(0,
              Math.min(parentName.length(), UNIQUE_NAME_MAX_PARENT_CHARS));
    } else {
      parentName = "";
    }
    if ((entryName != null) && !entryName.isEmpty()) {
      entryName = entryName.substring(0, Math.min(entryName.length(), UNIQUE_NAME_MAX_NAME_CHARS));
    } else {
      entryName = "";
    }
    String result = projectName + ":" + parentName + "." + entryName;
    result = getUniqueName(result);
    return result;
  }

  private static String formatUniqueNameToAllowedSyntax(String uniqueName2Check) {
    return uniqueName2Check.replaceAll("\\s+", "_").replace("(", "[").replace(")", "]")
            .replace(":.", ":").replace("=", "_").replace(",", "").replace("<", "kleiner")
            .replace(">", "größer");
  }

  /*
   * If the given uniqueName already exists an additional counter is added to make it unique
   */
  public static String getUniqueName(String uniqueNameSuggestion) {
    String testedUniqueName = formatUniqueNameToAllowedSyntax(uniqueNameSuggestion);
    int x = 1;
    if (uniqueNames.contains(testedUniqueName)) {
      x++;
      testedUniqueName = formatUniqueNameToAllowedSyntax(uniqueNameSuggestion + "_" + x);
    }
    return testedUniqueName;
  }

}
