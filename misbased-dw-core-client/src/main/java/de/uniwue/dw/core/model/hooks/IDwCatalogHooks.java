package de.uniwue.dw.core.model.hooks;

/**
 * Catalog hooks define locations in a data-warehouse catalog with pre-defined semantics, however,
 * facts belonging to these entries are not part of the core dw and must be provided by plug-ins.
 * 
 * Note that this interface may change to enums or a service mechanism in future releases.
 */
public interface IDwCatalogHooks {

  /**
   * Each project's root entry should use this String as its own project-value.
   */
  String ROOT_PROJECT_NAME = "untersuchung";

  /**
   * Root-Entry for information of clinical letter documents.
   */
  String PROJECT_HOOK_LETTER = "arztbriefe";

  /**
   * Root-Entry for information of clinical letter documents.
   */
  String EXT_HOOK_ID_LETTER_ROOT = "arztbriefe";

  /**
   * Root-Entry for information of clinical letter documents.
   */
  String EXT_HOOK_ID_LETTER_COMPLETE_TEXT = "brieftext";

  /**
   * EXT-ID for admission date entry.
   */
  String EXT_HOOK_ADMISSION_DATE = "aufnahme_Datum";

  /**
   * Project for admission date entry.
   */
  String PROJECT_HOOK_ADMISSION_DATE = "aufnahmeEntlassung";

  /**
   * EXT-ID for case-id entry.
   */
  String EXT_HOOK_CASE_ID = "caseid";

  /**
   * Project for case-id entry.
   */
  String PROJECT_HOOK_CASE_ID = "metadaten";

  /**
   * EXT-ID for case-id entry.
   */
  String EXT_HOOK_PATIENT_ID = "pid";

  /**
   * Project for case-id entry.
   */
  String PROJECT_HOOK_PATIENT_ID = "metadaten";


  /**
   * Project for case-id entry.
   */
  String PROJECT_HOOK_GROUP_ID = "gruppen";
}
