package de.uniwue.dw.imports.api;

/**
 * Keys that provide access to values of configuration properties related to dw-import-logic.
 */
public interface IDwImportsConfigurationKeys {

  public static final String PARAM_HDP_DB_TYPE = "hdp.db_type";

  public static final String PARAM_HDP_SERVER = "hdp.server";

  public static final String PARAM_HDP_DB_NAME = "hdp.db_name";

  public static final String PARAM_HDP_TRUSTED_CONNECTION = "hdp.trusted_connection";

  /**
   * Should performane optimizations be active that only work when the DB is empty ? Default is
   * false
   */
  public static final String PARAM_TREAT_ASINITIAL_IMPORT = "dw.imports.treatAsInitialImport";

  /**
   * Should the ImporterManager check everything before he starts importing ? Default is true
   */
  public static final String PARAM_DO_CHECKS_BEFORE_START = "dw.imports.doChecksBeforeStart";

  /**
   * String parameter that specifies the folder containing the data to import
   */
  public static final String PARAM_SAP_EXPORT_DIR = "dw.imports.dir.sap_export";

  /**
   * String parameter that specifies the folder containing the catalogs to import
   */
  public static final String PARAM_TERMINOLOGIES_DIR = "dw.imports.dir.external_terminology_dir";

  /**
   * String parameter that specifies the file with patient IDs which are exclusively imported to
   * filter the import
   */
  public static final String PARAM_PID_FILTER_FILE = "dw.imports.pidFilterFile";

  /**
   * Multiple files from the same domain can be imported in parallel
   */
  public static final String PARAM_PARALLELIZE_IMPORT = "dw.imports.parallelize";

  /**
   * String parameter that specifies the folder used for backup. This mechanism should not be used
   * any more as we now check the importLog for which files have already been imported, so nothing
   * has to be moved any more
   */
  public static final String PARAM_BACKUP_DIR = "dw.imports.dir.backup";

  /**
   * String parameter that specifies which Java-importer have to be imported. This parameter does
   * not affect the ConfiguredImporter which is used when his config-directory is set. If this
   * parameter omitted or left empty all importers are used. To use no Java-importer set it to "^$"
   */
  public static final String PARAM_IMPORTER_REGEX_FILTER = "dw.imports.regexFilter";

  /**
   * String parameter that specifies which Java-importer have NOT to be imported. This parameter
   * does not affect the ConfiguredImporter which is used when his cofig-directory is set. If this
   * parameter omitted or left empty no importers are excluded.
   */
  public static final String PARAM_IMPORTER_REGEX_FILTER_EXCLUDE = "dw.imports.regexFilterExclude";

  /**
   * boolean parameter that specifies if all old tables should be deleted. As most other tables are
   * obsolete when the catalog has been deleted they are also erased with this option. Use with
   * extreme care !
   */
  public static final String PARAM_DROP_ALL_TABLES = "dw.imports.pipeline.execute.drop_catalog_tables";

  /**
   * boolean parameter that specifies if the meta data tables should be deleted.
   */
  public static final String PARAM_DROP_METADATA_TABLES = "dw.imports.pipeline.execute.drop_metadata_tables";

  /**
   * similar to PARAM_DROP_ALL_OLD_TABLES but the table dwcatalog is retained
   */
  public static final String PARAM_DROP_FACT_TABLES = "dw.imports.pipeline.execute.drop_data_tables";

  /**
   * when set to true the meta data (cases, docs, pids) are not loaded at the beginning of an import
   * but instead when an importer tries to access a certain information. It should be set to true
   * when daily imports are done because then the amount of accesses is small. For a full import
   * this option should be false. When set to false the importer needs at least 6GB of RAM to store
   * all the meta data
   */
  public static final String PARAM_LOAD_IMPORT_METADATA_LAZY = "dw.imports.pipeline.execute.load_import_metadata_lazy";

  /**
   * As the Doc metadata take about 5GB of RAM their non lazy loading can be selectively activated
   */
  public static final String PARAM_LOAD_IMPORT_DOC_METADATA = "dw.imports.pipeline.execute.load_import_doc_metadata";

  /**
   * should a file which has been successfully imported be moved into the backup directory ? For
   * more details see option "dw.imports.dir.backup"
   */
  public static final String PARAM_MOVE_FILE_AFTER_UPDATE = "dw.imports.pipeline.execute.move_files_after_update";

  /**
   * import catalog if necessary. Every importer checks if there already exists some entries for his
   * project. If this is the case he does not import any more catalog entries but is skipped instead
   */
  public static final String PARAM_IMPORT_CATALOG = "dw.imports.pipeline.execute.import_catalogs_if_necessary";

  /**
   * Should every fact which is marked as "storno" be deleted from the fact table. Default is true
   */
  public static final String PARAM_DELETE_STORNOS = "dw.imports.pipeline.execute.delete_stornos";

  /**
   */
  public static final String PARAM_REPORT_SEND_MAIL_ON_SUCCESS = "dw.imports.pipeline.execute.send_mail_on_success";

  /**
   */
  public static final String PARAM_REPORT_SEND_MAIL_ON_FAIL = "dw.imports.pipeline.execute.send_mail_on_fail";

  /**
   * Only data with a timestamp after/before these parameters is imported. To omit these parameters
   * for a full import, set these paramters to an empty string or just remove them. These parameters
   * can be used to create small test databases. The timestamps have to have the format yyyy.mm.dd
   */
  public static final String PARAM_ONLY_AFTER = "dw.imports.pipeline.execute.only_after";

  public static final String PARAM_ONLY_BEFORE = "dw.imports.pipeline.execute.only_before";

  /**
   * If PIDs, CaseID or DocIDs are missing in the helper tables during import this is counted as an
   * error by default. Is this settings is set to FALSE it will only be counted as a warning.
   */
  public static final String PARAM_IMPORT_MISSING_METAINFO_IS_ERROR = "dw.imports.pipeline.execute.missingMetaInfoIsError";

  /**
   * Should any metadata be imported at all ? If set to false no case-, doc-, pid- and mov-meta data
   * is imported. If set to true the import can be further parametrized with the four following
   * options.
   */
  public static final String PARAM_IMPORT_METAINFOS = "dw.imports.pipeline.execute.import_metainfos";

  /**
   * Should PID-meta be imported ?
   */
  public static final String PARAM_IMPORT_PATIENT_METAINFOS = "dw.imports.pipeline.execute.import_patient_metainfos";

  /**
   * Should case-meta be imported ?
   */
  public static final String PARAM_IMPORT_CASE_METAINFOS = "dw.imports.pipeline.execute.import_case_metainfos";

  /**
   * Should doc-meta be imported ?
   */
  public static final String PARAM_IMPORT_DOC_METAINFOS = "dw.imports.pipeline.execute.import_doc_metainfos";

  /**
   * Should movement-meta be imported ?
   */
  public static final String PARAM_IMPORT_MOV_METAINFOS = "dw.imports.pipeline.execute.import_mov_metainfos";

  /**
   * Should any infos be imported. Default: true. If set to false no meta data is imported either.
   */
  public static final String PARAM_IMPORT_INFOS = "dw.imports.pipeline.execute.import_infos";

  /**
   * When a meta datum is set to "storno" all facts attached to it have to be deleted. This means
   * that for such a PID all infos of all docs of all cased of this pid have to be deleted. This
   * option should be set to true
   */
  public static final String PARAM_DELETE_STORNOS_OF_META_DATA = "dw.imports.pipeline.execute.delete_stornos_of_metadata";

  /**
   * When catalog entries have been deleted behind the back and the facts still reside those
   * abandoned facts have to be deleted. Default: true.
   */
  public static final String PARAM_DELETE_INFOS_WO_CATALOG_ENTRIES = "dw.imports.pipeline.execute.delete_infos_wo_catalog_entries";

  /**
   * This amount of lines in a csv file have to fail so that the whole file is declared failed
   */
  public static final String PARAM_CSV_FAIL_RATE = "dw.imports.pipeline.csv_failrate";

  /**
   * Should the update write a lock parameter into the DWSystemParams table, so that only one update
   * can take place at a time ?
   */
  public static final String PARAM_LOCK_DW_FOR_UPDATE = "dw.imports.lock";

  /**
   * Should there be any logging of erros for the import. Default is true
   */
  public static final String PARAM_DO_LOG_ERRORS = "dw.imports.doLogErrors";

  /**
   * Should there be any logging of infos and warnings for the import. Default is true
   */
  public static final String PARAM_DO_LOG_INFOS_AND_WARNINGS = "dw.imports.doLogInfosAndWarnings";

  /**
   * String parameter where the directory with xml import configs lies relative to the main import
   * dir
   */
  public static final String PARAM_IMPORT_CONFIGS = "dw.import.dir.importConfigs";

  public static final String PARAM_IMPORT_SORT_CATALOG_ROOT = "dw.import.dir.sortCatalogRoot";

  public static final String PARAM_EMAIL_SERVER = "email.server.address";

  public static final String PARAM_EMAIL_PORT = "email.server.port";

  public static final String PARAM_EMAIL_USER = "email.user";

  public static final String PARAM_EMAIL_PASSWORD = "email.password";

  public static final String PARAM_EMAIL_ADDRESS = "email.sendFrom";

  public static final String PARAM_EMAIL_RECEIVER = "email.sendTo";

  public static final String PARAM_EXPECTED_DOMAINS = "dw.imports.pipeline.execute.domains_with_expect_data";

  public static final String PARAM_IMPORT_ADAPTER_FACTORY_CLASS = "dw.imports.adapterFactoryClass";

}
