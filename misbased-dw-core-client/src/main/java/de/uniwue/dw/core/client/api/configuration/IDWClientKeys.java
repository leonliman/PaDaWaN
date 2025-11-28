package de.uniwue.dw.core.client.api.configuration;

public interface IDWClientKeys {

  /**
   * When set to true the inserts into the fact table are not done by bulk inserts and no simple
   * inserts. Instead for every insert it is checked whether the PID-AttrID-MeasureTime-triple
   * already exists. If yes the table row is updated, if no a simple insert is performed. WARNING:
   * This mode is very slow !
   */
  String PARAM_DO_SQL_UPDATES = "dw.imports.pipeline.execute.doSQLUpdate";

  /**
   * Should a lot of indices be created on the DWInfo-table ? If we want to perform a big import
   * this should be turned of to quicken import operations. Afterwards this should be turned on
   * again
   */
  String PARAM_CREATE_INFO_INDICES = "dw.createInfoIndices";

  /**
   * This only creates a single index on the PID field resulting in a huge reduction of the time needed to create an
   * index with any query engine using AbstractDataSource2Index (this index is included if dw.createInfoIndices is
   * set to true)
   */
  String PARAM_CREATE_INFO_INDEX_ON_PID = "dw.createInfoIndexOnPID";

  String PARAM_CREATE_MSSQL_FULLTEXT_CATALOG = "dw.createMSSQLFulltextCatalog";

  String PARAM_USE_FULLTEXT_INDEX = "dw.useFulltextIndex";

  String PARAM_LDAP_SERVER = "ldap.server";

  String PARAM_LDAP_OU = "ldap.ou";

  String PARAM_LDAP_GROUP_OU = "ldap.group.ou";

  String PARAM_LDAP_DC = "ldap.dc";

  /*
   * How many items have to be found in the statistical query mode to show the real number. All
   * results below this number are replaced by an asterix
   */
  String PARAM_K_ANONYMITY = "dw.query.kanonymity";

  String PARAM_USE_PROKET_PASSWORD = "dw.useProketPasswords";

  /**
   * GUI parameters
   */
  String PARAM_DOCUMENT_ID_EXTID = "client.catalog.document_id.extid";

  String PARAM_DOCUMENT_ID_PROJECT = "client.catalog.document_id.project";

  String PARAM_DOCUMENT_GROUP_ID_EXTID = "client.catalog.document_group_id.extid";

  String PARAM_DOCUMENT_GROUP_ID_PROJECT = "client.catalog.document_group_id.project";

  String PARAM_DOCUMENT_TIME_EXTID = "client.catalog.document_time.extid";

  String PARAM_DOCUMENT_TIME_PROJECT = "client.catalog.document_time.project";

  String PARAM_DOCUMENT_SUGGESTER_EXTID = "client.catalog.suggester.default.extid";

  String PARAM_DOCUMENT_SUGGESTER_PROJECT = "client.catalog.suggester.default.project";

  String PARAM_MOST_COMMON_TEXT_FIELD_EXTID = "client.catalog.most_common_text_field.extid";

  String PARAM_MOST_COMMON_TEXT_FIELD_PROJECT = "client.catalog.most_common_text_field.project";

  String PARAM_MOST_COMMON_TEXT_FIELDX_EXTID = "client.catalog.most_common_text_fieldXX.extid";

  String PARAM_MOST_COMMON_TEXT_FIELDX_PROJECT = "client.catalog.most_common_text_fieldXX.project";

  String PARAM_CLIENT_ADAPTER_FACTORY_CLASS = "client.adapterFactoryClass";

  String PARAM_CATALOG_CLIENT_FACTORY_CLASS = "client.catalogClientFactoryClass";

  String PARAM_CATALOG_CLIENT_SHOULD_FIX_INVALID_UNIQUE_NAMES_IN_DATABASE = "client.catalogClientShouldFixInvalidUniqueNamesInDatabase";

}
