package de.uniwue.dw.core.sql;

/**
 * Provide table names, column names etc. for usage in applications.
 */
public interface IDwSqlSchemaConstant {

  String T_CATALOG = "DWCatalog";

  String T_CATALOG_COUNT = "DWCatalogCount";

  String T_CATALOG_METADATA = "DWCatalogMetaData";

  String T_CATALOG_NUMDATA = "DWCatalogNumData";

  String T_CATALOG_CHOICES = "DWCatalogChoices";

  String T_GROUP = "DWGroup";

  String T_GROUP_CATALOG_PERMISSION = "DWGroupCatalogPermission";

  String T_GROUP_CASE_PERMISSION = "DWGroupCasePermission";

  String T_USER_IN_GROUP = "DWUserInGroup";

  String T_IMPORT_CASES = "DWImportCases";

  String T_IMPORT_DELETE = "DWImportDelete";

  String T_IMPORT_DOCS = "DWImportDocs";

  String T_IMPORT_PIDS = "DWImportPIDs";

  String T_INFO = "DWInfo";

  String T_QUERY = "DWQuery";

  String T_QUERY_LOG = "DWQueryLog";

  String T_REF_ID = "DWRefID";

  String T_ERROR_LOG = "DWImportLog";

  String T_INDEX_PARAMS = "IndexParams";

  String T_INDEX_LOG = "IndexLog";

  String T_IE_MEMORY = "IEMemory";

  String T_IE_SPANS = "IESpans";

  String T_USERS = "DWUsers";

  String T_CATALOG_MAPPER = "DWCatalogMapper";

  String T_SYSTEM_PARAMS = "DWSystemParams";

}
