package de.uniwue.dw.query.model;

public interface IQueryKeys {

  String PARAM_GUI_CLIENT_TYPE = "gui.client.type";

  String PARAM_QUICK_SEARCH_EXAMPLE = "client.quick.search.example";

  String PARAM_INDEXER_COMMIT_AFTER_DOCS = "indexer.commit.after.docs";

  String PARAM_INDEXER_INDEX_DATA = "indexer.indexData";

  String PARAM_INDEXER_INDEX_CATALOG = "indexer.indexCatalog";

  String PARAM_INDEXER_INDEX_PREFIX = "indexer.indexPrefix";

  String PARAM_INDEXER_INDEX_ENTIRE_CATALOG = "indexer.indexEntireCatalog";

  String PARAM_INDEXER_PROJECT_TO_INDEX = "indexer.projectToIndex";

  String PARAM_INDEXER_PID_TO_INDEX = "indexer.pidToIndex";

  String PARAM_INDEXER_ATTRID_TO_INDEX = "indexer.attridToIndex";

  String PARAM_INDEXER_CALCULATE_CATALOG_COUNT = "indexer.calculateCatalogCount";

  String PARAM_INDEXER_DELETE_INDEX = "indexer.deleteIndex";

  String PARAM_INDEXER_INDEX_ALL_DATA = "indexer.indexAllData";

  String PARAM_INDEXER_PROCESS_DELETED_INFOS = "indexer.processDeletedInfos";

  /**
   * Experimental setting to change the Solr update mode so that not the entire patient/case with updated infos is
   * deleted and newly indexed but instead only the changed infos are updated; this only works, if the changed infos
   * belong to an already existing or a new catalog entry; if a catalog is just deleted this mode won't delete the
   * associated infos from the Solr index
   */
  String PARAM_INDEXER_DO_INCREMENTAL_UPDATE = "indexer.doIncrementalUpdate";

  String PARAM_USE_CACHE = "query.useCache";

  String PARAM_HIT_HIGHLIGHT_FRAGSIZE = "query.highlight.fragsize";

  String PARAM_HIT_HIGHLIGHT_PRE = "query.highlight.hit.pre";

  String PARAM_HIT_HIGHLIGHT_STYLE = "query.highlight.hit.style";

  String PARAM_HIT_HIGHLIGHT_POST = "query.highlight.hit.post";

  String PARAM_IGNORE_USER_FOR_QUERY_LOGGING = "query.logging.ignoreUser";

  String PARAM_CATALOG_CLIENT_PROVIDER_CLASS = "query.catalogClientProviderClass";

  String PARAM_LOCK_DW_FOR_UPDATE = "indexer.lockForUpdate";

  /**
   * If true, every query will be extended with group privileges of a user. Then a user can only see
   * the documents, for which he is entitled. All other documents will be hidden for the user and
   * will not appear in the aggregated numbers in the statistic tool or as a result row in the
   * document query tool.
   */
  String PARAM_FILTER_DOCUMENTS_BY_GROUPS = "dw.groups.document.filter";

  String PARAM_DO_CATALOG_COUNT_PERSONALISATION = "dw.catalog.doCountPersonalisation";

  String PARAM_QUERY_ADAPTER_FACTORY_CLASS = "query.adapterFactoryClass";

  String PARAM_INDEXER_CLEAN_CATALOG_INDEX = "indexer.cleanCatalogIndex";

  String PARAM_INDEXER_DO_PARALLEL_INDEXING = "indexer.doParallelIndexing";

  String PARAM_QUERY_ALWAYS_GROUP_DISTINCT_QUERIES_ON_DOC_LEVEL = "query.alwaysGroupDistinctQueriesOnDocLevel";

}
