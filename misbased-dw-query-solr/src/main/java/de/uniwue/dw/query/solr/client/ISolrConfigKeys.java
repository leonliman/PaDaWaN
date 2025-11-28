package de.uniwue.dw.query.solr.client;

public interface ISolrConfigKeys {

  String PARAM_SOLR_USER_NAME = "solr.user";

  String PARAM_SOLR_PASSWORD = "solr.password";

  String PARAM_SOLR_SERVER_URL = "solr.server_url";

  String PARAM_SOLR_PATIENT_LAYER_SERVER_URL = "solr.patientLayer.server_url";

  String PARAM_INDEX_PATIENT_LAYER = "solr.indexPatientLayer";

  String PARAM_INDEX_TEXT_AS_STRING = "solr.indexTextForRegexQuery";

  String PARAM_INDEX_NEGEX = "solr.indexNegex";

  String PARAM_EMBEDDED_SOLR_HOME_DIR = "embeddedSolr.homeDir";

}
