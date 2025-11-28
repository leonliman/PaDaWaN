package de.uniwue.dw.query.solr.client;

public interface ISolrConstants {

  String FIELD_EXISTANCE_FIELD = "containing_fields";

  String SOLR_FIELD_ID = "id";

  String SOLR_FIELD_PATIENT_ID = "patient";

  String SOLR_FIELD_CASE_ID = "long_caseid";

  String SOLR_FIELD_GROUPS = "string_groups";

  String SOLR_FIELD_BOOLEAN_STRUCTURE_VALUE = "string_value";

  String SOLR_FIELD_DOC_TYPE = "string_doc_type";

  String SOLR_FIELD_PARENT_CHILD_LAYER = "string_layer";

  String SOLR_FIELD_EXTREME_VALUE = "string_extreme_value";
  String SOLR_FIELD_FIRST_VALUE = "string_first_value";
  String SOLR_FIELD_LAST_VALUE = "string_last_value";
  String SOLR_FIELD_MIN_VALUE = "string_min_value";
  String SOLR_FIELD_MAX_VALUE = "string_max_value";

  String SOLR_FIELD_MEASURE_TIME = "date_measure_time";

  String SOLR_FIELD_REF_ID = "long_refID";

  String SOLR_FIELD_DOC_ID = "long_docID";

  String PATIENT_DOC_TYPE = "patient";

  String CASE_DOC_TYPE = "case";

  String DOCUMENT_DOC_TYPE = "document";

  String INFO_DOC_TYPE = "info";

  String PARENT_LAYER = "parent";

  String PARENT_INTERMEDIATE_LAYER = "parent_intermediate";

  String CHILD_LAYER = "child";

  String CASE_PREFIX = "c";

  String DOCUMENT_PREFIX = "d";

  String PATIENT_PREFIX = "p";

  String INFO_GROUP_PREFIX = "ig";

  String INFO_PREFIX = "i";

  String MIN = "min";

  String MAX = "max";

  String FIRST = "first";

  String LAST = "last";

  String LEAF_COUNT = "leaf_count";

  String LEAF_TEXT = "leaf_text";

  String MAX_SON_COUNT = "max_son_count";

  String SONS_TEXT = "sons_text";

  String SONS_COUNT = "sons_count";

  int STRING_FIELD_MAX_LENGTH = 30000;

  int MAX_ROWS = 1000000;

  int TOTAL_WORK = 1000;

  int SENDING_QUERY_WORK = 50;

  int RECEIVING_DOCS_WORK = 650;

  // public static final int HIGHLIGHTING_WORK = 50;
  int CREATING_EXCEL_WORK = 50;

  int WRITING_EXCEL_WORK = 250;

  int NUMBER_OF_EXPORTED_ROWS = 1000000;

  enum DOC_TYPE {
    PATIENT(PATIENT_DOC_TYPE, PATIENT_PREFIX),
    CASE(CASE_DOC_TYPE, CASE_PREFIX),
    DOCUMENT(DOCUMENT_DOC_TYPE, DOCUMENT_PREFIX);

    public String docType, prefix;

    DOC_TYPE(String docType, String prefix) {
      this.docType = docType;
      this.prefix = prefix;
    }

  }

}
