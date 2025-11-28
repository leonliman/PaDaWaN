package de.uniwue.dw.solr.api;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.query.model.quickSearch.suggest.Suggester;
import de.uniwue.dw.query.solr.client.ISolrConfigKeys;
import de.uniwue.dw.query.solr.client.SolrManager;
import de.uniwue.dw.query.solr.model.manager.EmbeddedSolrManager;
import de.uniwue.dw.query.solr.suggest.IndexLookupUtils;
import de.uniwue.misc.util.ConfigException;
import org.apache.solr.client.solrj.SolrRequest;

public class DWSolrConfig extends DwClientConfiguration implements ISolrConfigKeys {

  private static DWSolrConfig instance = null;

  private static final int SNIPPET_FRAGMENT_CHAR_SIZE_DEFAULT = 100;

  private SolrManager solrManager;

  private IndexLookupUtils indexLookupUtils;

  private Suggester suggester;

  private boolean isEmbedded = false;

  public static synchronized DWSolrConfig getInstance() {
    if (instance == null) {
      instance = new DWSolrConfig();
      singletons.add(instance);
    }
    return instance;
  }

  public static boolean indexPatientLayer() {
    boolean param = getInstance().getBooleanParameter(PARAM_INDEX_PATIENT_LAYER, false);
    return param || getSolrPatientLayerServerUrl() != null;
  }

  public static boolean indexTextForRegexQueries() {
    return getInstance().getBooleanParameter(PARAM_INDEX_TEXT_AS_STRING, false);
  }

  public static String getSolrPassword() {
    return getInstance().getParameter(PARAM_SOLR_PASSWORD);
  }

  public static String getSolrUser() {
    return getInstance().getParameter(PARAM_SOLR_USER_NAME);
  }

  public static String getSolrServerUrl() {
    return getInstance().getParameter(PARAM_SOLR_SERVER_URL);
  }

  public static String getSolrPatientLayerServerUrl() {
    return getInstance().getParameter(PARAM_SOLR_PATIENT_LAYER_SERVER_URL);
  }

  public SolrManager getSolrManager() {
    if (solrManager != null)
      return solrManager;
    else if (getSolrUser() != null && getSolrPassword() != null && getSolrServerUrl() != null) {
      solrManager = new SolrManager(getSolrUser(), getSolrPassword(), getSolrServerUrl(),
              getSolrPatientLayerServerUrl());
      return solrManager;
    } else
      throw new IllegalStateException(
              "SolrManager is not initialized. Call DwClientConfiguration.getInstance().loadProperties() first.");
  }

  public void setSolrManager(SolrManager newSolrManager) {
    this.solrManager = newSolrManager;
  }

  public void setSolrManager(SolrManager newSolrManager, boolean isEmbedded) {
    this.solrManager = newSolrManager;
    this.isEmbedded = isEmbedded;
  }

  public static SolrRequest.METHOD getSolrMethodToUse() {
    if (getInstance().isEmbedded)
      return SolrRequest.METHOD.GET;
    else
      return SolrRequest.METHOD.POST;
  }

  public IndexLookupUtils getIndexLookupUtils() {
    if (indexLookupUtils == null) {
      indexLookupUtils = new IndexLookupUtils(getSolrManager());
    }
    return indexLookupUtils;
  }

  public Suggester getSuggester() {
    if (suggester == null)
      suggester = new Suggester(getIndexLookupUtils());
    return suggester;
  }

  public static String getEmbeddedSolrHomeDir() {
    return getInstance().getParameter(PARAM_EMBEDDED_SOLR_HOME_DIR);
  }

  @Override
  public void clear() throws ConfigException {
    super.clear();
    suggester = null;
    indexLookupUtils = null;
    if (solrManager != null) {
      if (solrManager instanceof EmbeddedSolrManager) {
        ((EmbeddedSolrManager) solrManager).shutdown();
      }
      solrManager = null;
    }
  }

}
