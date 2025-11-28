package de.uniwue.dw.query.solr;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.query.model.client.DefaultQueryRunner;
import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.client.IGUIClient;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.ContentOperator;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.result.PatientQueryRunnable;
import de.uniwue.dw.query.model.result.export.ExportConfiguration;
import de.uniwue.dw.query.solr.export.Solr7PatientQueryRunnable;

public class SolrQueryRunner extends DefaultQueryRunner {

  public SolrQueryRunner(IGUIClient guiClient) {
    super(guiClient);
  }

  @Override
  public PatientQueryRunnable runQueryInternal(QueryRoot queryRoot, User user,
          ExportConfiguration exportConfig) throws GUIClientException {
    // return new SolrPatientQueryRunnable(queryRoot, user, exportConfig, guiClient);
    return new Solr7PatientQueryRunnable(queryRoot, user, exportConfig, guiClient);
  }

  @Override
  public boolean canProcessFulltextNearOperators() {
    return true;
  }

  @Override
  public boolean canProcessGroupCases() {
    return true;
  }

  @Override
  public boolean canProcessGroupDocs() {
    return true;
  }

  @Override
  public String getEngineVersion() {
    return "2.1";
  }

  @Override
  public boolean hasToDoPostProcessing(QueryRoot queryRoot) throws QueryException {
    boolean hasToDoPostProcessing = super.hasToDoPostProcessing(queryRoot);
    if (!hasToDoPostProcessing) {
      for (QueryAttribute anAttr : queryRoot.getAttributesRecursive()) {
        if (((anAttr.getContentOperator() == ContentOperator.BETWEEN)
                || (anAttr.getContentOperator() == ContentOperator.MORE)
                || (anAttr.getContentOperator() == ContentOperator.MORE_OR_EQUAL)
                || (anAttr.getContentOperator() == ContentOperator.LESS)
                || (anAttr.getContentOperator() == ContentOperator.LESS_OR_EQUAL))
                && !queryRoot.isOnlyCount()) {
          // if patient data has to be returned (no only count) then polish the returned data so
          // that they really reflects the constrained data
          hasToDoPostProcessing = true;
        }
      }
    }
    return hasToDoPostProcessing;
  }

}
