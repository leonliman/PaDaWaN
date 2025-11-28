package de.uniwue.dw.query.sql.util;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.query.model.client.DefaultQueryRunner;
import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.client.IGUIClient;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.result.PatientQueryRunnable;
import de.uniwue.dw.query.model.result.export.ExportConfiguration;
import de.uniwue.dw.query.model.visitor.StructureErrorVisitor;

public class SQLQueryRunner extends DefaultQueryRunner {

  public SQLQueryRunner(IGUIClient guiClient) {
    super(guiClient);
  }

  @Override
  public PatientQueryRunnable runQueryInternal(QueryRoot queryRoot, User user,
          ExportConfiguration exportConfig) throws GUIClientException {
    PatientQueryRunnable runnable = new SQLQueryRunnable(0, queryRoot, user, exportConfig,
            guiClient);
    return runnable;
  }

  @Override
  protected StructureErrorVisitor createStructureErrorVisitor() {
    return new SQLStructureErrorVisitor(this);
  }

  @Override
  public boolean canProcessInfoAdditonalInfos() {
    return false;
  }

  @Override
  public boolean canProcessMultipleRows() {
    return true;
  }

  @Override
  public boolean canProcessDistincts() {
    return true;
  }

  @Override
  public boolean canDoPostProcessing() {
    return false;
  }

  @Override
  public boolean canProcessGroupCases() {
    return true;
  }

  @Override
  public String getEngineVersion() {
    return "2.0";
  }
}
