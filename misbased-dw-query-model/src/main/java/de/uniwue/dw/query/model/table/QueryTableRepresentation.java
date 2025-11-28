package de.uniwue.dw.query.model.table;

import java.util.List;

import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.manager.IQueryIOManager;
import de.uniwue.dw.query.model.visitor.QueryTableEntryVisitor4PatientQuery;
import de.uniwue.dw.query.model.visitor.QueryTableEntryVisitor4StatQuery;
 
public class QueryTableRepresentation {

  private QueryRoot query;

  private IQueryIOManager queryIOManager;

  public QueryTableRepresentation(QueryRoot query, IQueryIOManager queryIOManager) {
    this.query = query;
    this.queryIOManager = queryIOManager;
  }

  public QueryRoot getQuery() {
    return query;
  }

  public void setQuery(QueryRoot query) {
    this.query = query;
  }

  public boolean isDistributionQuery() {
    return query.isStatisticQuery();
  }

  public List<QueryTableEntry> getQueryTableEntryList() throws QueryException {
    if (isDistributionQuery())
      return query.accept(new QueryTableEntryVisitor4StatQuery(queryIOManager));
    else
      return query.accept(new QueryTableEntryVisitor4PatientQuery(queryIOManager));
  }
  
  public boolean isCountPatients(){
    return query.isDistinct();
  }
  
  public int get(){
    return query.getLimitResult();
  }

}
