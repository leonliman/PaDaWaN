package de.uniwue.dw.query.model.tests;

import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.result.Result;

public class QueryTest {

  public QueryRoot query;

  public Result desiredResult;

  public String queryFilename;

  public QueryTest(String aQueryFilename, QueryRoot aQuery, Result aResult) {
    queryFilename = aQueryFilename;
    query = aQuery;
    desiredResult = aResult;
  }

}
