package de.uniwue.dw.query.model.exception;

import de.uniwue.dw.query.model.client.GUIClientException;

public class QueryException extends GUIClientException {

  private static final long serialVersionUID = -7862727672884240692L;

  public static enum QueryExceptionType {
    NONE,
    // when trying to save a query, a query with the same name has been found
    QUERY_ALREADY_EXISTS,
    // there is a problem in the structure of a query
    QUERY_STRUCTURE
  }
  

  public QueryExceptionType type = QueryExceptionType.NONE;

  public QueryException(QueryExceptionType aType) {
    this(aType, aType.toString());
  }
  
  
  public QueryException(QueryExceptionType aType, String message) {
    super(message);
    type = aType;
  }

  public QueryException(String aMessage) {
    super(aMessage);
  }

  public QueryException(Exception e) {
    super(e);
  }

}
