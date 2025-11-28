package de.uniwue.dw.query.sql.util;

import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.query.model.client.DefaultQueryRunner;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.exception.QueryStructureException;
import de.uniwue.dw.query.model.exception.QueryStructureException.QueryStructureExceptionType;
import de.uniwue.dw.query.model.lang.ContentOperator;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.visitor.StructureErrorVisitor;

import java.util.Set;

public class SQLStructureErrorVisitor extends StructureErrorVisitor {

  public SQLStructureErrorVisitor(DefaultQueryRunner queryRunner) {
    super(queryRunner);
  }

  @Override
  public Set<QueryStructureException> visit(QueryAttribute queryElem) throws QueryException {
    Set<QueryStructureException> result = super.visit(queryElem);
    if ((queryElem.getCatalogEntry().getDataType() == CatalogEntryType.DateTime)
            && (queryElem.getContentOperator() != ContentOperator.EXISTS)) {
      result.add(new QueryStructureException(
              QueryStructureExceptionType.ENGINE_CANNOT_PERFORM_OPERATION, queryElem,
              "SQL query engine cannot process DateType catalogEntries"));
    }
    if ((queryElem.getContentOperator() == ContentOperator.CONTAINS_NOT)
            || (queryElem.getContentOperator() == ContentOperator.NOT_EXISTS)) {
      result.add(new QueryStructureException(
              QueryStructureExceptionType.ENGINE_CANNOT_PERFORM_OPERATION, queryElem,
              "SQL query engine cannot process " + queryElem.getContentOperator() +
                      " parent_shell"));
    }
    return result;
  }

}