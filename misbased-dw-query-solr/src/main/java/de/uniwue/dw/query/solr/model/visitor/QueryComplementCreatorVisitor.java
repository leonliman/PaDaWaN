package de.uniwue.dw.query.solr.model.visitor;

import java.io.IOException;
import java.sql.SQLException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;

import org.xml.sax.SAXException;

import de.uniwue.dw.core.model.manager.ICatalogClientManager;
import de.uniwue.dw.query.model.DWQueryConfig;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.ContentOperator;
import de.uniwue.dw.query.model.lang.IQueryElementVisitor;
import de.uniwue.dw.query.model.lang.QueryAnd;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryElem;
import de.uniwue.dw.query.model.lang.QueryIDFilter;
import de.uniwue.dw.query.model.lang.QueryNTrue;
import de.uniwue.dw.query.model.lang.QueryNot;
import de.uniwue.dw.query.model.lang.QueryOr;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.lang.QueryStatisticColumn;
import de.uniwue.dw.query.model.lang.QueryStatisticFilter;
import de.uniwue.dw.query.model.lang.QueryStatisticRow;
import de.uniwue.dw.query.model.lang.QueryStructureContainingElem;
import de.uniwue.dw.query.model.lang.QueryStructureElem;
import de.uniwue.dw.query.model.lang.QuerySubQuery;
import de.uniwue.dw.query.model.lang.QueryTempFilter;
import de.uniwue.dw.query.model.lang.QueryTempOpAbs;
import de.uniwue.dw.query.model.lang.QueryTempOpRel;
import de.uniwue.dw.query.model.lang.QueryValueCompare;
import de.uniwue.dw.query.model.manager.QueryManipulationManager;

public class QueryComplementCreatorVisitor implements IQueryElementVisitor<QueryStructureElem> {
  private ICatalogClientManager catalogClientManager;

  public QueryComplementCreatorVisitor() throws SQLException {
    this(DWQueryConfig.getInstance().getCatalogClientManager());

  }

  public QueryComplementCreatorVisitor(ICatalogClientManager catalogClientManager) {
    this.catalogClientManager = catalogClientManager;
  }

  @Override
  public QueryStructureElem visit(QueryAttribute queryElem) throws QueryException {
    try {
      return invertOperator(queryElem);
    } catch (XMLStreamException | SQLException | ParserConfigurationException | SAXException | IOException e) {
      throw new QueryException(e);
    }
  }

  private QueryAttribute invertOperator(QueryAttribute elem) throws QueryException, XMLStreamException, SQLException,
          ParserConfigurationException, SAXException, IOException {
    QueryStructureElem copyElem = QueryStructureElem.copyElem(catalogClientManager, elem);
    QueryAttribute copiedAttribute = (QueryAttribute) copyElem;
    ContentOperator operator = copiedAttribute.getContentOperator();
    ContentOperator newOperator = operator;
    // String newContent = elem.getDesiredContent();
    switch (operator) {
      case CONTAINS:
        newOperator = ContentOperator.CONTAINS_NOT;
        break;
      case CONTAINS_POSITIVE:
        newOperator = ContentOperator.CONTAINS_NOT_POSITIVE;
        break;
      case CONTAINS_NOT:
        newOperator = ContentOperator.CONTAINS;
        break;
      case CONTAINS_NOT_POSITIVE:
        newOperator = ContentOperator.CONTAINS_POSITIVE;
        break;
      case EXISTS:
        newOperator = ContentOperator.NOT_EXISTS;
        break;
      case NOT_EXISTS:
        newOperator = ContentOperator.EXISTS;
        break;
      case LESS:
        newOperator = ContentOperator.MORE_OR_EQUAL;
        break;
      case LESS_OR_EQUAL:
        newOperator = ContentOperator.MORE;
        break;
      case EQUALS:
        newOperator = ContentOperator.CONTAINS_NOT;
        break;
      case MORE_OR_EQUAL:
        newOperator = ContentOperator.LESS;
        break;
      case MORE:
        newOperator = ContentOperator.LESS_OR_EQUAL;
      default:
        break;
    }
    copiedAttribute.setContentOperator(newOperator);
    return copiedAttribute;
  }

  @Override
  public QueryStructureElem visit(QueryTempOpAbs queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryStructureElem visit(QueryValueCompare queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryStructureElem visit(QueryTempOpRel queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryStructureElem visit(QuerySubQuery queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryStructureElem visit(QueryStatisticColumn queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryStructureElem visit(QueryStatisticFilter queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryStructureElem visit(QueryStatisticRow queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryStructureElem visit(QueryNot queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryStructureElem visit(QueryNTrue queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryStructureElem visit(QueryOr queryElem) throws QueryException {
    return buildAndList(queryElem);
  }

  @Override
  public QueryStructureElem visit(QueryTempFilter queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryStructureElem visit(QueryAnd queryElem) throws QueryException {
    return buildOrList(queryElem);
  }

  private QueryStructureElem buildOrList(QueryStructureContainingElem queryElem) throws QueryException {
    QueryOr orList = new QueryOr(null);
    for (QueryElem child : queryElem.getChildren()) {
      QueryStructureElem transformedChild = child.accept(this);
      transformedChild.setParent(orList);
      orList.addChild(transformedChild);
    }
    return orList;
  }

  private QueryStructureElem buildAndList(QueryStructureContainingElem queryElem) throws QueryException {
    QueryAnd andList = new QueryAnd(null);
    for (QueryElem child : queryElem.getChildren()) {
      QueryStructureElem transformedChild = child.accept(this);
      transformedChild.setParent(andList);
      andList.addChild(transformedChild);
    }
    return andList;
  }

  @Override
  public QueryStructureElem visit(QueryRoot queryElem) throws QueryException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryStructureElem visit(QueryIDFilter idFilter) throws QueryException {
    QueryStructureElem orList = buildOrList(idFilter);
    idFilter.removeAllChildren();
    idFilter.addChild(orList);
    orList.setParent(idFilter);
    return idFilter;
  }

}
