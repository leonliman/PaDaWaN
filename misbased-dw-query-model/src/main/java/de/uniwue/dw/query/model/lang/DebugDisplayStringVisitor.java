package de.uniwue.dw.query.model.lang;

import de.uniwue.dw.core.model.data.CatalogEntryType;

public class DebugDisplayStringVisitor {

  public static String getDisplayString(QueryElem anElem) {
    DebugDisplayStringVisitor visitor = new DebugDisplayStringVisitor();
    if (anElem instanceof QueryAttribute) {
      return visitor.visit((QueryAttribute) anElem);
    } else if (anElem instanceof QueryIDFilter) {
      return visitor.visit((QueryIDFilter) anElem);
    } else if (anElem instanceof QueryRoot) {
      return visitor.visit((QueryRoot) anElem);
    } else if (anElem instanceof QueryStructureElem) {
      return visitor.visitElem((QueryStructureElem) anElem);
    }
    return visitor.visitElem(anElem);
  }

  public String visit(QueryRoot anElem) {
    String result = visitElem(anElem);
    if (anElem.isDisplayPID()) {
      result += " (display PID)";
    }
    if (anElem.isOnlyCount()) {
      result += " (only count)";
    }
    return result;
  }

  public String visit(QueryIDFilter anElem) {
    String result = visitElem(anElem);
    result += "(" + anElem.getFilterIDType().toString() + ")";
    return result;
  }

  public String visit(QueryAttribute anElem) {
    String result = visitElem(anElem);
    result = result.replace(anElem.getXMLName(), anElem.getQueryAttributeName());
    if (anElem.getContentOperator() != ContentOperator.EXISTS) {
      if (anElem.getContentOperator() == ContentOperator.EQUALS) {
        result += " (X = " + anElem.getDesiredContent() + ")";
      } else if (anElem.getContentOperator() == ContentOperator.LESS) {
        result += " (X < " + anElem.getDesiredContent() + ")";
      } else if (anElem.getContentOperator() == ContentOperator.LESS_OR_EQUAL) {
        result += " (X <= " + anElem.getDesiredContent() + ")";
      } else if (anElem.getContentOperator() == ContentOperator.MORE) {
        result += " (X > " + anElem.getDesiredContent() + ")";
      } else if (anElem.getContentOperator() == ContentOperator.MORE_OR_EQUAL) {
        result += " (X >= " + anElem.getDesiredContent() + ")";
      } else if (anElem.getContentOperator() == ContentOperator.BETWEEN) {
        result += " (" + anElem.getDesiredContent() + " < X < ";
      } else if (anElem.getContentOperator() == ContentOperator.CONTAINS) {
        result += " (contains '" + anElem.getDesiredContent() + "')";
      }
    }
    if (anElem.displayInfoDate()) {
      result += " (display measure time)";
    }
    if (anElem.displayCaseID()) {
      result += " (display caseID)";
    }
    if (anElem.displayDocID()) {
      result += " (display docID)";
    }
    if (anElem.getCatalogEntry().getDataType() == CatalogEntryType.Bool) {
    } else if (anElem.getReductionOperator() == ReductionOperator.NONE) {
      result += " [all]";
    } else {
      result += " [" + anElem.getReductionOperator() + "]";
    }
    for (QueryTempOpAbs op : anElem.getTempOpsAbs()) {
      result += " {" + op.getDisplayString() + "}";
    }
    for (QueryTempOpRel op : anElem.getTemporalOpsRel()) {
      result += " {" + op.getDisplayString() + "}";
    }
    return result;
  }

  public String visitElem(QueryElem anElem) {
    String result = "";
    result += anElem.getClass().toString();
    return result;
  }
  
  public String visitElem(QueryStructureElem anElem) {
    String result = "";
    if (!anElem.active) {
      result += "// ";
    }
    result += anElem.getXMLName();
    if ((anElem.getComment() != null) && !anElem.getComment().isEmpty()) {
      result += "  (" + anElem.getComment() + ")";
    }
    if (anElem.optional) {
      result += " (optional)";
    }
    return result;
  }

}
