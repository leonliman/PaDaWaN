package de.uniwue.dw.query.model.exception;

import de.uniwue.dw.query.model.lang.DebugDisplayStringVisitor;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryElem;

import java.util.Set;

public class QueryStructureException extends QueryException {

  private static final long serialVersionUID = -177671499793063588L;

  public QueryElem elem;

  public QueryStructureExceptionType structureType;

  public static String getReadableErrorString(Set<QueryStructureException> setOfErrors)
          throws QueryException {
    String result = "";
    for (QueryStructureException anEx : setOfErrors) {
      switch (anEx.structureType) {
        case MORE_THAN_ONE_ATTR_WIHTOUT_RED_OP:
          result +=
                  "Es befinden sich mehr als ein Attribut ohne Reduktions-Operator in der Anfrage. "
                          +
                          "Reduzieren sie mindestens alle bis auf einen indem sie diese z.B. auf \"erster Wert\" oder \"kleinster Wert\" setzen";
          break;
        case NO_NON_OPTIONAL_OP_FOR_ROOT:
          result += "Alle Attribute der Anfrage sind optional. Bitte setzen Sie eines davon auf nicht-optional";
          break;
        case NO_CASCADED_TIME_RESTRICTIONS:
          result += "Zeit Restriktionen sind nicht schachtelbar";
          break;
        case NO_DESIRED_CONTENT_GIVEN:
          result += "Das Attribut " + DebugDisplayStringVisitor.getDisplayString(anEx.elem)
                  + " benötigt einen \"desired content\"";
          break;
        case NO_OUTPUT_ATTRIBUTE:
          result += "Alle Attribute erzeugen keine Ausgabespalten im Ergebnis. Bitte setzten Sie mindestens ein Attribut aus \"Ausgabe erzeugen\"";
          break;
        case NO_UPPER_BOUND_PROVIDED:
          result += "Das Attribut" + DebugDisplayStringVisitor.getDisplayString(anEx.elem)
                  + " benötigt eine obere Grenze";
          break;
        case NOT_WITHOUT_CHILD:
          result += "Ein NOT hat kein Element das es negiert";
          break;
        case VALUE_IS_NO_NUMBER:
          result += "Das Attribut " + DebugDisplayStringVisitor.getDisplayString(anEx.elem)
                  + " hat einen \"desired content\" der keine Zahl ist aber eine Zahl sein muss";
          break;
        case VALUE_IS_NO_DATE:
          result += "Das Attribut " + DebugDisplayStringVisitor.getDisplayString(anEx.elem)
                  + " hat einen \"desired content\" der kein Datum ist aber ein Datum sein muss";
          break;
        case TOO_NARROW_INTERVALS:
          QueryAttribute anAttr = ((QueryAttribute) anEx.elem);
          String[] split = anAttr.getDesiredContentSplitted();
          double lastValue = 0;
          boolean isFirst = true;
          for (String s : split) {
            double curValue = Double.parseDouble(s);
            if ((curValue - lastValue < 5) && !isFirst) {
              result += "Datenschutz: Wählen Sie Intervalle von min. 5 Einheiten für das Attribut '"
                      + DebugDisplayStringVisitor.getDisplayString(anEx.elem)
                      + "'. Folgendes Intervall war zu klein: " + lastValue + " bis " + curValue;
            }
            lastValue = curValue;
            isFirst = false;
          }
          break;
        case TIME_INTERVAL_TOO_NARROW:
          result += "Datenschutz: Wählen Sie ein Intervall von min. 30 Tage für das Attribut '"
                  + DebugDisplayStringVisitor.getDisplayString(anEx.elem);
          break;
        case INTERVAL_SYNTAX:
          result += "Das Argument von '" + DebugDisplayStringVisitor.getDisplayString(anEx.elem)
                  +
                  "' muss bei dem Operator 'Intervallgrenzen' z. B. folgende Form besitzen: 18 50 65 80.5";
          break;
        case NOT_ALLOWED_FILTER_OPERATOR:
          result += "Sie haben '" + DebugDisplayStringVisitor.getDisplayString(anEx.elem)
                  + "' als Filter ausgewählt. Der Operator '"
                  + ((QueryAttribute) anEx.elem).getContentOperator()
                  + "' kann nicht als Filter verwendet werden.";
          break;
        case ENGINE_CANNOT_PERFORM_OPERATION:
          result += "Die ausgewählte Suchengine unterstützt den Operator "
                  + DebugDisplayStringVisitor.getDisplayString(anEx.elem) + " nicht";
          break;
        default:
          break;
      }
      result += "\n.";
    }
    if (setOfErrors.size() > 0) {
      result = result.substring(0, result.length() - 2);
    }
    return result;
  }

  public QueryStructureException(QueryStructureExceptionType type, QueryElem anElem) {
    this(type, anElem, type.toString());
  }

  public QueryStructureException(QueryStructureExceptionType type) {
    this(type, null);
  }

  public QueryStructureException(QueryStructureExceptionType type, QueryElem anElem,
          String aMessage) {
    super(QueryExceptionType.QUERY_STRUCTURE);
    this.structureType = type;
    this.elem = anElem;
  }

  public static enum QueryStructureExceptionType {
    NONE, ENGINE_CANNOT_PERFORM_OPERATION,
    // with more than one attribute without a reduction parent_shell the result would experience a
    // combinatorial explosion like in an SQL statement with joins and too few restrictions
    MORE_THAN_ONE_ATTR_WIHTOUT_RED_OP,
    // like with MORE_THAN_ONE_ATTR_WIHTOUT_RED_OP this is not possible
    MORE_THAN_ONE_ATTR_WITH_MULTIPLE_ROWS,
    // the query needs at least one attribute which is not optional. Otherwise the query would
    // return the whole database
    NO_NON_OPTIONAL_OP_FOR_ROOT,
    // With no output attribute the query would return nothing, which is a bit useless
    NO_OUTPUT_ATTRIBUTE,
    // A NOT always needs something it negates
    NOT_WITHOUT_CHILD,
    // time restrictions cannot be cascaded
    NO_CASCADED_TIME_RESTRICTIONS,
    // A desired content is expected but not given
    NO_DESIRED_CONTENT_GIVEN,
    // A number value is expected but not given
    VALUE_IS_NO_NUMBER,
    // A date value is expected but not given
    VALUE_IS_NO_DATE,
    // An upper bound is needed but not given
    NO_UPPER_BOUND_PROVIDED,
    // Because of data privacy issues in a statistic query the desired value intervals of
    // attribute's have to have a certain length
    TOO_NARROW_INTERVALS,
    // Because of data privacy issues in a statistic query the desired value interval of
    // a time attribute has to have a certain length
    TIME_INTERVAL_TOO_NARROW,
    // The intervals for a statistic query have the wrong syntax
    INTERVAL_SYNTAX,
    // Not allowed filter parent_shell
    NOT_ALLOWED_FILTER_OPERATOR,
    // two boundaries are expected
    INVALID_NUMBER_OF_INTERVAL_BOUNDERIES
  }

}
