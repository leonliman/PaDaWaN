package de.uniwue.dw.query.model.table;

import static de.uniwue.dw.query.model.lang.ContentOperator.BETWEEN;
import static de.uniwue.dw.query.model.lang.ContentOperator.CONTAINS;
import static de.uniwue.dw.query.model.lang.ContentOperator.CONTAINS_NOT;
import static de.uniwue.dw.query.model.lang.ContentOperator.CONTAINS_NOT_POSITIVE;
import static de.uniwue.dw.query.model.lang.ContentOperator.CONTAINS_POSITIVE;
import static de.uniwue.dw.query.model.lang.ContentOperator.EMPTY;
import static de.uniwue.dw.query.model.lang.ContentOperator.EQUALS;
import static de.uniwue.dw.query.model.lang.ContentOperator.EXISTS;
import static de.uniwue.dw.query.model.lang.ContentOperator.LESS;
import static de.uniwue.dw.query.model.lang.ContentOperator.LESS_OR_EQUAL;
import static de.uniwue.dw.query.model.lang.ContentOperator.MORE;
import static de.uniwue.dw.query.model.lang.ContentOperator.MORE_OR_EQUAL;
import static de.uniwue.dw.query.model.lang.ContentOperator.NOT_EXISTS;
import static de.uniwue.dw.query.model.lang.ContentOperator.PER_INTERVALS;
import static de.uniwue.dw.query.model.lang.ContentOperator.PER_MONTH;
import static de.uniwue.dw.query.model.lang.ContentOperator.PER_YEAR;
import de.uniwue.dw.query.model.lang.ContentOperator;

public class Operator {

  public static final ContentOperator[] STATISTIC_TOOL_BOOL_OPERATORS = { EXISTS, NOT_EXISTS };

  public static final ContentOperator[] STATISTIC_TOOL_STRUCTURE_OPERATORS = { EXISTS, NOT_EXISTS };

  public static final ContentOperator[] STATISTIC_TOOL_NUMERIC_OPERATORS = { EXISTS, NOT_EXISTS,
      LESS_OR_EQUAL, LESS, MORE, MORE_OR_EQUAL, PER_INTERVALS };

  public static final ContentOperator[] STATISTIC_TOOL_DATE_OPERATORS = { EXISTS, NOT_EXISTS,
      BETWEEN, PER_MONTH, PER_YEAR };

  public static final ContentOperator[] STATISTIC_TOOL_TEXT_OPERATORS = { EXISTS, NOT_EXISTS,
      CONTAINS, CONTAINS_NOT, CONTAINS_POSITIVE, CONTAINS_NOT_POSITIVE };

  public static final ContentOperator[] SE_TEXT_OPERATORS = { EMPTY, EXISTS, NOT_EXISTS, CONTAINS,
      CONTAINS_NOT, CONTAINS_POSITIVE, CONTAINS_NOT_POSITIVE };

  public static final ContentOperator[] SE_ALL_OPERATORS = { EMPTY, LESS, LESS_OR_EQUAL, EQUALS,
      MORE, MORE_OR_EQUAL, BETWEEN, EXISTS, NOT_EXISTS };

  public static final ContentOperator[] SE_BOOL_OPERATORS = { EMPTY, EXISTS, NOT_EXISTS };

  public static final ContentOperator[] SE_STRUCTURE_OPERATORS = { EMPTY, EXISTS, NOT_EXISTS };

  public static final ContentOperator[] NO_OPERATORS = {};

  public static final ContentOperator[] SE_DATE_OPERATORS = { EMPTY, LESS, LESS_OR_EQUAL, MORE,
      MORE_OR_EQUAL, BETWEEN, EXISTS, NOT_EXISTS };

  // this is used to format numeric arguments: e.g. replacing . and ,
  public static final ContentOperator[] ALL_NUMERIC_OPERATORS_WITH_ARGUMENT = { LESS,
      LESS_OR_EQUAL, EQUALS, MORE, MORE_OR_EQUAL, PER_INTERVALS, BETWEEN };

}
