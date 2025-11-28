package de.uniwue.dw.query.model.lang;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.api.configuration.SpecialCatalogEntries;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.query.model.DWQueryConfig;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.quickSearch.ModelConverter;
import de.uniwue.misc.util.StringUtilsUniWue;
import de.uniwue.misc.util.TimeUtil;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static de.uniwue.dw.query.model.lang.ContentOperator.*;

public class DisplayStringVisitor implements IQueryElementVisitor<String> {

  public static final BigDecimal SMALL_VALUE_FOR_INTERVALL_BOUNDARY = new BigDecimal("0.0001");

  private static final int TRUNCATE_CATALOG_NAME_AFTER_CHARS = 50;

  private static final int TRUNCATE_WORD_AFTER_CHARS = 20;

  public static String getDisplayString(QueryStructureElem queryElem) throws QueryException {
    if (queryElem == null) {
      return "";
    }
    DisplayStringVisitor visitor = new DisplayStringVisitor();
    String displayString = queryElem.accept(visitor);
    displayString = makeNice(displayString);
    return displayString.trim();
  }

  public static String makeNice(String displayString) {
    if (displayString.matches("\\(.*?\\)"))
      displayString = displayString.substring(1, displayString.length() - 1);
    return displayString;
  }

  public static String createHeaderText(CatalogEntry catalogEntry) {
    return catalogEntry.getName();
  }

  protected static String formatWords(String value) {
    if (value.length() > TRUNCATE_WORD_AFTER_CHARS)
      return value.substring(0, TRUNCATE_WORD_AFTER_CHARS) + "..";
    return value;
  }

  protected static String formatCatalogName(String value) {
    if (value.length() > TRUNCATE_CATALOG_NAME_AFTER_CHARS)
      return value.substring(0, TRUNCATE_CATALOG_NAME_AFTER_CHARS) + "..";
    return value;
  }

  protected static String formatNumber(String value) {
    if (value.matches("\\d+\\.\\d{3}1")) {
      NumberFormat df = DecimalFormat.getNumberInstance();
      BigDecimal number = new BigDecimal(value);
      BigDecimal exactNumber = number.subtract(SMALL_VALUE_FOR_INTERVALL_BOUNDARY);
      return df.format(exactNumber);
    }
    return value;
  }

  protected static String getDocumentTimeDisplayText(QueryAttribute queryElem)
          throws QueryException {
    ContentOperator operator = queryElem.getContentOperator();
    String name = queryElem.getCatalogEntry().getName();
    if (operator == ContentOperator.EXISTS)
      return name;
    if (operator == ContentOperator.NOT_EXISTS)
      return "-" + name;
    if (operator == ContentOperator.EQUALS)
      return name + " = " + queryElem.getDesiredContent();
    if (operator == ContentOperator.LESS || operator == ContentOperator.LESS_OR_EQUAL)
      return " < " + formatSolrDisplayDate(queryElem, false);
    if (operator == ContentOperator.MORE || operator == ContentOperator.MORE_OR_EQUAL)
      return " > " + formatSolrDisplayDate(queryElem, true);
    if (operator == ContentOperator.BETWEEN)
      return formatSolrDisplayDate(queryElem, false);
    if (operator == PER_YEAR) {
      String[] split = queryElem.getDesiredContentSplitted();
      return name + " [" + split[0] + " , " + (Integer.valueOf(split[1]) + 1) + "]";
    }
    return name + " " + operator + " " + queryElem.getDesiredContent();
  }

  protected static String formatSolrDisplayDate(QueryAttribute anAttr, boolean isGreater)
          throws QueryException {
    String[] split = anAttr.getDesiredContentSplitted();
    String solrDate1, solrDate2 = null;
    Calendar cal2 = null;
    solrDate1 = split[0];
    if (split.length == 2) {
      solrDate2 = split[1];
      cal2 = TimeUtil.parseDate2Calendar(solrDate2);
    }
    Calendar cal1 = TimeUtil.parseDate2Calendar(solrDate1);
    if (cal1 != null) {
      if (cal2 == null) {
        if (cal1.get(Calendar.DAY_OF_YEAR) == 1)
          if (isGreater)
            return cal1.get(Calendar.YEAR) - 1 + "";
          else
            return cal1.get(Calendar.YEAR) + "";
        else
          return anAttr.getDesiredContent();
      } else {
        if (cal1.get(Calendar.DAY_OF_YEAR) == 1 && cal2.get(Calendar.DAY_OF_YEAR) == 1)
          return cal1.get(Calendar.YEAR) + "";
        else if (cal1.get(Calendar.DAY_OF_MONTH) == 1 && cal2.get(Calendar.DAY_OF_MONTH) == 1)
          return new SimpleDateFormat("MMM yyyy").format(cal1.getTime());
        else {
          String germanFormat = formatSolrDate2GermanFormat(solrDate1) + " "
                  + QueryAttribute.PERIOD_DELIMITER + " " + formatSolrDate2GermanFormat(solrDate2);
          return germanFormat;
        }
      }
    }
    return anAttr.getDesiredContent();
  }

  protected static boolean isListElem(QueryElem elem) {
    if (elem == null)
      return false;
    else
      return (elem instanceof QueryAnd || elem instanceof QueryOr);
  }

  public static String formatSolrDate2GermanFormat(String value) {
    if (value.matches("\\d{4}-\\d{1,2}-\\d{1,2}T\\d{2}:\\d{2}:\\d{2}Z")) {
      Date date;
      try {
        date = TimeUtil.getDateFormat5().parse(value);
        return TimeUtil.format2GermanFormat(date);
      } catch (ParseException e) {
        e.printStackTrace();
      }
    }
    return value;
  }

  @Override
  public String visit(QueryAttribute queryElem) throws QueryException {
    String result = queryElem.getDisplayName();
    if (result == null) {
      result = getDisplayText(queryElem);
    }
    for (QueryTempOpAbs aTempOp : queryElem.getTempOpsAbs()) {
      result += visit(aTempOp) + " ";
    }
    for (QueryTempOpRel aTempOp : queryElem.getTemporalOpsRel()) {
      result += visit(aTempOp) + " ";
    }
    for (QueryValueCompare anOp : queryElem.getValueCompares()) {
      result += visit(anOp) + " ";
    }
    return result;
  }

  protected String getDisplayText(QueryAttribute queryElem) throws QueryException {
    boolean usingKIRaImport = DWQueryConfig.getInstance().getBooleanParameter("dw.index.neo4j.useKIRaImport", false);
    if (!usingKIRaImport && (getDocumentTimeCatalogEntry() != null) && queryElem.getCatalogEntry()
            .getAttrId() == getDocumentTimeCatalogEntry().getAttrId()) {
      return getDocumentTimeDisplayText(queryElem);
    }
    String calculationCommentPrefix = "#N4JCalc#";
    String correlationAnalysisCommentPrefix = "#N4JCorr#";
    String name = createHeaderText(queryElem.getCatalogEntry());
    if (queryElem.getCatalogEntry().isRoot() && queryElem.getComment() != null &&
            queryElem.getComment().replaceFirst(correlationAnalysisCommentPrefix, "")
                    .startsWith(calculationCommentPrefix)) {
      Pattern pattern = Pattern.compile("(\\{\\w*,\\w*})");
      Matcher matcher = pattern.matcher(queryElem.getComment());
      name = "( " + queryElem.getComment().replaceFirst(correlationAnalysisCommentPrefix, "")
              .replaceFirst(calculationCommentPrefix, "").replace(" ## ", "").trim() + " )";
      while (matcher.find()) {
        String[] split = matcher.group(1).replace("{", "").replace("}", "").split(",");
        try {
          CatalogEntry curEntry = DwClientConfiguration.getInstance().getCatalogManager()
                  .getEntryByRefID(split[0].trim(), split[1].trim());
          name = name.replace(matcher.group(1), "'" + curEntry.getName() + "'");
        } catch (SQLException e) {
          throw new QueryException(e);
        }
      }
      int numOpeningBrackets = StringUtils.countMatches(name, "(");
      int numClosingBrackets = StringUtils.countMatches(name, ")");
      if (numOpeningBrackets > numClosingBrackets)
        name += ")".repeat(Math.max(0, numOpeningBrackets - numClosingBrackets));
    }
    if (queryElem.getReductionOperator() != ReductionOperator.NONE) {
      name += " - " + queryElem.getReductionOperator().getQuickSearchText();
    }
    ContentOperator operator = queryElem.getContentOperator();
    if (operator == EXISTS)
      return name;
    if (operator == NOT_EXISTS)
      return "NOT EXISTS - " + name;
    String content = queryElem.getDesiredContent().trim().replaceAll("\\s+", " ");
    if (operator == EQUALS)
      return name + " = " + content;
    if (operator == LESS)
      return name + " < " + formatNumber(content);
    if (operator == LESS_OR_EQUAL)
      return name + " <= " + formatNumber(content);
    if (operator == MORE)
      return name + " > " + formatNumber(content);
    if (operator == MORE_OR_EQUAL)
      return name + " >= " + formatNumber(content);
    if (operator == BETWEEN) {
      String[] split = queryElem.getDesiredContentSplitted();
      return name + " ]" + formatNumber(split[0]) + ", " + formatNumber(split[1]) + "]";
    }
    if (operator == PER_YEAR) {
      String[] split = queryElem.getDesiredContentSplitted();
      return name + " [" + split[0] + " , " + (Integer.valueOf(split[1]) + 1) + "]";
    }
    if (operator == CONTAINS || operator == CONTAINS_NOT || operator == CONTAINS_POSITIVE
            || operator == CONTAINS_NOT_POSITIVE)
      return name + " " + operator + " " + formatWords(content);
    return name + " " + operator.getDisplayString() + " " + formatWords(content);
  }

  private CatalogEntry getDocumentTimeCatalogEntry() {
    SpecialCatalogEntries specialCatalogEntries = DwClientConfiguration.getInstance()
            .getSpecialCatalogEntries();
    if (specialCatalogEntries != null) {
      return specialCatalogEntries.getDocumentTime();
    } else {
      return null;
    }
  }

  @Override
  public String visit(QueryOr queryElem) throws QueryException {
    if (queryElem.getName() != null) {
      String name = queryElem.getName();
      int lastIndexOfSlash = name.lastIndexOf("/");
      if (lastIndexOfSlash > 0) {
        name = name.substring(lastIndexOfSlash + 1);
      }
      return name;
    } else
      return visitQueryOr(queryElem);
  }

  private String visitQueryOr(QueryOr queryElem) throws QueryException {
    List<String> list = new ArrayList<>();
    for (QueryStructureElem qElem : queryElem.getChildren()) {
      String subQueryText = qElem.accept(this);
      if (!qElem.optional && !subQueryText.isEmpty()) {
        list.add(subQueryText);
      }
    }
    String result = StringUtilsUniWue.concat(list, " OR ");
    if (!result.isEmpty() && list.size() >= 2)
      // if (!result.isEmpty() && isListElem(queryElem.getParent()))
      result = "(" + result + ")";
    return result;
  }

  @Override
  public String visit(QueryAnd queryElem) throws QueryException {
    if (queryElem.getName() != null) {
      String name = queryElem.getName();
      int lastIndexOfSlash = name.lastIndexOf("/");
      if (lastIndexOfSlash > 0) {
        name = name.substring(lastIndexOfSlash + 1);
      }
      return name;
    } else
      return visitQueryAnd(queryElem);
  }

  private String visitQueryAnd(QueryAnd queryElem) throws QueryException {
    List<String> list = new ArrayList<>();
    for (QueryStructureElem qElem : queryElem.getChildren()) {
      String subQueryText = qElem.accept(this);
      if (!qElem.optional && !subQueryText.isEmpty()) {
        list.add(subQueryText);
      }
    }
    String result = StringUtilsUniWue.concat(list, " AND ");
    // if (!result.isEmpty() && isListElem(queryElem.getParent()))
    if (!result.isEmpty() && list.size() >= 2)
      result = "(" + result + ")";
    return result;
  }

  @Override
  public String visit(QueryRoot queryElem) throws QueryException {
    List<String> list = new ArrayList<>();
    for (QueryStructureElem child : queryElem.getChildren()) {
      String visit = child.accept(this);
      if (visit != null && !visit.isEmpty())
        list.add(visit);
    }
    return StringUtilsUniWue.concat(list, " AND ");
  }

  @Override
  public String visit(QueryIDFilter queryElem) throws QueryException {
    List<String> list = new ArrayList<>();
    for (QueryStructureElem child : queryElem.getChildren()) {
      String visit = child.accept(this);
      if (visit != null && !visit.isEmpty())
        list.add(visit);
    }
    return StringUtilsUniWue.concat(list, " AND ");
  }

  @Override
  public String visit(QueryTempFilter queryElem) throws QueryException {
    List<String> list = new ArrayList<>();
    for (QueryStructureElem ele : queryElem.getChildren())
      list.add(ele.accept(this));
    String concat = StringUtilsUniWue.concat(list, " ");
    return concat;
  }

  @Override
  public String visit(QuerySubQuery queryElem) throws QueryException {
    return ModelConverter.STORED_QUERY_PREFIX + queryElem.getName();
    // List<String> list = new ArrayList<String>();
    // for (QueryStructureElem ele : queryElem.getChildren())
    // list.add(ele.accept(this));
    // String concat = StringUtilsUniWue.concat(list, " ");
    // return concat;
  }

  @Override
  public String visit(QueryStatisticColumn queryElem) throws QueryException {
    List<String> list = new ArrayList<>();
    for (QueryStructureElem ele : queryElem.getChildren())
      list.add(ele.accept(this));
    String concat = StringUtilsUniWue.concat(list, " ");
    return concat;
  }

  @Override
  public String visit(QueryStatisticFilter queryElem) throws QueryException {
    return visitQueryAnd(queryElem);
  }

  @Override
  public String visit(QueryStatisticRow queryElem) throws QueryException {
    List<String> list = new ArrayList<>();
    for (QueryStructureElem ele : queryElem.getChildren())
      list.add(ele.accept(this));
    String concat = StringUtilsUniWue.concat(list, " ");
    return concat;
  }

  @Override
  public String visit(QueryNot queryElem) throws QueryException {
    List<String> list = new ArrayList<>();
    for (QueryStructureElem ele : queryElem.getChildren())
      list.add(ele.accept(this));
    String concat = StringUtilsUniWue.concat(list, " ");
    return concat;
  }

  @Override
  public String visit(QueryNTrue queryElem) throws QueryException {
    List<String> list = new ArrayList<>();
    for (QueryStructureElem ele : queryElem.getChildren())
      list.add(ele.accept(this));
    String concat = StringUtilsUniWue.concat(list, " ");
    return concat;
  }

  @Override
  public String visit(QueryTempOpAbs queryElem) throws QueryException {
    return queryElem.getDisplayString();
  }

  @Override
  public String visit(QueryTempOpRel queryElem) throws QueryException {
    return queryElem.getDisplayString();
  }

  @Override
  public String visit(QueryValueCompare queryElem) throws QueryException {
    return queryElem.getDisplayString();
  }

}
