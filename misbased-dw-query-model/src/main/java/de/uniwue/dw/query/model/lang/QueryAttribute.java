package de.uniwue.dw.query.model.lang;

import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryIDFilter.FilterIDType;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentLexer;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser;
import de.uniwue.misc.util.TimeUtil;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

public class QueryAttribute extends QueryStructureElem {

  public static final String[] PERIOD_DELIMITER_OLD = { "bis", "..." };

  public static final String PERIOD_DELIMITER = ";";

  // TODO: wenn der Name eines Attributes in der Query angegeben ist sollte dieser auch als Spaltenüberschrift verwendet werden

  // only Attributes and the root can have an ID. It can be used for referencing it from other
  // QueryElements (e.g. a QueryValueComp)
  private String id;

  // the catalog entry this attributes refers to
  private CatalogEntry catalogEntry;

  // if the content of this attribute should suffice certain value constraints
  // (which are further defined by 'desiredContent' and 'desiredContentBetweenUpperBound')
  private ContentOperator contentOperator = ContentOperator.EXISTS;

  // a desired number or text which should match
  private String desiredContent = "";

  // a desired upper bound for the "y < x < z"-parent_shell
  // private String desiredContentBetweenUpperBound = "";

  // does this attributes generate one (min, max, earliest, latest)
  // or multiple rows (none) in the result
  private ReductionOperator reductionOperator = ReductionOperator.NONE;

  // should the attribute's value create a column of its own ?
  private boolean displayValue = true;

  // should the attribute's measure date also create a column of its own ?
  private boolean displayInfoDate;

  // should the attribute's caseID also create a column of its own ?
  private boolean displayCaseID;

  // should the attribute's docID also create a column of its own ?
  private boolean displayDocID;

  // displayed name of the attribute. (optional)
  private String displayName;

  // should the attribute's value be displayed in the result column or
  // should only the existence of the attribute be returned (indicated by an "x" in the result
  // column)
  private boolean onlyDisplayExistence = false;

  // should the attribute match when catalog-subEntries of the given catalogEntry are found ?
  private boolean searchSubEntries = false;

  // When the reductionOperator is set to NONE and multiple values are returned for this attribute,
  // should the output contain multiple rows for this result, or should the results be concatenated
  // by "," and put into a single cell ?
  private boolean multipleRows = false;

  // should the values of the attribute be restricted to a comparison to values of other attributes
  // of the same catalogEntry ?
  private List<QueryValueCompare> valueCompares = new ArrayList<>();

  // EXCEL-stuff
  // ------------------
  // // should the cells in the resulting Excel sheet contain the value itself or
  // // should they be written into a file and referenced by a link ?
  private boolean valueInFile = false;

  // // the width of the column in the resulting Excel sheet
  private int width;

  // // some formula which is written in the first row of the Excel sheet for this column
  private String headFormula = "";

  private QueryListSelector listSelector;

  // how often does the attribute have to exist in the requested patients/cases/docs
  private int minCount = 0;

  // how often may the attribute exist in the requested patients/cases/docs
  private int maxCount = 0;

  // are the members "minCount" and "maxCount" related to patients/cases/documents
  private FilterIDType countType = FilterIDType.PID;

  private ExtractionMode extractionMode = ExtractionMode.None;

  public QueryAttribute(QueryStructureContainingElem aContainer, CatalogEntry anEntry, int position)
          throws QueryException {
    super(aContainer, position);
    setCatalogEntry(anEntry);
    if (getRoot() != null) {
      // this can be null if the attribute is not yet properly hooked up in a complete query
      String alias = getRoot().getNewAlias();
      setId(alias);
    }
  }

  public QueryAttribute(QueryStructureContainingElem aContainer, CatalogEntry anEntry)
          throws QueryException {
    this(aContainer, anEntry, (aContainer == null) ? 0 : aContainer.getChildren().size());
  }

  public QueryAttribute(CatalogEntry catalogEntry, ContentOperator contentOperator,
          String desiredContent) throws QueryException {
    this(catalogEntry, contentOperator, desiredContent, ReductionOperator.NONE);
  }

  public QueryAttribute(CatalogEntry catalogEntry, ContentOperator contentOperator,
          String desiredContent, ReductionOperator reductionOperator) throws QueryException {
    this(null, catalogEntry);
    this.contentOperator = contentOperator;
    this.desiredContent = desiredContent;
    this.setReductionOperator(reductionOperator);
  }

  public static void main(String[] args) {
    System.out.println("a,b,c".split(",", -1).length);
    System.out.println(",a,b,c".split(",", -1).length);
    System.out.println("a,b,c,".split(",", -1).length);
    System.out.println(",a,b,c,".split(",", -1).length);
  }

  @Override
  public boolean displayColumns() {
    return displayValue() || displayCaseID || displayDocID || displayInfoDate;
  }

  // this method is for the effects after a copy from a source element has been created
  @Override
  public void copyFinalize(QueryStructureElem sourceElem) {
    setId("");
  }

  public String getQueryAttributeName() {
    return getCatalogEntry().getName();
  }

  public CatalogEntry getCatalogEntry() {
    return catalogEntry;
  }

  public void setCatalogEntry(CatalogEntry catalogEntry) {
    if (catalogEntry == null) {
      throw new RuntimeException("catalogEntry has to be given");
    }
    this.catalogEntry = catalogEntry;
  }

  public String getDesiredOpString() {
    if ((contentOperator != ContentOperator.EXISTS)) {
      return contentOperator.toString();
    } else {
      return "";
    }
  }

  public QueryTempOpAbs getDesiredDate() {
    if (getTempOpsAbs().size() > 0) {
      return getTempOpsAbs().get(0);
    } else {
      return null;
    }
  }

  public boolean hasRestrictionsWithOtherAttributes() {
    List<QueryIDFilter> ancestorIDFilters = getAncestorIDFilters();
    for (QueryIDFilter aFilter : ancestorIDFilters) {
      for (QueryAttribute anAttr : aFilter.getAttributesRecursive()) {
        if (anAttr != this) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public void generateXML(XMLStreamWriter writer) throws QueryException {
    try {
      writer.writeStartElement(getXMLName());
      generateXMLAttributes(writer);
      for (QueryTempOpAbs anOp : getTempOpsAbs()) {
        anOp.generateXML(writer);
      }
      for (QueryTempOpRel anOp : getTemporalOpsRel()) {
        anOp.generateXML(writer);
      }
      for (QueryValueCompare anOp : getValueCompares()) {
        anOp.generateXML(writer);
      }
      writer.writeEndElement();
    } catch (XMLStreamException e) {
      throw new QueryException(e);
    }
  }

  @Override
  public void generateXMLAttributes(XMLStreamWriter writer) throws XMLStreamException {
    super.generateXMLAttributes(writer);
    writer.writeAttribute("extID", getCatalogEntry().getExtID());
    writer.writeAttribute("domain", getCatalogEntry().getProject());
    if (getCatalogEntry().getName() != null) {
      writer.writeAttribute("attrName", getCatalogEntry().getName());
    }
    if (getId() != null) {
      writer.writeAttribute("elementID", getId());
    }
    if (!displayValue()) {
      writer.writeAttribute("displayValue", ((Boolean) displayValue()).toString());
    }
    if (isOnlyDisplayExistence()) {
      writer.writeAttribute("onlyDisplayExistence",
              ((Boolean) isOnlyDisplayExistence()).toString());
    }
    if (getValueInFile()) {
      writer.writeAttribute("valueInFile", Boolean.toString(getValueInFile()));
    }
    if (contentOperator != ContentOperator.EXISTS) {
      writer.writeAttribute("contentOperator", contentOperator.toString());
    }
    if ((getDesiredContent() != null) && !getDesiredContent().isEmpty()) {
      writer.writeAttribute("desiredContent", getDesiredContent());
    }
    if (displayInfoDate()) {
      writer.writeAttribute("infoDate", Boolean.toString(displayInfoDate()));
    }
    if (displayCaseID()) {
      writer.writeAttribute("caseID", Boolean.toString(displayCaseID()));
    }
    if (displayDocID()) {
      writer.writeAttribute("docID", Boolean.toString(displayDocID()));
    }
    if (getReductionOperator() != ReductionOperator.NONE) {
      writer.writeAttribute("reductionOp", getReductionOperator().toString());
    }
    if (isMultipleRows()) {
      writer.writeAttribute("multipleRows", Boolean.toString(isMultipleRows()));
    }
    if (getCatalogEntry().getDataType() == CatalogEntryType.Bool) {
      if (!isSearchSubEntries()) {
        writer.writeAttribute("searchSubEntries", Boolean.toString(isSearchSubEntries()));
      }
    } else {
      if (isSearchSubEntries()) {
        writer.writeAttribute("searchSubEntries", Boolean.toString(isSearchSubEntries()));
      }
    }
    if (displayName != null) {
      writer.writeAttribute("displayName", displayName);
    }
    if (getExtractionMode() != ExtractionMode.None) {
      writer.writeAttribute("extractionMode", getExtractionMode().toString());
    }
  }

  @Override
  public List<QueryStructureElem> getReducingElemsRecursive() {
    List<QueryStructureElem> result = new ArrayList<>();
    if (getReductionOperator() != ReductionOperator.NONE) {
      result.add(this);
    }
    return result;
  }

  @Override
  public List<QueryAttribute> getAttributesRecursive() {
    List<QueryAttribute> result = new ArrayList<>();
    result.add(this);
    return result;
  }

  @Override
  public List<QueryStructureElem> getChildren() {
    return new ArrayList<>();
  }

  @Override
  public String getXMLName() {
    return "Attribute";
  }

  public String getDesiredContent() {
    return desiredContent;
  }

  public void setDesiredContent(String desiredContent) {
    if (desiredContent != null && catalogEntry.getDataType().equals(CatalogEntryType.Number)
            && desiredContent.contains(",")) {
      if (!desiredContent.contains(PERIOD_DELIMITER)) {
        try {
          setDesiredContent(
                  NumberFormat.getInstance(Locale.GERMANY).parse(desiredContent).doubleValue());
        } catch (ParseException e) {
          this.desiredContent = desiredContent;
        }
      } else {
        String[] desiredContentSplit = desiredContent.split(PERIOD_DELIMITER);
        if (desiredContentSplit.length == 2) {
          try {
            double lowerBound = NumberFormat.getInstance(Locale.GERMANY).parse(desiredContentSplit[0]).doubleValue();
            double upperBound = NumberFormat.getInstance(Locale.GERMANY).parse(desiredContentSplit[1]).doubleValue();
            setDesiredContent(lowerBound, upperBound);
          } catch (ParseException e) {
            this.desiredContent = desiredContent;
          }
        } else {
          this.desiredContent = desiredContent;
        }
      }
    } else {
      this.desiredContent = desiredContent;
    }
  }

  public void setDesiredContent(double desiredContent) {
    this.desiredContent = Double.toString(desiredContent);
  }

  public void setDesiredContent(Timestamp desiredContent) {
    String contentString = TimeUtil.getSdfWithTime().format(desiredContent);
    setDesiredContent(contentString);
  }

  public Double getDesiredContentDouble() {
    return Double.valueOf(desiredContent);
  }

  public Date getDesiredContentDate() {
    return TimeUtil.parseDate(desiredContent);
  }

  public void setDesiredContent(double desiredContent, double desiredContentUpperBound) {
    setDesiredContent(desiredContent + PERIOD_DELIMITER + desiredContentUpperBound);
  }

  public void setDesiredContent(Timestamp desiredContent, Timestamp desiredContentUpperBound) {
    String contentString1 = TimeUtil.getSdfWithTime().format(desiredContent);
    String contentString2 = TimeUtil.getSdfWithTime().format(desiredContentUpperBound);
    setDesiredContent(contentString1 + PERIOD_DELIMITER + contentString2);
  }

  public String[] getDesiredContentSplitted() throws QueryException {
    if (desiredContent != null) {
      String splitter = PERIOD_DELIMITER;
      for (String oldPeriodDelimiter : PERIOD_DELIMITER_OLD) {
        if (desiredContent.contains(oldPeriodDelimiter)) {
          splitter = oldPeriodDelimiter;
        }
      }
      splitter = "\\s*" + Pattern.quote(splitter) + "\\s*";
      String[] split = desiredContent.split(splitter, -1);
      if ((split.length > 0) && split[0].isEmpty()) {
        if (getCatalogEntry().getDataType() == CatalogEntryType.Number) {
          split[0] = Integer.toString(Integer.MIN_VALUE);
        } else if (getCatalogEntry().getDataType() == CatalogEntryType.DateTime) {
          // TODO: welches Format wird hier erwartet ?
          split[0] = Integer.toString(Integer.MIN_VALUE);
        } else {
          throw new QueryException("Wrong data type");
        }
      }
      if ((split.length > 0) && split[split.length - 1].isEmpty()) {
        if (getCatalogEntry().getDataType() == CatalogEntryType.Number) {
          split[split.length - 1] = Integer.toString(Integer.MAX_VALUE);
        } else if (getCatalogEntry().getDataType() == CatalogEntryType.DateTime) {
          // TODO: welches Format wird hier erwartet ?
          split[split.length - 1] = Integer.toString(Integer.MAX_VALUE);
        } else {
          throw new QueryException("Wrong data type");
        }
      }
      return split;
    }
    return new String[] {};
  }

  // I am referenced by which relative temporal operators ?
  public List<QueryTempOpRel> getReferencingTempRelOps() {
    List<QueryTempOpRel> result = new ArrayList<>();
    for (QueryTempOpRel anOp : getRoot().getTempOpsRelRecursive()) {
      if (anOp.getRefElem() == this) {
        result.add(anOp);
      }
    }
    return result;
  }

  // I am referenced by which relative quantitative operators ?
  public List<QueryValueCompare> getReferencingValueRelOps() {
    List<QueryValueCompare> result = new ArrayList<>();
    for (QueryValueCompare anOp : getRoot().getValueComparesRecursive()) {
      if (anOp.refElem == this) {
        result.add(anOp);
      }
    }
    return result;
  }

  /**
   * Returns a part of the split desired content. the index starts with 0.
   */
  public String getDesiredContentSplitted(int index) throws QueryException {
    String[] splitted = getDesiredContentSplitted();
    if (index >= 0 && index < splitted.length)
      return splitted[index];
    else
      return "";
  }

  public double getDesiredContentBetweenUpperBoundDouble()
          throws NumberFormatException, QueryException {
    return Double.parseDouble(getDesiredContentSplitted(1));
  }

  public double getDesiredContentBetweenLowerBoundDouble()
          throws NumberFormatException, QueryException {
    return Double.parseDouble(getDesiredContentSplitted(0));
  }

  public Date getDesiredContentBetweenUpperBoundDate() throws QueryException {
    return TimeUtil.parseDate(getDesiredContentSplitted(1));
  }

  public Date getDesiredContentBetweenLowerBoundDate() throws QueryException {
    return TimeUtil.parseDate(getDesiredContentSplitted(0));
  }

  public ParseTree getDesiredContentAsParseTree() {
    String desiredContentToUse = desiredContent;
    if (desiredContent.startsWith("{") && desiredContent.endsWith("}"))
      desiredContentToUse = desiredContentToUse.substring(1, desiredContentToUse.length() - 1);
    TextContentLexer lexer = new TextContentLexer(new ANTLRInputStream(desiredContentToUse));
    TextContentParser parser = new TextContentParser(new CommonTokenStream(lexer));
    return parser.expression();
  }

  public boolean getValueInFile() {
    return valueInFile;
  }

  public void setValueInFile(boolean valueInFile) {
    this.valueInFile = valueInFile;
  }

  public int getWidth() {
    return width;
  }

  public void setWidth(int width) {
    this.width = width;
  }

  public boolean displayInfoDate() {
    return displayInfoDate;
  }

  public void setDisplayInfoDate(boolean infoDate) {
    this.displayInfoDate = infoDate;
  }

  public boolean displayCaseID() {
    return displayCaseID;
  }

  public void setDisplayCaseID(boolean caseIDFlag) {
    this.displayCaseID = caseIDFlag;
  }

  public boolean displayDocID() {
    return displayDocID;
  }

  public void setDisplayDocID(boolean docIDFlag) {
    this.displayDocID = docIDFlag;
  }

  public ContentOperator getContentOperator() {
    return contentOperator;
  }

  public void setContentOperator(ContentOperator contentOperator) {
    this.contentOperator = contentOperator;
  }

  public String getHeadFormula() {
    return headFormula;
  }

  public void setHeadFormula(String headFormula) {
    this.headFormula = headFormula;
  }

  @Override
  public List<QueryStructureElem> getSiblings() {
    return new ArrayList<>();
  }

  @Override
  public <T> T accept(IQueryElementVisitor<T> visitor) throws QueryException {
    return visitor.visit(this);
  }

  public ReductionOperator getReductionOperator() {
    return reductionOperator;
  }

  public void setReductionOperator(ReductionOperator reductionOperator) {
    this.reductionOperator = reductionOperator;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(getCatalogEntry().getName()).append(" ");
    result.append(contentOperator).append(" ");
    String content = desiredContent;
    if (desiredContent != null && desiredContent.length() > 20) {
      content = desiredContent.substring(0, 20);
      content += "..";
    }
    result.append(content).append(" ");
    if (optional)
      result.append("(optional) ");
    return result.toString();
  }

  public String getDisplayName() {
    return displayName;
  }

  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  public boolean displayValue() {
    return displayValue;
  }

  public void setDisplayValue(boolean displayValue) {
    this.displayValue = displayValue;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public boolean isOnlyDisplayExistence() {
    return onlyDisplayExistence;
  }

  public void setOnlyDisplayExistence(boolean onlyDisplayExistence) {
    this.onlyDisplayExistence = onlyDisplayExistence;
  }

  public boolean isSearchSubEntries() {
    return searchSubEntries;
  }

  public void setSearchSubEntries(boolean searchSubEntries) {
    this.searchSubEntries = searchSubEntries;
  }

  public boolean hasToBeMadeMultipleRows() {
    return isMultipleRows();
  }

  public boolean isMultipleRows() {
    return multipleRows;
  }

  public void setMultipleRows(boolean multipleRows) {
    this.multipleRows = multipleRows;
  }

  public List<QueryValueCompare> getValueCompares() {
    return valueCompares;
  }

  public void setValueCompares(List<QueryValueCompare> valueCompares) {
    this.valueCompares = valueCompares;
  }

  public QueryListSelector getListSelector() {
    return listSelector;
  }

  public void setListSelector(QueryListSelector listSelector) {
    this.listSelector = listSelector;
  }

  public int getMinCount() {
    return minCount;
  }

  public void setMinCount(int minCount) {
    this.minCount = minCount;
  }

  public int getMaxCount() {
    return maxCount;
  }

  public void setMaxCount(int maxCount) {
    this.maxCount = maxCount;
  }

  public FilterIDType getCountType() {
    return countType;
  }

  public void setCountType(FilterIDType countType) {
    this.countType = countType;
  }

  public ExtractionMode getExtractionMode() {
    return extractionMode;
  }

  public void setExtractionMode(ExtractionMode extractionMode) {
    this.extractionMode = extractionMode;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((catalogEntry == null) ? 0 : catalogEntry.hashCode());
    result = prime * result + ((contentOperator == null) ? 0 : contentOperator.hashCode());
    result = prime * result + ((countType == null) ? 0 : countType.hashCode());
    result = prime * result + ((desiredContent == null) ? 0 : desiredContent.hashCode());
    result = prime * result + (displayCaseID ? 1231 : 1237);
    result = prime * result + (displayDocID ? 1231 : 1237);
    result = prime * result + (displayInfoDate ? 1231 : 1237);
    result = prime * result + ((displayName == null) ? 0 : displayName.hashCode());
    result = prime * result + (displayValue ? 1231 : 1237);
    result = prime * result + ((extractionMode == null) ? 0 : extractionMode.hashCode());
    result = prime * result + ((headFormula == null) ? 0 : headFormula.hashCode());
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    result = prime * result + ((listSelector == null) ? 0 : listSelector.hashCode());
    result = prime * result + maxCount;
    result = prime * result + minCount;
    result = prime * result + (multipleRows ? 1231 : 1237);
    result = prime * result + (onlyDisplayExistence ? 1231 : 1237);
    result = prime * result + ((reductionOperator == null) ? 0 : reductionOperator.hashCode());
    result = prime * result + (searchSubEntries ? 1231 : 1237);
    result = prime * result + ((valueCompares == null) ? 0 : valueCompares.hashCode());
    result = prime * result + (valueInFile ? 1231 : 1237);
    result = prime * result + width;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    QueryAttribute other = (QueryAttribute) obj;
    if (catalogEntry == null) {
      if (other.catalogEntry != null)
        return false;
    } else if (!catalogEntry.equals(other.catalogEntry))
      return false;
    if (contentOperator != other.contentOperator)
      return false;
    if (countType != other.countType)
      return false;
    if (desiredContent == null) {
      if (other.desiredContent != null)
        return false;
    } else if (!desiredContent.equals(other.desiredContent))
      return false;
    if (displayCaseID != other.displayCaseID)
      return false;
    if (displayDocID != other.displayDocID)
      return false;
    if (displayInfoDate != other.displayInfoDate)
      return false;
    if (displayName == null) {
      if (other.displayName != null)
        return false;
    } else if (!displayName.equals(other.displayName))
      return false;
    if (displayValue != other.displayValue)
      return false;
    if (extractionMode != other.extractionMode)
      return false;
    if (headFormula == null) {
      if (other.headFormula != null)
        return false;
    } else if (!headFormula.equals(other.headFormula))
      return false;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    if (listSelector == null) {
      if (other.listSelector != null)
        return false;
    } else if (!listSelector.equals(other.listSelector))
      return false;
    if (maxCount != other.maxCount)
      return false;
    if (minCount != other.minCount)
      return false;
    if (multipleRows != other.multipleRows)
      return false;
    if (onlyDisplayExistence != other.onlyDisplayExistence)
      return false;
    if (reductionOperator != other.reductionOperator)
      return false;
    if (searchSubEntries != other.searchSubEntries)
      return false;
    if (valueCompares == null) {
      if (other.valueCompares != null)
        return false;
    } else if (!valueCompares.equals(other.valueCompares))
      return false;
    if (valueInFile != other.valueInFile)
      return false;
    return width == other.width;
  }

}
