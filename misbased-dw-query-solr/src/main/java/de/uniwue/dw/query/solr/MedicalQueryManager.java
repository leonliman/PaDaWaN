package de.uniwue.dw.query.solr;

import de.uniwue.dw.query.model.lang.ContentOperator;

public class MedicalQueryManager {

  private static final String XML_QUERY_TAG = "Query";

  private static final String XML_ATTRIBUTE_TAG = "Attribute";

  private static final String XML_SUB_QUERY_TAG = "SubQuery";

  private static final Object XML_AND_TAG = "And";

  private static final Object XML_OR_TAG = "Or";

  private static final Object XML_IDFILTER_TAG = "IDFilter";

  private static final Object XML_SELECTSTRING_TAG = "SelectString";

  private static final Object XML_TEXT_TAG = "#text";

  public static ContentOperator mqlOperator2AttributeOperator(String mqlOperator) {
    if (mqlOperator == null)
      return ContentOperator.EMPTY;
    switch (mqlOperator) {
      case "more":
        return ContentOperator.MORE;
      case "moreOrEqual":
        return ContentOperator.MORE_OR_EQUAL;
      case "equals":
        return ContentOperator.EQUALS;
      case "less":
        return ContentOperator.LESS;
      case "lessOrEqual":
        return ContentOperator.LESS_OR_EQUAL;
      case "between":
        return ContentOperator.BETWEEN;
      case "contains":
        return ContentOperator.CONTAINS;
      case "containsPositive":
        return ContentOperator.CONTAINS_POSITIVE;
      case "containsNotPositive":
        return ContentOperator.CONTAINS_NOT_POSITIVE;
      case "exists":
        return ContentOperator.EXISTS;
      case "notExists":
        return ContentOperator.NOT_EXISTS;
      case "perYear":
        return ContentOperator.PER_YEAR;
      case "perMonth":
        return ContentOperator.PER_MONTH;
      default:
        return ContentOperator.EMPTY;
    }
  }

  //
  // private Node attributeNode;
  //
  // public QueryAllQueriesManager allQueriesManager;
  //
  // public SQLQueryLogAdapter logAdapter;
  //
  // public CatalogManager catalogManager;
  //
  //
  // public MedicalQueryManager(DW3DataSourcePrefs somePrefs) throws SQLException {
  // allQueriesManager = new QueryAllQueriesManager(somePrefs.getSQLManager());
  // logAdapter = new SQLQueryLogAdapter(somePrefs.getSQLManager());
  // catalogManager = new CatalogManager(somePrefs.getSQLManager());
  // }
  //
  //
  // public void logQuery(Query query) throws QueryException {
  // String mql = toMQL(query);
  // try {
  // logAdapter.insert(mql);
  // } catch (UnknownHostException e) {
  // throw new QueryException(e);
  // } catch (SQLException e) {
  // throw new QueryException(e);
  // }
  // }
  //
  //
  // public String toMQL(Query query) throws QueryException {
  // StringBuilder mql = new StringBuilder();
  // mql.append("<Query caseID=\"true\">\n");
  // mql.append("<IDFilter filterIDType=\"CaseID\">\n");
  // AndList elem = query.getQueryElements();
  // for (QueryElement aSubElem : elem.getQueryElements()) {
  // mql.append(queryElement2MQL(aSubElem, 0));
  // }
  // mql.append("</IDFilter>\n");
  // String selectString = query.getSelectString();
  // if ((selectString != null) && !selectString.isEmpty()) {
  // mql.append(" <SelectString value=\"" + selectString + "\" />\n");
  // }
  // mql.append("</Query>\n");
  // return mql.toString();
  // }
  //
  //
  // private String queryElement2MQL(QueryElement elem, int offset) {
  // if (elem instanceof AndList) {
  // return andList2MQL((AndList) elem, offset);
  // } else if (elem instanceof OrList) {
  // return orList2MQL((OrList) elem, offset);
  // } else if (elem instanceof Attribute)
  // return attr2MQL((Attribute) elem, offset);
  // else
  // throw new IllegalArgumentException("Unknown type");
  // }
  //
  //
  // private String attr2MQL(Attribute ele, int offset) {
  // StringBuilder mql = new StringBuilder();
  // mql.append(getIndentation(offset));
  // mql.append("<Attribute ");
  // if (ele.catalogEntry != null) {
  // mql.append(prop2XML("domain", ele.catalogEntry.project));
  // mql.append(prop2XML("extID", ele.catalogEntry.extID));
  // mql.append(prop2XML("name", ele.catalogEntry.name));
  // } else {
  // mql.append(prop2XML("domain", "Anfragen"));
  // if (ele.rawQuery != null) {
  // mql.append(prop2XML("name", ele.rawQuery.name));
  // }
  // }
  // if (ele.parent_shell.isEmpty())
  // mql.append(prop2XML("optional", "true"));
  // else if (ele.parent_shell.equals(Attribute.GREATER))
  // mql.append(prop2XML("contentOperator", "more"));
  // else if (ele.parent_shell.equals(Attribute.GREATER_EQUALS))
  // mql.append(prop2XML("contentOperator", "moreOrEqual"));
  // else if (ele.parent_shell.equals(Attribute.LESS))
  // mql.append(prop2XML("contentOperator", "less"));
  // else if (ele.parent_shell.equals(Attribute.LESS_EQAULS))
  // mql.append(prop2XML("contentOperator", "lessOrEqual"));
  // else if (ele.parent_shell.equals(Attribute.IN_RANGE))
  // mql.append(prop2XML("contentOperator", "between"));
  // else if (ele.parent_shell.equals(Attribute.EQUALS))
  // mql.append(prop2XML("contentOperator", "equals"));
  // else if (ele.parent_shell.equals(Attribute.CONTAINS))
  // mql.append(prop2XML("contentOperator", "contains"));
  // else if (ele.parent_shell.equals(Attribute.CONTAINS_POSITIVE))
  // mql.append(prop2XML("contentOperator", "containsPositive"));
  // else if (ele.parent_shell.equals(Attribute.EXITS))
  // mql.append(prop2XML("contentOperator", "exists"));
  // else if (ele.parent_shell.equals(Attribute.NOT_EXITS))
  // mql.append(prop2XML("contentOperator", "notExists"));
  // if ((ele.alias != null) && !ele.alias.isEmpty()) {
  // mql.append(prop2XML("alias", ele.alias));
  // }
  // if (!ele.argument.isEmpty())
  // mql.append(prop2XML("desiredContent", ele.argument));
  // if (ele.mutipleValueSelector != ReductionOperator.Latest)
  // mql.append(prop2XML("mutipleValueSelector", ele.mutipleValueSelector.toString()));
  // mql.append("/>");
  // return mql.toString();
  // }
  //
  //
  // private String orList2MQL(OrList elem, int offset) {
  // StringBuilder sb = new StringBuilder();
  // sb.append(getIndentation(offset) + "<Or ");
  // if (elem.isOptional())
  // sb.append(prop2XML("optional", "true"));
  // sb.append(">\n");
  // for (QueryElement e : elem.getQueryElements()) {
  // sb.append(getIndentation(offset) + queryElement2MQL(e, offset + 1) + "\n");
  // }
  // sb.append(getIndentation(offset) + "</Or>");
  // return sb.toString();
  // }
  //
  //
  // private String andList2MQL(AndList elem, int offset) {
  // StringBuilder sb = new StringBuilder();
  // sb.append(getIndentation(offset) + "<And ");
  // if (elem.isOptional())
  // sb.append(prop2XML("optional", "true"));
  // sb.append(">\n");
  // for (QueryElement e : elem.getQueryElements()) {
  // sb.append(queryElement2MQL(e, offset + 1) + "\n");
  // }
  // sb.append(getIndentation(offset) + "</And>\n");
  // return sb.toString();
  // }
  //
  //
  // private String getIndentation(int depth) {
  // StringBuilder sb = new StringBuilder();
  // for (int i = 0; i <= depth; i++)
  // sb.append(" ");
  // return sb.toString();
  // }
  //
  //
  // private String toMQL(Attribute ele) {
  // StringBuilder mql = new StringBuilder();
  // mql.append("<Attribute ");
  // mql.append(prop2XML("attrID", ele.catalogEntry.getAttrId()));
  // mql.append(prop2XML("domain", ele.catalogEntry.project));
  // mql.append(prop2XML("extID", ele.catalogEntry.extID));
  // mql.append(prop2XML("name", ele.catalogEntry.name));
  // if (ele.parent_shell.isEmpty())
  // mql.append(prop2XML("optional", "true"));
  // else if (ele.parent_shell.equals(Attribute.GREATER))
  // mql.append(prop2XML("contentOperator", "more"));
  // else if (ele.parent_shell.equals(Attribute.GREATER_EQUALS))
  // mql.append(prop2XML("contentOperator", "moreOrEqual"));
  // else if (ele.parent_shell.equals(Attribute.EQUALS))
  // mql.append(prop2XML("contentOperator", "equals"));
  // else if (ele.parent_shell.equals(Attribute.LESS))
  // mql.append(prop2XML("contentOperator", "less"));
  // else if (ele.parent_shell.equals(Attribute.LESS_EQAULS))
  // mql.append(prop2XML("contentOperator", "lessOrEqual"));
  // else if (ele.parent_shell.equals(Attribute.IN_RANGE))
  // mql.append(prop2XML("contentOperator", "between"));
  // else if (ele.parent_shell.equals(Attribute.CONTAINS))
  // mql.append(prop2XML("contentOperator", "contains"));
  // else if (ele.parent_shell.equals(Attribute.CONTAINS_POSITIVE))
  // mql.append(prop2XML("contentOperator", "containsPositive"));
  // if (!ele.argument.isEmpty())
  // mql.append(prop2XML("desiredContent", ele.argument));
  // if (ele.mutipleValueSelector != ReductionOperator.Latest) {
  // mql.append(prop2XML("mutipleValueSelector", ele.mutipleValueSelector.toString()));
  // }
  // mql.append("/>");
  // return mql.toString();
  // }
  //
  //
  // private String prop2XML(String property, String value) {
  // return property + "=\"" + value + "\" ";
  // }
  //
  //
  // private String prop2XML(String property, int i) {
  // return prop2XML(property, i + "");
  // }
  //
  //
  // public Query parse(String mql) throws QueryException {
  // // if (!mql.contains("idFilterType=\"case\"")) {
  // // throw new
  // //
  // QueryException("Solr query can only return results based on same caseID and not on same PID
  // (use \"FROM CASE\"!).");
  // // }
  // Document doc = XMLUtil.getDoc(mql);
  // AndList andList = null;
  // String selectString = null;
  // doc.getDocumentElement().normalize();
  // NodeList childNodes = doc.getChildNodes().item(0).getChildNodes();
  // int caseidFilterCount = 0;
  // for (int i = 0; i <= childNodes.getLength() - 1; i++) {
  // Node node = childNodes.item(i);
  // String nodeName = node.getNodeName();
  // if (nodeName.equals(XML_IDFILTER_TAG)) {
  // andList = parseIDFilterList(node);
  // caseidFilterCount++;
  // if (caseidFilterCount > 1) {
  // throw new QueryException("only one caseID filter is allowed with Lucene queries");
  // }
  // } else if (nodeName.equals(XML_AND_TAG)) {
  // // although this should not be allowed in Lucene queries I allow it for compatibility
  // // reasons
  // andList = parseAndList(node);
  // } else if (nodeName.equals(XML_SELECTSTRING_TAG)) {
  // selectString = XMLUtil.getString(node, "value");
  // }
  // }
  // Query q = new Query(andList, selectString);
  // q.shrink();
  // System.out.println("---------");
  // System.out.println(toMQL(q));
  // System.out.println("---------");
  // return q;
  // }
  //
  //
  // private AndList parseAndList(Node list) throws QueryException {
  // NodeList childNodes = list.getChildNodes();
  // AndList andList = new AndList();
  // boolean optional = XMLUtil.getBoolean(list, "optional", false);
  // andList.setOptional(optional);
  // for (int i = 0; i <= childNodes.getLength() - 1; i++) {
  // QueryElement queryElement = parse(childNodes.item(i));
  // if (queryElement != null)
  // andList.add(queryElement);
  // }
  // return andList;
  // }
  //
  //
  // private QueryElement parse(Node node) throws QueryException {
  // String nodeName = node.getNodeName();
  // if (nodeName.equals(XML_ATTRIBUTE_TAG))
  // return parseAttribute(node);
  // else if (nodeName.equals(XML_AND_TAG))
  // return parseAndList(node);
  // else if (nodeName.equals(XML_OR_TAG))
  // return parseOrList(node);
  // else if (nodeName.equals("ID"))
  // return parseFilterID(node);
  // else if (nodeName.equals(XML_TEXT_TAG))
  // return null;
  // else
  // throw new IllegalArgumentException("Unsuported XML-Tag: " + nodeName);
  // }
  //
  //
  // private QueryElement parseFilterID(Node node) throws QueryException {
  // String ID = XMLUtil.getString(node, "value");
  // CatalogEntry anEntry = getCatalogEntry("CaseID", "MetaDaten");
  // Attribute attr = new Attribute(anEntry, "=", ID, null, -1, "", "");
  // return attr;
  // }
  //
  //
  // private void parseSelectString(Node node) {
  // }
  //
  //
  // private AndList parseIDFilterList(Node node) throws QueryException {
  // return parseAndList(node);
  // }
  //
  //
  // private QueryElement parseOrList(Node node) throws QueryException {
  // NodeList childNodes = node.getChildNodes();
  // OrList orList = new OrList();
  // boolean optional = XMLUtil.getBoolean(node, "optional", false);
  // orList.setOptional(optional);
  // for (int i = 0; i <= childNodes.getLength() - 1; i++) {
  // QueryElement queryElement = parse(childNodes.item(i));
  // if (queryElement != null)
  // orList.add(queryElement);
  // }
  // return orList;
  // }
  //
  //
  // private QueryElement parseAttribute(Node node) throws QueryException {
  // String extID = XMLUtil.getString(node, "extID");
  // String alias = XMLUtil.getString(node, "alias");
  // String domain = XMLUtil.getString(node, "domain");
  //
  // CatalogEntry anEntry = null;
  // if (!domain.equals("Anfragen")) {
  // anEntry = getCatalogEntry(extID, domain);
  // }
  //
  // String desiredContent = XMLUtil.getString(node, "desiredContent");
  // String parent_shell = XMLUtil.getString(node, "contentOperator");
  // parent_shell = mqlOperator2AttributeOperator(parent_shell);
  // String mutipleValueSelectorSimple = XMLUtil.getString(node, "mutipleValueSelector");
  // ReductionOperator mutipleValueSelector = ReductionOperator.None;
  // if (mutipleValueSelectorSimple != null) {
  // if (Attribute.reductionOpDictSimple2Enum.containsKey(mutipleValueSelectorSimple)) {
  // mutipleValueSelector = Attribute.reductionOpDictSimple2Enum.get(mutipleValueSelectorSimple);
  // } else {
  // mutipleValueSelector = ReductionOperator.valueOf(mutipleValueSelectorSimple);
  // }
  // }
  // boolean optional = XMLUtil.getBoolean(node, "optional", false);
  // if (!optional && parent_shell.isEmpty()) {
  // parent_shell = Attribute.EXITS;
  // }
  //
  // Attribute attr = new Attribute(anEntry, parent_shell, desiredContent, mutipleValueSelector, -1, "",
  // alias);
  // if (domain.equals("Anfragen")) {
  // String name = XMLUtil.getString(node, "name");
  // attr.rawQuery = allQueriesManager.getQuery(name);
  // }
  //
  // QueryElement queryElement = toQueryElement(attr, false);
  // queryElement.shrink();
  // return queryElement;
  // }
  //
  //
  // private CatalogEntry getCatalogEntry(String refID, String domain) throws QueryException {
  // CatalogEntry anEntry = null;
  // if ((refID != null) && (domain != null)) {
  // try {
  // anEntry = catalogManager.getEntryByRefID(refID, domain);
  // } catch (SQLException e) {
  // throw new QueryException(e);
  // }
  // } else {
  // String errorMsg = "Either a valid AttrID or a extID with domain has to be given.";
  // throw new QueryException(errorMsg);
  // }
  // return anEntry;
  // }
  //
  //
  // public QueryElement toQueryElement(Attribute attr, boolean expandSavedQueries) throws
  // QueryException {
  // if ((attr.rawQuery != null) && expandSavedQueries) {
  // Query query = parse(attr.rawQuery.xml);
  // AndList andList = query.getQueryElements();
  // andList.setOptional(attr.isOptional());
  // String position = attr.getPosition();
  // andList.propagatePosition(position);
  // return andList;
  // } else {
  // return attr;
  // }
  // }
  //
  //
  // public QueryElement toQueryElement(Attribute attr, boolean expandSavedQueries, String name)
  // throws QueryException {
  // if ((attr.rawQuery != null) && expandSavedQueries) {
  // Query query = parse(attr.rawQuery.xml);
  // AndList andList = query.getQueryElements();
  // andList.setName(name);
  // andList.setOptional(attr.isOptional());
  // String position = attr.getPosition();
  // andList.propagatePosition(position);
  // return andList;
  // } else {
  // return attr;
  // }
  // }
  //
  //
  // public AndList createAndList(List<List<Attribute>> orlists) {
  // AndList andList = new AndList();
  // for (List<Attribute> orList : orlists) {
  // if (orList.size() == 1) {
  // try {
  // QueryElement queryElement = toQueryElement(orList.get(0), true);
  // andList.add(queryElement);
  // } catch (QueryException e) {
  // e.printStackTrace();
  // }
  // } else
  // andList.add(new OrList(orList));
  // }
  // return andList;
  // }

}
