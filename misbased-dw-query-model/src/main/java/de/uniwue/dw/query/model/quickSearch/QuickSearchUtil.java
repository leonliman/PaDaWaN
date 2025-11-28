package de.uniwue.dw.query.model.quickSearch;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.manager.DataSourceException;
import de.uniwue.dw.core.model.manager.ICatalogAndTextSuggester;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchLexer;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser;

public class QuickSearchUtil {

  private static final String ILLEGAL_CATALOG_NAME_CHARS_REGEX = ".*?[ <>=()].*?";

  private static final Pattern ILLEGAL_CATALOG_NAME_CHARS = Pattern
          .compile(ILLEGAL_CATALOG_NAME_CHARS_REGEX);

  public static boolean catalogNameMustBeFormatted(String name) {
    return ILLEGAL_CATALOG_NAME_CHARS.matcher(name).matches();
  }

  public static String formatCatalogEntryName(String name) {
    if (catalogNameMustBeFormatted(name)) {
      name = "'" + name + "'";
    }
    return name;
  }

  public static boolean isOneQueryToken(String input) {
    if (input == null || input.isEmpty())
      return false;
    QuickSearchLexer lexer = new QuickSearchLexer(new ANTLRInputStream(input));
    QuickSearchParser parser = new QuickSearchParser(new CommonTokenStream(lexer));
    parser.queryAttributeOnly();
    int errors = parser.getNumberOfSyntaxErrors();
    System.out.println(errors);
    return errors == 0;
  }

  public static String deformatCatalogEntryName(String name) {
    if (name.startsWith("'") && name.endsWith("'")) {
      name = name.substring(1, name.length() - 1);
    }
    return name;
  }

  public static String formatCatalogEntryWithAtNotation(CatalogEntry entry) {
    if (entry == null)
      return "";
    String name = formatCatalogEntryName(entry.getName());
    String domain = entry.getProject();
    return name + "@" + domain;
  }

  public static String formatCatalogEntryWithUniquiNameNotation(CatalogEntry entry) {
    if (entry == null)
      return "";
    String name = entry.getUniqueName();
    // name=entry.getName();
    return name;
  }

  // public static CatalogEntry parseEntryByNameWithatNotation(String name, User user,
  // ICatalogAndTextSuggester suggester) {
  // System.out.println("parseEntryByName(" + name + ")");
  // QuickSearchLexer lexer = new QuickSearchLexer(new ANTLRInputStream(name));
  // QuickSearchParser parser = new QuickSearchParser(new CommonTokenStream(lexer));
  // ParseTree tree = parser.queryAttribute();
  // parser.getBuildParseTree();
  // System.out.println(tree.toStringTree(parser));
  // QuickSearchAttributeParserVisitor visitor = new QuickSearchAttributeParserVisitor();
  // visitor.visit(tree);
  // String entryName = visitor.getName();
  // entryName = deformatCatalogEntryName(entryName);
  // String domain = visitor.getDomain();
  // if (domain != null)
  // domain += "*";
  // System.out.println("'" + entryName + "'@" + domain);
  // if (entryName == null)
  // entryName = name;
  // CatalogEntry catalogEntry = suggester.getCatalogEntryByName(entryName, domain, user);
  // System.out.println(catalogEntry);
  // return catalogEntry;
  // }
  //
  public static CatalogEntry parseEntryByName(String name, User user,
          ICatalogAndTextSuggester suggester) {
    CatalogEntry catalogEntry;
    try {
      catalogEntry = suggester.getCatalogEntryByNameOrUniqueName(name, user);
      return catalogEntry;
    } catch (DataSourceException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static String formatTextQueryTerms(String content) {
    content = content.trim();
    if (content == null)
      return "";
    String[] keyWords = { "and", "und", "&&", "or", "oder", "||", "<", "<=", ">", ">=", "=", "$",
        "[" };
    String lowerCase = content.toLowerCase();
    if (Arrays.asList(keyWords).parallelStream().filter(n -> lowerCase.contains(n)).findAny()
            .isPresent())
      return "'" + content + "'";
    return content;
  }
}
