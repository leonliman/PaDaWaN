package de.uniwue.dw.query.model.quickSearch;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.manager.ICatalogAndTextSuggester;
import de.uniwue.dw.query.model.lang.QueryStructureElem;
import de.uniwue.dw.query.model.manager.IQueryClientIOManager;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchLexer;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser.ParseContext;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser.QueryAttributeOnlyContext;
import de.uniwue.dw.query.model.quickSearch.suggest.SuggestObject;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class QuickSearchAPI {

  public static final String SUCCCESORS = "Nachfolger";

  public static final String ALL_SUCCCESORS = "Alle";

  private static Logger logger = LogManager.getLogger(QuickSearchAPI.class);

  public static QueryStructureElem parse(QuickSearchLine line, User user,
          ICatalogAndTextSuggester suggester, IQueryClientIOManager queryClientIOManager) {
    QuickSearchLexer lexer = new QuickSearchLexer(CharStreams.fromString(line.getQueryText()));
    QuickSearchParser parser = new QuickSearchParser(new CommonTokenStream(lexer));
    ParseTree parseContext = parser.parse();
    // ParseTree parseContext = parse(input);
    logger.debug(parseContext.toStringTree(parser));
    QueryStructureElem elem = new QuickSearch2QueryRootVisitor(line.getQueryText(), user, suggester,
            queryClientIOManager).visit(parseContext);
    elem.setOptional(!line.isFilter());
    elem.setFilterUnkown(line.isFilterUnknown());
    elem.getAttributesRecursive().forEach(n -> n.setDisplayValue(line.isDisplayInResult()));
    return elem;
  }

  public static void parseAndPrintTree(String input) {
    ParseTree parseContext = parse(input);
    new QuickSearchPrintVisitor().visit(parseContext);
  }

  public static String parseToStringTree(String input) {
    QuickSearchLexer l = new QuickSearchLexer(CharStreams.fromString(input));
    CommonTokenStream tokenStream = new CommonTokenStream(l);
    QuickSearchParser parser = new QuickSearchParser(tokenStream);
    ParseTree tree = parser.parse();
    if (parser.getNumberOfSyntaxErrors() > 0) {
      printTokens(parser, tokenStream);
    }
    // parser.getBuildParseTree();
    return tree.toStringTree(parser);
  }

  public static String parseAndThrowExecptionOnError(String input) throws ParseException {
    QuickSearchLexer l = new QuickSearchLexer(CharStreams.fromString(input));
    CommonTokenStream tokenStream = new CommonTokenStream(l);
    QuickSearchParser parser = new QuickSearchParser(tokenStream);
    ParseTree tree = parser.parse();
    if (parser.getNumberOfSyntaxErrors() > 0) {
      printTokens(parser, tokenStream);
      throw new ParseException(input, 0);
    }
    parser.getBuildParseTree();
    return tree.toStringTree(parser);
  }

  public static int parseAndCheckErrors(String input) {
    QuickSearchLexer l = new QuickSearchLexer(CharStreams.fromString(input));
    CommonTokenStream tokenStream = new CommonTokenStream(l);
    QuickSearchParser parser = new QuickSearchParser(tokenStream);
    parser.parse();
    return parser.getNumberOfSyntaxErrors();
  }

  private static void printTokens(QuickSearchParser parser, CommonTokenStream tokenStream) {
    Vocabulary vocabulary = parser.getVocabulary();
    for (Token t : tokenStream.getTokens()) {
      String text = t.getText();
      String displayName = vocabulary.getDisplayName(t.getType());
      System.out.println(text + " " + displayName);
    }
  }

  private static ParseTree parse(String input) {
    QuickSearchLexer lexer = new QuickSearchLexer(CharStreams.fromString(input));
    QuickSearchParser parser = new QuickSearchParser(new CommonTokenStream(lexer));
    return parser.parse();
  }

  public static FormattingResult formatLine(String input, User user,
          ICatalogAndTextSuggester suggester, IQueryClientIOManager queryClientIOManager) {
    if (input == null || input.isEmpty())
      return new FormattingResult("");
    QuickSearchLexer lexer = new QuickSearchLexer(CharStreams.fromString(input));
    QuickSearchParser parser = new QuickSearchParser(new CommonTokenStream(lexer));
    QuickSearchSyntaxErrorListener errorListener = new QuickSearchSyntaxErrorListener();
    parser.removeErrorListeners();
    parser.addErrorListener(errorListener);
    ParseContext parse = parser.parse();
    boolean inputHasErrors = errorListener.getErrors().size() > 0;
    QuickSearchUniqueNameNotationFormatVisitor formatter = new QuickSearchUniqueNameNotationFormatVisitor(
            input, user, suggester, queryClientIOManager);
    String formattedText = formatter.visit(parse);
    if (inputHasErrors)
      formattedText = input;
    List<SyntaxError> errors = formatter.getErrors();
    addIfNotExistend(errors, errorListener.getErrors());
    return new FormattingResult(formattedText, errors);
  }

  private static void addIfNotExistend(List<SyntaxError> errors, List<SyntaxError> errorsToAdd) {
    for (SyntaxError toAdd : errorsToAdd) {
      boolean itExistsAnErrorAtTheSamePosition = errors.parallelStream()
              .anyMatch(n -> n.getPosition() == toAdd.getPosition());
      if (!itExistsAnErrorAtTheSamePosition) {
        errors.add(toAdd);
      }
    }
  }

  public static String getUnfinishedAttributeInput(String input) {
    QuickSearchLexer l = new QuickSearchLexer(CharStreams.fromString(input));
    QuickSearchParser parser = new QuickSearchParser(new CommonTokenStream(l));
    parser.removeErrorListeners();
    ParseTree tree = parser.input();
    // parser.getBuildParseTree();
    // System.out.println(tree.toStringTree(parser));
    QuickSearchSuggestVisitor visitor = new QuickSearchSuggestVisitor();
    visitor.visit(tree);
    return visitor.getText(input);
  }

  public static AttributeDuringTipping parseAttributeDuringTipping(String input,
          ICatalogAndTextSuggester suggester) {
    QuickSearchLexer l = new QuickSearchLexer(CharStreams.fromString(input));
    QuickSearchParser p = new QuickSearchParser(new CommonTokenStream(l));
    p.removeErrorListeners();
    QuickSearchFirstErrorListener errorListener = new QuickSearchFirstErrorListener();
    p.addErrorListener(errorListener);
    QueryAttributeOnlyContext tree = p.queryAttributeOnly();
    String unparsableText = errorListener.getUnparsableText(input);
    // p.getBuildParseTree();
    // System.out.println(tree.toStringTree(p));
    QuickSearchAttributeDuringTippingVisitor visitor = new QuickSearchAttributeDuringTippingVisitor(
            input, unparsableText);
    visitor.visit(tree);
    return visitor.getAttributeDuringTipping();
  }

  public static class QuickSearchFirstErrorListener extends BaseErrorListener {
    private int firstErrorIndex = Integer.MAX_VALUE;

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
            int charPositionInLine, String msg, RecognitionException e) {
      firstErrorIndex = Math.min(firstErrorIndex, charPositionInLine);
      // System.err.println("line " + line + ":" + charPositionInLine + " " + msg);
    }

    public String getUnparsableText(String input) {
      if (firstErrorIndex == Integer.MAX_VALUE)
        return "";
      return input.substring(firstErrorIndex);
    }
  }

  private static QuickSearchUniqueNameNotationFormatVisitor getQuickSearchUniqueNameNotationFormatVisitor(
          String input, User user, ICatalogAndTextSuggester suggester,
          IQueryClientIOManager queryClientIOManager) {
    QuickSearchLexer lexer = new QuickSearchLexer(CharStreams.fromString(input));
    QuickSearchParser parser = new QuickSearchParser(new CommonTokenStream(lexer));
    QuickSearchSyntaxErrorListener errorListener = new QuickSearchSyntaxErrorListener();
    parser.removeErrorListeners();
    parser.addErrorListener(errorListener);
    ParseContext parse = parser.parse();
    String stringTree = parse.toStringTree(parser);
    System.out.println(stringTree);
    boolean inputHasErrors = errorListener.getErrors().size() > 0;
    QuickSearchUniqueNameNotationFormatVisitor formatter = new QuickSearchUniqueNameNotationFormatVisitor(
            input, user, suggester, queryClientIOManager);
    String formattedText = formatter.visit(parse);
    if (inputHasErrors)
      formattedText = input;
    List<SyntaxError> errors = formatter.getErrors();
    addIfNotExistend(errors, errorListener.getErrors());

    return formatter;
  }

  private static List<SuggestObject> getQuickSearchToAttributes(String input, User user,
          ICatalogAndTextSuggester suggester, IQueryClientIOManager queryClientIOManager) {
    QuickSearchLexer lexer = new QuickSearchLexer(CharStreams.fromString(input));
    QuickSearchParser parser = new QuickSearchParser(new CommonTokenStream(lexer));
    ParseContext parse = parser.parse();
    QuickSearchToAttributesFormatVisitor formatter = new QuickSearchToAttributesFormatVisitor(input,
            user, suggester, queryClientIOManager);

    return formatter.visit(parse);
  }

  public static List<SuggestObject> getQueryEntriesTokenized(String input, User user,
          ICatalogAndTextSuggester suggester, IQueryClientIOManager queryClientIOManager) {
    ArrayList<SuggestObject> result = new ArrayList<>();
    if (input == null || input.isEmpty())
      return result;

    return getQuickSearchToAttributes(input, user, suggester,
            queryClientIOManager);
  }

  public static SuggestObject getQueryEntryTokenized(String input, User user,
          ICatalogAndTextSuggester suggester, IQueryClientIOManager queryClientIOManager) {
    if (input == null || input.isEmpty())
      return null;

    List<SuggestObject> suggestObjects = getQuickSearchToAttributes(input, user, suggester,
            queryClientIOManager);
    SuggestObject suggestObject = suggestObjects.get(0);
    suggestObject.setSuggestText(input);
    return suggestObject;
  }
}
