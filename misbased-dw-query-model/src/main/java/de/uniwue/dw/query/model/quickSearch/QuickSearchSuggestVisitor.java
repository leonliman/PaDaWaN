package de.uniwue.dw.query.model.quickSearch;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNodeImpl;

import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchBaseVisitor;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser;

public class QuickSearchSuggestVisitor extends QuickSearchBaseVisitor<String> {

  private int start = -1;

  @SuppressWarnings("unused")
  private int stop = -1;

  public String getText(String input) {
    if (start >= 0)
      return input.substring(start, input.length());
    else
      return "";
  }

  @Override
  public String visitText(QuickSearchParser.TextContext ctx) {
    List<String> childs = new ArrayList<>();
    Token startToken = ctx.getStart();
    if (startToken != null)
      start = startToken.getCharPositionInLine();
    Token stopToken = ctx.getStop();
    if (stopToken != null)
      stop = stopToken.getCharPositionInLine();
    for (int i = 0; i <= ctx.getChildCount() - 1; i++) {
      ParseTree child = ctx.getChild(i);
      childs.add(child.getText());
      if (child instanceof TerminalNodeImpl) {
        stop = ((TerminalNodeImpl) child).getSymbol().getStopIndex();
      }
    }
    return childs.stream().collect(Collectors.joining(" "));
  }

  @Override
  protected String aggregateResult(String aggregate, String nextResult) {
    if (aggregate == null && nextResult == null)
      return null;
    if (aggregate != null && nextResult == null)
      return aggregate;
    else if (aggregate == null && nextResult != null)
      return nextResult;
    else {
      return aggregate + " " + nextResult;
    }
  }
}
