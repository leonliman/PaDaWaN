package de.uniwue.dw.query.model.quickSearch;

import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchBaseVisitor;
import de.uniwue.dw.query.model.quickSearch.parser.QuickSearchParser;

public class QuickSearchTextAttributeVisitor extends QuickSearchBaseVisitor<String> {

  private String input;

  private String catalogEntry;

  private String queryTokens;

  public QuickSearchTextAttributeVisitor(String input) {
    this.input = input;
  }

  @Override
  public String visitCatalogEntry(QuickSearchParser.CatalogEntryContext ctx) {
    this.catalogEntry = ctx.getText();
    return catalogEntry;
  }

  @Override
  public String visitQueryTokens(QuickSearchParser.QueryTokensContext ctx) {
    int start = ctx.getStart().getStartIndex();
    int stop = ctx.getStop().getStopIndex() + 1;
    queryTokens = input.substring(start, stop);
    return queryTokens;
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

  public String getCatalogEntry() {
    return catalogEntry;
  }

  public void setCatalogEntry(String catalogEntry) {
    this.catalogEntry = catalogEntry;
  }

  public String getQueryTokens() {
    return queryTokens;
  }

  public void setQueryTokens(String queryTokens) {
    this.queryTokens = queryTokens;
  }
  
  public boolean hasCatalogEntry(){
    return catalogEntry!=null;
  }

  public boolean hasQueryTokens() {
    return queryTokens != null;
  }
}
