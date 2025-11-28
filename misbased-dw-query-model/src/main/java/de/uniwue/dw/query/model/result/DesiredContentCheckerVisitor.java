package de.uniwue.dw.query.model.result;

import de.uniwue.dw.query.model.quickSearch.parser.TextContentBaseVisitor;
import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser;

public class DesiredContentCheckerVisitor extends TextContentBaseVisitor<Void> {

  public boolean nearExists = false;

  public boolean regexExists = false;

  @Override
  public Void visitNear(TextContentParser.NearContext ctx) {
    nearExists = true;
    return super.visitNear(ctx);
  }

  @Override
  public Void visitRegex(TextContentParser.RegexContext ctx) {
    regexExists = true;
    return super.visitRegex(ctx);
  }
}
