package de.uniwue.dw.query.model.quickSearch.parser;

import static org.junit.Assert.*;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Test;

import de.uniwue.dw.query.model.quickSearch.parser.TextContentParser.RegexQueryContext;
import de.uniwue.dw.query.model.result.Highlighter;

public class RegexHighlighterTest {

  @Test
  public void testRegexOnly() {
    assertEquals("<match>Blutdruck 145</match>", eval("/Blutdruck ([0-9]+)/", "Blutdruck 145"));
    assertEquals("<match>Blutdruck 145</match>", eval("/Blutdruck ZAHL/", "Blutdruck 145"));
    assertEquals("<match>a/b</match>", eval("/a\\/b/", "a/b"));
    assertEquals("<match>Blutdruck 145/160</match>",
            eval("/Blutdruck ([0-9]+)\\/([0-9]+)/", "Blutdruck 145/160"));
    assertEquals("<match>Blutdruck 145/160</match>",
            eval("/Blutdruck ZAHL\\/ZAHL/", "Blutdruck 145/160"));
    assertEquals("Männlich, Alter <match>32</match> Jahre",
            eval("/ZAHL/", "Männlich, Alter 32 Jahre"));
  }

  @Test
  public void testOutputDefinition() {
    assertEquals("145", eval("/Blutdruck ([0-9]+)/$1", "Blutdruck 145"));
    assertEquals("145", eval("/Blutdruck ZAHL/ZAHL", "Blutdruck 145"));
    assertEquals("145", eval("/Blutdruck ZAHL/$1", "Blutdruck 145"));
    assertEquals("145", eval("/Blutdruck ([0-9]+)\\/([0-9]+)/$1", "Blutdruck 145/160"));
    assertEquals("160", eval("/Blutdruck ([0-9]+)\\/([0-9]+)/$2", "Blutdruck 145/160"));
    assertEquals("145", eval("/Blutdruck ZAHL\\/ZAHL/$1", "Blutdruck 145/160"));
    assertEquals("160", eval("/Blutdruck ZAHL\\/ZAHL/$2", "Blutdruck 145/160"));

    assertEquals("123", eval("/abc ZAHL ZAHL def/$1", "abc 123 456 def"));
    assertEquals("456", eval("/abc ZAHL ZAHL def/$2", "abc 123 456 def"));
    assertEquals("123", eval("/abc ZAHL ([0-9]+) def/$1", "abc 123 456 def"));
    assertEquals("456", eval("/abc ZAHL ([0-9]+) def/$2", "abc 123 456 def"));
    assertEquals("123", eval("/abc ([0-9]+) ([0-9]+) def/$1", "abc 123 456 def"));
    assertEquals("456", eval("/abc ([0-9]+) ([0-9]+) def/$2", "abc 123 456 def"));
    assertEquals("123", eval("/abc ([0-9]+) ZAHL def/$1", "abc 123 456 def"));
    assertEquals("456", eval("/abc ([0-9]+) ZAHL def/$2", "abc 123 456 def"));

    assertEquals("123-456", eval("/abc ([0-9]+) ([0-9]+) def/$1-$2", "abc 123 456 def"));
  }

  @Test
  public void testFilterAndOutputDefinition() {
    assertEquals("145", eval("/Blutdruck ([0-9]+)/[$1 > 130] $1", "Blutdruck 145"));
    assertEquals("145", eval("/Blutdruck ZAHL/[$1 > 130] ZAHL", "Blutdruck 145"));
    assertEquals("145", eval("/Blutdruck ZAHL/[ZAHL > 130] ZAHL", "Blutdruck 145"));
    assertEquals("145", eval("/Blutdruck ZAHL/[ZAHL > 130] $1", "Blutdruck 145"));
    assertEquals("145", eval("/Blutdruck ([0-9]+)\\/([0-9]+)/[$1 > 130]$1", "Blutdruck 145/160"));
    assertEquals("160", eval("/Blutdruck ([0-9]+)\\/([0-9]+)/[$1 > 130]$2", "Blutdruck 145/160"));
    assertEquals("145", eval("/Blutdruck ZAHL\\/ZAHL/[$1 > 130]$1", "Blutdruck 145/160"));
    assertEquals("160", eval("/Blutdruck ZAHL\\/ZAHL/[$1 > 130]$2", "Blutdruck 145/160"));

    assertEquals("123", eval("/abc ZAHL ZAHL def/[$1 > 100] $1", "abc 123 456 def"));
    assertEquals("123", eval("/abc ZAHL ZAHL def/[$2 > 100] $1", "abc 123 456 def"));
    assertEquals("456", eval("/abc ZAHL ZAHL def/[$1 > 100] $2", "abc 123 456 def"));
    assertEquals("456", eval("/abc ZAHL ZAHL def/[$2 > 100] $2", "abc 123 456 def"));
    assertEquals("123", eval("/abc ZAHL ZAHL def/[ZAHL > 100] $1", "abc 123 456 def"));
    assertEquals("123", eval("/abc ZAHL ZAHL def/[ZAHL > 100] ZAHL", "abc 123 456 def"));
    assertEquals("123", eval("/abc ZAHL ZAHL def/[$2 > 100] ZAHL", "abc 123 456 def"));

    assertEquals("123", eval("/abc ZAHL ([0-9]+) def/[$1 > 100]$1", "abc 123 456 def"));
    assertEquals("456", eval("/abc ZAHL ([0-9]+) def/[$1 > 100]$2", "abc 123 456 def"));
    assertEquals("123", eval("/abc ZAHL ([0-9]+) def/[$2 > 100]$1", "abc 123 456 def"));
    assertEquals("456", eval("/abc ZAHL ([0-9]+) def/[$2 > 100]$2", "abc 123 456 def"));
    assertEquals("123", eval("/abc ([0-9]+) ([0-9]+) def/[$1 > 100]$1", "abc 123 456 def"));
    assertEquals("456", eval("/abc ([0-9]+) ([0-9]+) def/[$1 > 100]$2", "abc 123 456 def"));
    assertEquals("123", eval("/abc ([0-9]+) ([0-9]+) def/[$2 > 100]$1", "abc 123 456 def"));
    assertEquals("456", eval("/abc ([0-9]+) ([0-9]+) def/[$2 > 100]$2", "abc 123 456 def"));
    assertEquals("123", eval("/abc ([0-9]+) ZAHL def/[$1 > 100]$1", "abc 123 456 def"));
    assertEquals("456", eval("/abc ([0-9]+) ZAHL def/[$1 > 100]$2", "abc 123 456 def"));
    assertEquals("123", eval("/abc ([0-9]+) ZAHL def/[$2 > 100]$1", "abc 123 456 def"));
    assertEquals("456", eval("/abc ([0-9]+) ZAHL def/[$2 > 100]$2", "abc 123 456 def"));

    assertEquals("123-456", eval("/abc ([0-9]+) ([0-9]+) def/$1-$2", "abc 123 456 def"));

    assertEquals("2015-12-24", eval("/ZAHL\\.ZAHL\\.ZAHL/$3-$2-$1", "geb am 24.12.2015 in"));
  }

  @Test
  public void testOutputDefinitionWithMultipleHits() {
    assertEquals("123, 456", eval("/abc ZAHL/$1", "abc 123 xyz abc 456"));
    assertEquals("123, 456, 789", eval("/abc ZAHL/$1", "abc 123 xyz abc 456 abc 789"));
    assertEquals("123, 456, 789", eval("/abc ZAHL/$1", "abc 123 xyz abc 456 bbbbbbbb abc 789"));
  }

  private static String eval(String query, String text) {
    TextContentLexer lexer = new TextContentLexer(new ANTLRInputStream(query));
    TextContentParser parser = new TextContentParser(new CommonTokenStream(lexer));
    RegexQueryContext ctx = parser.regexQuery();
    String stringTree = ctx.toStringTree(parser);
    System.out.println(stringTree);
    Highlighter highlighter = new Highlighter();
    String[] highlight = highlighter.highlight(ctx, text);
    return highlight[0];
  }

}
