package de.uniwue.dw.query.model.quickSearch;

import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

public class QuickSearchSyntaxErrorListener extends BaseErrorListener {
  private List<SyntaxError> errors = new ArrayList<>();

  public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
          int charPositionInLine, String msg, RecognitionException e) {
    String message = createMessageText(msg);
    SyntaxError syntaxError = new SyntaxError(charPositionInLine, message);
    errors.add(syntaxError);
  }

  private String createMessageText(String msg) {
    String result = msg;
    if (msg.startsWith("missing")) {
      result = result.replace("missing ", "Es fehlt ");
      result = result.replace(" at ", " bei ");
      result = result.replace("'<EOF>'", "dem Ende der Zeile");
    } else if (msg.startsWith("extraneous input")) {
      result = result.replace("extraneous input", "Unpassender Eingabe.");
      result = result.replace(" expecting <EOF>", "");
      result = result.replace(" expecting ", " es würde passen: ");
    } else if (msg.startsWith("mismatched input")) {
      result = result.replace("mismatched input", "Unpassender Eingabe.");
      result = result.replace(" '<EOF>' ", " ");
      result = result.replace(" expecting ", " Es würde passen: ");
    } else {
      System.err.println(msg);
      result = "Die Eingabe hat eine fehlerhafte Syntax.";
    }
    return result;
  }

  public List<SyntaxError> getErrors() {
    return errors;
  }

}
