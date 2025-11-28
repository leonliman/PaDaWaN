package de.uniwue.dw.exchangeFormat;

import de.uniwue.dw.exchangeFormat.grammar.MultipleValuesBaseListener;
import de.uniwue.dw.exchangeFormat.grammar.MultipleValuesLexer;
import de.uniwue.dw.exchangeFormat.grammar.MultipleValuesParser;
import de.uniwue.dw.exchangeFormat.grammar.MultipleValuesParser.MetaDataContext;
import de.uniwue.dw.exchangeFormat.grammar.MultipleValuesParser.ValueContext;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.ArrayList;
import java.util.List;

public interface EFMultipleValuesFormat {

  public static String valueSeparator = "<##>";

  public default List<EFSingleValue> getValuesFromString(String valuesString) {
    MultipleValuesLexer lexer = new MultipleValuesLexer(CharStreams.fromString(valuesString));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    MultipleValuesParser parser = new MultipleValuesParser(tokens);
    ParseTree tree = parser.field();
    ParseTreeWalker walker = new ParseTreeWalker();
    MultipleValuesWalker valuesWalker = new MultipleValuesWalker();
    try {
      walker.walk(valuesWalker, tree);
    } catch (StackOverflowError e) {
      System.err.println("Stackoverflow for: " + valuesString);
    }
    return valuesWalker.getFoundValues();
  }

  public class MultipleValuesWalker extends MultipleValuesBaseListener {

    private List<EFSingleValue> foundValues;

    public MultipleValuesWalker() {
      this.foundValues = new ArrayList<EFSingleValue>();
    }

    @Override
    public void enterValue(ValueContext ctx) {
      foundValues.add(new EFSingleValue(ctx.getText().trim()));
    }

    @Override
    public void enterMetaData(MetaDataContext ctx) {
      foundValues.get(foundValues.size() - 1).addMetaData(ctx.getText().trim());
    }

    public List<EFSingleValue> getFoundValues() {
      return foundValues;
    }

  }

  public class EFSingleValue {

    private static String metaDataSeparator = "<###>";

    private String value;

    private List<String> metaData;

    public EFSingleValue(String value) {
      this.value = value;
      this.metaData = new ArrayList<String>();
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }

    public List<String> getMetaData() {
      return metaData;
    }

    public String getFirstMetaData() {
      if (!metaData.isEmpty()) {
        return metaData.get(0);
      } else {
        return null;
      }
    }

    public void addMetaData(String... metaData) {
      for (String singleMetaData : metaData) {
        this.metaData.add(singleMetaData);
      }
    }

    @Override
    public String toString() {
      StringBuilder valueStringBuilder = new StringBuilder();
      valueStringBuilder.append(getValue());
      for (String metaDataString : getMetaData()) {
        valueStringBuilder.append(metaDataSeparator);
        valueStringBuilder.append(metaDataString);
      }
      return valueStringBuilder.toString();
    }
  }

  public default String buildStringFromEFValue(EFSingleValue value) {
    List<EFSingleValue> singleValueList = new ArrayList<EFSingleValue>();
    singleValueList.add(value);
    return buildStringFromEFValues(singleValueList);
  }

  public default String buildStringFromEFValues(List<EFSingleValue> values) {
    StringBuilder valuesStringBuilder = new StringBuilder();
    if (!values.isEmpty()) {
      valuesStringBuilder.append(values.get(0).toString());
      for (int i = 1; i < values.size(); i++) {
        valuesStringBuilder.append(valueSeparator);
        valuesStringBuilder.append(values.get(i).toString());
      }
    }
    return valuesStringBuilder.toString();
  }

}
