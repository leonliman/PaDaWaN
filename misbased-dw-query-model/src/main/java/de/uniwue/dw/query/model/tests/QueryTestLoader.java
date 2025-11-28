package de.uniwue.dw.query.model.tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import de.uniwue.dw.core.model.manager.ICatalogClientManager;
import de.uniwue.dw.query.model.QueryReader;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.result.Result;
import de.uniwue.dw.query.model.result.ResultReader;
import de.uniwue.misc.util.ResourceUtil;

public class QueryTestLoader {

  public static final String QUERY_FOLDER = "queries/";

  public static final String DELIMITER_IDENTIFIEIR = "---";

  public static final String QUERY_IDENTIFIER = "---Query---";

  public static final String RESULT_IDENTIFIER = "---Result---";

  private static final String RESULT_CSV_DELIMITER = ";";

  private static final String RESULT_CSV_DELIMITER2 = "\t";

  private ICatalogClientManager catalogClientManager;

  public QueryTestLoader(ICatalogClientManager catalogClientManager) {
    this.catalogClientManager = catalogClientManager;
  }

  public QueryTest readTest(String filename) throws IOException, QueryException {
    filename = "classpath:" + filename;
    List<String> lines = ResourceUtil.loadFile(filename);
    String queryXML = getQueryXML(lines);
    String resultString = getResultString(lines);
    QueryRoot root = QueryReader.read(catalogClientManager, queryXML);
    Result result;
    // the in query used delimiter will be used
    if (resultString.contains(RESULT_CSV_DELIMITER2)) {
      result = ResultReader.readCSV(resultString, RESULT_CSV_DELIMITER2);
    } else {
      result = ResultReader.readCSV(resultString, RESULT_CSV_DELIMITER);
    }
    QueryTest queryTest = new QueryTest(filename, root, result);
    return queryTest;
  }

  private String getResultString(List<String> lines) {
    return getSection(lines, RESULT_IDENTIFIER);
  }

  private String getQueryXML(List<String> lines) {
    return getSection(lines, QUERY_IDENTIFIER);
  }

  public String getSection(List<String> lines, String queryIdentifier) {
    List<String> section = new ArrayList<>();
    boolean cursorIsInSection = false;

    for (String line : lines) {
      if (line.startsWith(DELIMITER_IDENTIFIEIR)) {
        if (line.contains(queryIdentifier)) {
          cursorIsInSection = true;
        } else {
          if (cursorIsInSection)
            break;
        }
      } else {
        if (cursorIsInSection) {
          section.add(line);
        }
      }
    }
    String result = section.stream().collect(Collectors.joining("\n"));
    // do not trim the result as there may be potencial tabs at the end of the last line
    result = result.replaceAll("\n*$", "").replaceAll("^\n*", "");
    return result;
  }

}
