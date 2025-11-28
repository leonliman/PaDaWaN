package de.uniwue.dw.query.model.tests;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import de.uniwue.dw.core.model.manager.ICatalogClientManager;
import de.uniwue.dw.query.model.QueryReader;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.misc.util.Configuration;

public class QueryShrinkTestLoader {

  public static final String QUERY_FOLDER = "queries2shrink/";

  public static final String DELIMITER_IDENTIFIEIR = "---";

  public static final String QUERY_IDENTIFIER = "---input---";

  public static final String RESULT_IDENTIFIER = "---expected---";

  private ICatalogClientManager catalogClientManager;

  private QueryRoot input;

  private QueryRoot expected;

  public QueryShrinkTestLoader(String filename, ICatalogClientManager catalogClientManager)
          throws QueryException, IOException {
    this.catalogClientManager = catalogClientManager;
    analyse(filename);
  }

  private void analyse(String filename) throws IOException, QueryException {
    filename = "classpath:"  + filename;
    List<String> lines = loadFile(filename);
    String inputXML = getQueryXML(lines);
    String expectedXML = getResultString(lines);
    input = QueryReader.read(catalogClientManager, inputXML);
    expected = QueryReader.read(catalogClientManager, expectedXML);
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
        if (line.toLowerCase().contains(queryIdentifier.toLowerCase())) {
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
    return section.stream().collect(Collectors.joining("\n")).trim();
  }

  public List<String> loadFile(String path) throws IOException {
    System.out.println(path);
    Resource resource = getResourceParameter(path);
    // File file = new File(getClass().getClassLoader().getResource(path).getFile());
    InputStream inputStream = resource.getInputStream();
    Scanner s = new Scanner(inputStream);
    List<String> lines = new ArrayList<>();
    while (s.hasNext()) {
      lines.add(s.nextLine());
    }
    s.close();
    // List<String> lines = Files.lines(file.toPath()).collect(Collectors.toList());
    return lines;
  }

  public Resource getResourceParameter(String key) throws IOException {
    // http://docs.spring.io/spring/docs/current/spring-framework-reference/html/resources.html
    if (!(key.startsWith("file:") || key.startsWith("http:") || key.startsWith("classpath:")
            || key.startsWith("url:")))
      key = "file:" + key;
    Resource resource = getResource(key);
    return resource;
  }

  public static Resource[] getResources(String path) throws IOException {
    final PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver(
            Configuration.class.getClassLoader());
    final Resource[] resources = resolver.getResources(path);
    return resources;
  }

  public static Resource getResource(String path) throws IOException {
    Resource[] resources = getResources(path);
    if (resources.length == 1)
      return resources[0];
    return null;
  }

  public QueryRoot getInput() {
    return input;
  }

  public QueryRoot getExpected() {
    return this.expected;
  }

}
