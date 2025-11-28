package de.uniwue.misc.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.io.IOUtils;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

public class ResourceUtil {

  public static List<String> loadFile(String path) throws IOException {
    Resource resource = getResourceParameter(path);
    InputStream inputStream = resource.getInputStream();
    Scanner s = new Scanner(inputStream, "UTF-8");
    List<String> lines = new ArrayList<>();
    while (s.hasNext()) {
      lines.add(s.nextLine());
    }
    s.close();
    return lines;
  }

  public static String loadFileAsString(String path) throws IOException {
    Resource resource = getResourceParameter(path);
    InputStream inputStream = resource.getInputStream();
    StringWriter writer = new StringWriter();
    IOUtils.copy(inputStream, writer, "UTF-8");
    String result = writer.toString();
    return result;
  }

  public static Resource getResourceParameter(String key) throws IOException {
    // http://docs.spring.io/spring/docs/current/spring-framework-reference/html/resources.html
    if (!(key.startsWith("file:") || key.startsWith("http:") || key.startsWith("classpath:")
            || key.startsWith("url:"))) {
      key = "file:" + key;
    }
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

}
