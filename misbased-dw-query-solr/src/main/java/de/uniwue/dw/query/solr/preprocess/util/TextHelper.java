package de.uniwue.dw.query.solr.preprocess.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

public class TextHelper {
  private int replacementCounter = 0;

  private Map<String, String> replacementMap = new HashMap<String, String>();

  private static final String[] NUM_VALUES = { "intVal_Alter", "doubleVal_Groesse",
      "intVal_Gewicht", "intVal_BMI", "intVal_HF", "stringVal_Blutdruck", "intVal_Fieber" };

  public void uploadDocument(long id, String text) throws SolrServerException, IOException {
    String url = "http://localhost:8983/solr";
 SolrClient   server = new HttpSolrClient.Builder(url).build();
    uploadDocument(id, text, server);
    server.commit();
  }

  public void uploadDocument(long id, String text, SolrClient server)
          throws SolrServerException, IOException {
    Pattern preNegTermPattern = Pattern.compile(getPreNegTermPattern(), Pattern.CASE_INSENSITIVE);
    Pattern postNegTermPattern = Pattern.compile(getPostNegTermPattern(), Pattern.CASE_INSENSITIVE);
    Pattern pseudoNegTermPattern = Pattern.compile(getPseudoNegTermPattern(),
            Pattern.CASE_INSENSITIVE);
    ArrayList<String> conjunctionTerms = getConjunctionTerms();
    String filename = "abbreviations.txt";
    String abbreviations = getWordsFromFile(filename, false);
    uploadDocument(id, text, server, preNegTermPattern, postNegTermPattern, pseudoNegTermPattern,
            conjunctionTerms, abbreviations);
  }

  public void uploadDocument(long id, String text, SolrClient server, Pattern preNegTermPattern,
          Pattern postNegTermPattern, Pattern pseudoNegTermPattern,
          ArrayList<String> conjunctionTerms, String abbreviations)
                  throws SolrServerException, IOException {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", id);
    text = text.replaceAll("\n", " ").replaceAll("\\s+", " ");
    doc.addField("content", text);
    text = prepareText(text, abbreviations);
    ArrayList<String> segments = getSegments(text, conjunctionTerms);
    List<List<String>> negInText = getNegInText(segments, preNegTermPattern, postNegTermPattern,
            pseudoNegTermPattern);
    for (String possegment : negInText.get(0)) {
      doc.addField("possegments", possegment);
    }
    for (String negsegment : negInText.get(1)) {
      doc.addField("negsegments", negsegment);
    }
    HashMap<String, String[]> fieldName2Values = getNumValueOccurrences(segments);
    for (String fieldName : fieldName2Values.keySet()) {
      for (String value : fieldName2Values.get(fieldName)) {
        doc.addField(fieldName, value);
      }
    }
    server.add(doc);
  }

  public HashMap<String, String[]> getNumValueOccurrences(String text) {
    ArrayList<String> segments = getSegments(text, getConjunctionTerms());
    return getNumValueOccurrences(segments);
  }

  public static HashMap<String, String[]> getNumValueOccurrences(ArrayList<String> segments) {
    HashMap<String, String[]> fieldName2Values = new HashMap<>();
    for (String numValue : NUM_VALUES) {
      String numValuePattern = getNumValuePattern(numValue);
      String[] results = getNumValues(segments, numValuePattern);
      if (results != null) {
        fieldName2Values.put(numValue, results);
      }
    }
    return fieldName2Values;
  }

  public static String getNumValuePattern(String numValue) {
    String result = "ThisStringDoesNotMatchAnything";
    if (numValue.equals("intVal_Alter")) {
      result = "(\\d{2})(\\s|-)?jährige(.)?\\sPatient(in)?##" + "(\\d{2})\\sJahre\\salt##"
              + "Alter\\s(\\d{2})\\sJahre##" + "1";
    } else if (numValue.equals("doubleVal_Groesse")) {
      result = "(\\d{1,3},?\\d{1,2})(\\s)?(m|cm)($|\\W)##1";
    } else if (numValue.equals("intVal_Gewicht")) {
      result = "(\\d{1,3})(\\s)?kg($|\\W)##1";
    } else if (numValue.equals("intVal_BMI")) {
      result = "BMI.{0,20}?(\\d{2})##1";
    } else if (numValue.equals("intVal_HF")) {
      result = "(HF.{0,20}?|Herzfrequenz.{0,20}?)?(\\d{2,3})(\\s)?/(\\s)?min##2";
    } else if (numValue.equals("stringVal_Blutdruck")) {
      result = "Blutdruck.{0,20}?(\\d{2,3})(\\s)?/(\\s)?(\\d{2,3})(\\s)?(mmHG)?##1,4";
    } else if (numValue.equals("intVal_Fieber")) {
      result = "(Fieber.{0,20}?|Temperatur.{0,20}?)?(\\d{2})(\\s)?(Grad|°)(\\s)?(Celsius|C)?##2";
    }
    return result;
  }

  private static String[] getNumValues(ArrayList<String> segments, String numValuePattern) {
    String resultString = "";
    String[] numValSubPatterns = numValuePattern.split("##");
    int groupNumber1 = 0;
    int groupNumber2 = 0;
    boolean checkBloodPressure = false;
    if (numValuePattern.startsWith("Blutdruck")) {
      String[] groupNumbers = numValSubPatterns[numValSubPatterns.length - 1].split(",");
      groupNumber1 = Integer.parseInt(groupNumbers[0]);
      groupNumber2 = Integer.parseInt(groupNumbers[1]);
      checkBloodPressure = true;
    } else {
      groupNumber1 = Integer.parseInt(numValSubPatterns[numValSubPatterns.length - 1]);
    }
    for (int i = 0; i < numValSubPatterns.length - 1; i++) {
      String numValSubPattern = numValSubPatterns[i];
      Pattern pattern = Pattern.compile(numValSubPattern);
      Iterator<String> segmentsIT = segments.iterator();
      while (segmentsIT.hasNext()) {
        String segment = segmentsIT.next();
        Matcher matcher = pattern.matcher(segment);
        while (matcher.find()) {
          if (checkBloodPressure) {
            String bloodPressureString = matcher.group(groupNumber1) + "/"
                    + matcher.group(groupNumber2);
            if (isValidBloodPressure(bloodPressureString)) {
              if (resultString.equals("")) {
                resultString += bloodPressureString;
              } else {
                resultString += "##" + bloodPressureString;
              }
            }
          } else {
            if (resultString.equals("")) {
              resultString += matcher.group(groupNumber1);
            } else {
              resultString += "##" + matcher.group(groupNumber1);
            }
          }
        }
      }
    }
    if (resultString.equals("")) {
      return null;
    } else {
      return resultString.replaceAll(",", ".").split("##");
    }
  }

  private static boolean isValidBloodPressure(String bloodPressureString) {
    String[] values = bloodPressureString.split("/");
    int firstValue = Integer.parseInt(values[0]);
    int secondValue = Integer.parseInt(values[1]);
    if (firstValue > secondValue) {
      return true;
    }
    return false;
  }

  // public static void main(String[] args) throws SQLException,
  // SolrServerException, IOException {
  // long start = System.currentTimeMillis();
  //
  // indexAllEchos();
  // // deleteIndex();
  //
  // long elapsedTime = System.currentTimeMillis() - start;
  // System.out.println("Zeit für Indexierung (in ms): " + elapsedTime);
  // }

  // public static void indexAllEchos() throws SQLException,
  // SolrServerException,
  // IOException {
  // String url = "http://localhost:8983/solr";
  // SolrServer server = new HttpSolrServer(url);
  //
  // Pattern preNegTermPattern = Pattern.compile(getPreNegTermPattern(),
  // Pattern.CASE_INSENSITIVE);
  // Pattern postNegTermPattern = Pattern.compile(getPostNegTermPattern(),
  // Pattern.CASE_INSENSITIVE);
  // Pattern pseudoNegTermPattern = Pattern.compile(getPseudoNegTermPattern(),
  // Pattern.CASE_INSENSITIVE);
  // ArrayList<String> conjunctionTerms = getConjunctionTerms();
  // String filename = "abbreviations.txt";
  // String abbreviations = getWordsFromFile(filename, false);
  //
  // MsSql db = new MsSql();
  // Statement stmt = db.getConnection().createStatement();
  // String sql = "select infoid, value from DWInfo where AttrID=8";
  // ResultSet rs = stmt.executeQuery(sql);
  // while (rs.next()) {
  // long infoid = rs.getLong("infoid");
  // String echo = rs.getString("value");
  // uploadDocument(infoid, echo, server, preNegTermPattern,
  // postNegTermPattern, pseudoNegTermPattern, conjunctionTerms,
  // abbreviations);
  // }
  // rs.close();
  // stmt.close();
  // db.closeConnection();
  // server.commit();
  // }

  public static void deleteIndex() throws SolrServerException, IOException {
    String url = "http://localhost:8983/solr";
    SolrClient   server = new HttpSolrClient.Builder(url).build();
    server.deleteByQuery("*:*");
    server.commit();
  }

  public List<List<String>> getNegInText(String text) {
    Pattern preNegTermPattern = Pattern.compile(getPreNegTermPattern(), Pattern.CASE_INSENSITIVE);
    Pattern postNegTermPattern = Pattern.compile(getPostNegTermPattern(), Pattern.CASE_INSENSITIVE);
    Pattern pseudoNegTermPattern = Pattern.compile(getPseudoNegTermPattern(),
            Pattern.CASE_INSENSITIVE);
    ArrayList<String> conjunctionTerms = getConjunctionTerms();

    String filename = "abbreviations.txt";
    String abbreviations = getWordsFromFile(filename, false);
    text = prepareText(text, abbreviations);
    ArrayList<String> segments = getSegments(text, conjunctionTerms);
    return getNegInText(segments, preNegTermPattern, postNegTermPattern, pseudoNegTermPattern);

  }

  public static List<List<String>> getNegInText(ArrayList<String> segments,
          Pattern preNegTermPattern, Pattern postNegTermPattern, Pattern pseudoNegTermPattern) {
    ArrayList<String> positiveSegments = new ArrayList<>();
    ArrayList<String> negativeSegments = new ArrayList<>();
    Iterator<String> it = segments.iterator();
    while (it.hasNext()) {
      String curSegment = it.next().trim();
      if (!curSegment.isEmpty()) {
        if (checkNeg(curSegment, preNegTermPattern, postNegTermPattern, pseudoNegTermPattern)) {
          negativeSegments.add(curSegment + "\n");
        } else {
          positiveSegments.add(curSegment + "\n");
        }
      }
    }
    ArrayList<List<String>> result = new ArrayList<>();
    result.add(positiveSegments);
    result.add(negativeSegments);
    return result;
  }

  private static String[] splitConjunctionTerms(String segment,
          ArrayList<String> conjunctionTerms) {
    int countConjunctions = 0;
    Iterator<String> itConjunctions = conjunctionTerms.iterator();
    while (itConjunctions.hasNext()) {
      String currConjunction = itConjunctions.next();
      countConjunctions += segment.split(currConjunction).length - 1;
    }
    String[] result = null;
    if (countConjunctions > 0) {
      result = new String[countConjunctions + 1];
      for (int i = 0; i < countConjunctions; i++) {
        itConjunctions = conjunctionTerms.iterator();
        while (itConjunctions.hasNext()) {
          String currConjunction = itConjunctions.next();
          int posOfConjunction = segment.indexOf(currConjunction);
          if (posOfConjunction != -1) {
            result[i] = segment.substring(0, posOfConjunction);
            segment = segment.substring(posOfConjunction);
            break;
          }
        }
      }
      result[countConjunctions] = segment;
    } else {
      result = new String[1];
      result[0] = segment;
    }
    return result;
  }

  private ArrayList<String> getSegments(String processedText, ArrayList<String> conjunctionTerms) {
    String resultString = "";
    String[] splitText = processedText.split("\\.|:|;");
    for (String segment : splitText) {
      String[] splitSegment = segment.split(",");
      for (String split : splitSegment) {
        for (String subsplit : splitConjunctionTerms(split, conjunctionTerms)) {
          resultString += subsplit + "#SplitBorder#";
        }
      }
    }
    Iterator<String> keySetIT = replacementMap.keySet().iterator();
    while (keySetIT.hasNext()) {
      String toReplace = keySetIT.next();
      String replaceWith = replacementMap.get(toReplace);
      resultString = resultString.replaceAll(toReplace, replaceWith);
    }
    ArrayList<String> result = new ArrayList<String>();
    for (String subsplit : resultString.split("#SplitBorder#")) {
      result.add(subsplit);
    }
    return result;
  }

  private String prepareText(String text, String abbreviations) {
    replacementCounter = 0;
    replacementMap = new TreeMap<String, String>();
    String processedText = text;
    Pattern pattern = Pattern.compile(abbreviations.replaceAll("\\.", "[.]"));
    Matcher matcher = pattern.matcher(processedText);
    while (matcher.find()) {
      String toReplace = matcher.group();
      String replaceWith = "#REPL#" + replacementCounter;
      replacementMap.put(replaceWith, toReplace);
      processedText = processedText.replaceFirst(toReplace.replaceAll("\\.", "[.]"), replaceWith);
      replacementCounter += 1;
    }
    Pattern pattern2 = Pattern.compile("\\d+([\\.|:|;|,]\\d+)+");
    Matcher matcher2 = pattern2.matcher(processedText);
    while (matcher2.find()) {
      String toReplace = matcher2.group();
      String replaceWith = "#REPL#" + replacementCounter;
      replacementMap.put(replaceWith, toReplace);
      processedText = processedText.replaceFirst(toReplace.replaceAll("\\.", "[.]"), replaceWith);
      replacementCounter += 1;
    }
    return processedText;
  }

  public static String getPostNegTermPattern() {
    String result = "";
    String filename = "postNegTerms.txt";
    result += ".*?(";
    result += getWordsFromFile(filename, true);
    result += ").*?";
    return result;
  }

  public static String getPreNegTermPattern() {
    String result = ".*?(";
    String filename = "preNegTerms.txt";
    result += getWordsFromFile(filename, true);
    result += ").*?";
    return result;
  }

  public static String getPseudoNegTermPattern() {
    String result = ".*?(";
    String filename = "pseudoNegTerms.txt";
    result += getWordsFromFile(filename, true);
    result += ").*?";
    return result;
  }

  public static void main(String[] args) throws IOException, URISyntaxException {
    // String path = "abbreviations.txt";
    // URL url;
    // url = TextIndexerHelper.class.getClassLoader().getResource(path);
    // System.out.println(url);
    // url = TextIndexerHelper.class.getResource(path);
    // System.out.println(url);
    // path="/"+path;
    // url = TextIndexerHelper.class.getClassLoader().getResource(path);
    // System.out.println(url);
    // url = TextIndexerHelper.class.getResource(path);
    // System.out.println(url);
    String wordsFromFile = getWordsFromFile("abbreviations.txt", true);
  }

  public static String getWordsFromFile(String filename, boolean canBePrefix) {
    String result = "";
    try {
      File file = new File(TextIndexerHelper.class.getResource("/" + filename).getPath());
      BufferedReader in = new BufferedReader(new FileReader(file));
      String zeile = null;
      while ((zeile = in.readLine()) != null) {
        if (zeile.length() > 1) {
          if (result.length() > 1) {
            result += "|";
          }
          if (canBePrefix) {
            result += zeile + "\\w+" + "|" + zeile;
          } else {
            result += zeile;
          }
        }
      }
      in.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    result += "";
    return result;
  }

  public static ArrayList<String> getConjunctionTerms() {
    ArrayList<String> conjunctionTerms = new ArrayList<String>();
    try {
      String filename = "conjunctions.txt";
      File file = new File(TextIndexerHelper.class.getResource("/" + filename).getPath());
      BufferedReader in = new BufferedReader(new FileReader(file));
      String zeile = null;
      while ((zeile = in.readLine()) != null) {
        zeile = zeile.toLowerCase();
        conjunctionTerms.add(zeile);
      }
      in.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return conjunctionTerms;
  }

  private static boolean checkNeg(String segment, Pattern preNegTermPattern,
          Pattern postNegTermPattern, Pattern pseudoNegTermPattern) {
    if (pseudoNegTermPattern.matcher(segment).matches()) {
      return false;
    }

    if (preNegTermPattern.matcher(segment).matches()) {
      return true;
    }

    if (postNegTermPattern.matcher(segment).matches()) {
      return true;
    }

    return false;
  }
}
