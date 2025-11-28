package de.uniwue.dw.query.model.result;

import de.uniwue.dw.query.model.DWQueryConfig;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

import java.io.IOException;
import java.util.*;

public class NearQueryHighlighter {

  public static class NearQuery {
    public List<String> words;

    public int distance;

    public boolean isOrdered;

    public NearQuery(List<String> words, int distance, boolean isOrdered) {
      this.words = words;
      this.distance = distance;
      this.isOrdered = isOrdered;
    }
  }

  private static Analyzer analyzer = new StandardAnalyzer();

  public static String[] highlight(List<NearQuery> nearQueries, String text, String newValueShort, String newValue) {
    if (nearQueries != null) {
      text = cleanText(text);
      newValue = cleanText(newValue);
      for (NearQuery query : nearQueries) {
        String[] newValues = highlight(query, text, newValueShort, newValue);
        newValueShort = newValues[0];
        newValue = newValues[1];
      }
    }
    String[] newValues = { newValueShort, newValue };
    return newValues;
  }

  private static String cleanText(String text) {
    return text.replaceAll("<.+?>", " ");
  }

  private static String[] highlight(NearQuery query, String text, String newValueShort, String newValue) {
    List<String> searchWords = new ArrayList<>();
    for (String aToken : query.words) {
      searchWords.add(convertSearchTokenToRegex(aToken));
    }
    List<Hit> hits = computeMachtes(newValue, searchWords, query.distance, query.isOrdered);
    newValue = hightligt(newValue, hits);
    if (newValueShort == null || newValueShort.isEmpty())
      newValueShort = computeSnippet(text, hits);

    List<Hit> valueShortHits = computeMachtes(newValueShort, searchWords, query.distance, query.isOrdered);
    newValueShort = hightligt(newValueShort, valueShortHits);

    String[] newValues = { newValueShort, newValue };
    return newValues;
  }

  private static String convertSearchTokenToRegex(String searchToken) {
    String punctuationRegex = "[!\"#$%&'()+,-/:;<=>?@\\[\\\\\\]^_`{|}~]";
    String resultingSearchToken = searchToken.trim().toLowerCase().replaceAll(punctuationRegex, ".").replace("*", ".*");
    if (resultingSearchToken.substring(resultingSearchToken.length() - 1).matches(punctuationRegex)) {
      resultingSearchToken = resultingSearchToken.substring(0, resultingSearchToken.length() - 1) + ".?";
    }
    return resultingSearchToken;
  }

  private static String computeSnippet(String text, List<Hit> hits) {
    if (hits != null && !hits.isEmpty()) {
      Hit firstHit = hits.get(0);
      if (firstHit.start > 0) {
        return text.substring(Math.max(0, firstHit.start - (int) DWQueryConfig.queryHighlightFragSize() / 2),
                Math.min(text.length() - 1,
                        firstHit.start + firstHit.text.length() + (int) DWQueryConfig.queryHighlightFragSize() / 2));
      }
    }
    return null;
  }

  public static void main(String[] args) {
    testSpanQueryRegex();
  }

  private static void testSpanQueryRegex() {
    String text = "@@BEGIN@@    <b><u>Diagnosen:</u></b>    <b>Hyperkeratotisch-rhagadiformes Hand- und Fußekzem (L30)</b>    <b><u>Übernommene Diagnose: </u></b>    <b>Diabetes mellitus (tablettenpflichtig) </b>    <b><u>Diagnostik und relevante Befunde:</u></b>    <b>Labor: </b><i>@@DATE@@: </i>Cholesterin: 221 [130 - 220] mg/dl; Triglyceride: 201 [74 - 172] mg/dl (nicht nüchtern). Differential-blutbild, Leber- und Nierenwerte: Unauffällig.    <b>Mykologie: </b><i>Hautschuppen von plantar und palmar (@@DATE@@): </i>Mikroskopie negativ, kein Wachstum in der Kultur.    <b>Schilddrüse: </b>TSH und FT4: Normwertig.    <b><u>Therapie und Verlauf:</u></b> Von einer systemischen Therapie mit z. B. Toctino® nehmen wir auf Grund der Nebenwirkungen, insbesondere auf die Leberfunktion, zum jetzigen Zeitpunkt Abstand. Therapie der Hände und Füße mit Fußbad 2 x täglich, danach sofortiges Eincremen mit Carbamid Widmer Creme. Mehrmals täglich Rückfettung der Hände und Füße mit z. B. DAC Basiscreme. Auf die Hyperkeratosen Salizylvaseline 5% auf Hände und Füße 1x täglich über Nacht. Bei Feuchtarbeiten konsequentes Tragen von Handschuhen, darunter Baumwollhandschuhe. Eine Wiedervorstellung zur Kontrolle ist in 6 Wochen vorgesehen.   @@END@@";
    String[] tokens = { "Diabetes", "Mellitus", "diagnose" };
    List<String> tokenList = new ArrayList<>(Arrays.asList(tokens));
    boolean isOrderd = false;
    int distance = 7;
    NearQuery nearQuery = new NearQuery(tokenList, distance, isOrderd);
    List<NearQuery> nearQueries = new ArrayList<>();
    nearQueries.add(nearQuery);
    String[] highlight = highlight(nearQueries, text, null, text);
    System.out.println(highlight[0]);
    System.out.println(highlight[1]);

  }

  private static String hightligt(String text, List<Hit> hits) {
    if (text == null || text.isEmpty() || hits == null)
      return null;
    int position = 0;
    StringBuffer sb = new StringBuffer();
    for (Hit hit : hits) {
      if (hit.start <= text.length() - 1) {
        sb.append(text.substring(position, hit.start));

        String hitText = text.substring(hit.start, hit.end);
        String highlitedHit = DWQueryConfig.queryHighlightPre() + hitText + DWQueryConfig.queryHighlightPost();
        sb.append(highlitedHit);
        position = hit.end;
      }
    }
    if (position <= text.length() - 1)
      sb.append(text.substring(position, text.length() - 1));
    return sb.toString();

  }

  private static class Hit {
    String text;

    int start, end;

    public Hit(String text, int start, int end) {
      this.text = text;
      this.start = start;
      this.end = end;
    }

    @Override
    public String toString() {
      return text + " - " + start + " - " + end;
    }

    @Override
    public int hashCode() {
      return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Hit other = (Hit) obj;
      return other.toString().equals(toString());
    }

  }

  private static class HitWithTokenNumAndSearchTerm extends Hit {
    int tokenNum;

    String searchTerm;

    public HitWithTokenNumAndSearchTerm(String text, int start, int end, int tokenNum, String searchTerm) {
      super(text, start, end);
      this.tokenNum = tokenNum;
      this.searchTerm = searchTerm;
    }

    @Override
    public String toString() {
      return super.toString() + " - " + tokenNum + " - " + searchTerm;
    }

    @Override
    public int hashCode() {
      return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Hit other = (Hit) obj;
      return other.toString().equals(toString());
    }

  }

  private static List<Hit> computeMachtes(String text, List<String> searchWords, int distance, boolean ordered) {
    if (text == null || searchWords == null)
      return null;
    Map<String, List<HitWithTokenNumAndSearchTerm>> tokenizedText = tokenizeText(text, searchWords);
    if (tokenizedText.keySet().size() != searchWords.size())
      return new ArrayList<>();
    if (ordered) {
      return getOrderedHits(searchWords, distance, tokenizedText);
    } else {
      return getUnorderedHits(searchWords, distance, tokenizedText);
    }
  }

  private static List<Hit> getUnorderedHits(List<String> searchWords, int distance,
          Map<String, List<HitWithTokenNumAndSearchTerm>> tokenizedText) {
    Set<Hit> hitSet = new HashSet<>();
    int numOfSearchWords = tokenizedText.size();
    List<HitWithTokenNumAndSearchTerm> allHitsList = new ArrayList<>();
    for (String aSearchWord : tokenizedText.keySet()) {
      for (HitWithTokenNumAndSearchTerm aHit : tokenizedText.get(aSearchWord)) {
        allHitsList.add(aHit);
      }
    }
    sortHitList(allHitsList);
    for (int i = 0; (i + numOfSearchWords) <= allHitsList.size(); i++) {
      List<HitWithTokenNumAndSearchTerm> curWindow = getWindow(allHitsList, i, distance);
      if (checkWindowMatchesSearch(curWindow, numOfSearchWords)) {
        hitSet.addAll(curWindow);
      }
    }
    return hitSetToOrderedList(hitSet);
  }

  private static List<HitWithTokenNumAndSearchTerm> getWindow(List<HitWithTokenNumAndSearchTerm> allHitsList,
          int iStart, int distance) {
    List<HitWithTokenNumAndSearchTerm> window = new ArrayList<>();
    HitWithTokenNumAndSearchTerm windowStart = allHitsList.get(iStart);
    window.add(windowStart);
    for (int i = (iStart + 1); i < allHitsList.size(); i++) {
      HitWithTokenNumAndSearchTerm curHit = allHitsList.get(i);
      if (windowStart.tokenNum + distance >= curHit.tokenNum)
        window.add(curHit);
      else
        break;
    }
    return window;
  }

  private static boolean checkWindowMatchesSearch(List<HitWithTokenNumAndSearchTerm> window, int numOfSearchWords) {
    Set<String> searchWordsInWindow = new HashSet<>();
    for (HitWithTokenNumAndSearchTerm aHit : window) {
      searchWordsInWindow.add(aHit.searchTerm);
    }
    return searchWordsInWindow.size() == numOfSearchWords;
  }

  private static List<Hit> hitSetToOrderedList(Set<Hit> hitSet) {
    List<Hit> hitList = new ArrayList<>();
    hitList.addAll(hitSet);
    sortHitList(hitList);
    return hitList;
  }

  private static void sortHitList(List<? extends Hit> hitList) {
    Collections.sort(hitList, new Comparator<Hit>() {
      @Override
      public int compare(Hit hit1, Hit hit2) {
        return hit1.start - hit2.start;
      }
    });
  }

  private static List<Hit> getOrderedHits(List<String> searchWords, int distance,
          Map<String, List<HitWithTokenNumAndSearchTerm>> tokenizedText) {
    Set<Hit> hitSet = new HashSet<>();
    for (HitWithTokenNumAndSearchTerm aFirstHit : tokenizedText.get(searchWords.get(0))) {
      for (HitWithTokenNumAndSearchTerm aLastHit : tokenizedText.get(searchWords.get(searchWords.size() - 1))) {
        if ((aFirstHit.tokenNum + distance) < aLastHit.tokenNum)
          continue;
        if (searchWords.size() < 3) {
          hitSet.add(aFirstHit);
          hitSet.add(aLastHit);
        } else {
          List<List<HitWithTokenNumAndSearchTerm>> intermediateHits = new ArrayList<>();
          for (int i = 1; i < searchWords.size() - 1; i++) {
            List<HitWithTokenNumAndSearchTerm> curIntermediateHits = new ArrayList<>();
            for (HitWithTokenNumAndSearchTerm anIntermediateHit : tokenizedText.get(searchWords.get(i))) {
              if (anIntermediateHit.tokenNum > aFirstHit.tokenNum && anIntermediateHit.tokenNum < aLastHit.tokenNum) {
                curIntermediateHits.add(anIntermediateHit);
              }
            }
            if (!curIntermediateHits.isEmpty()) {
              intermediateHits.add(curIntermediateHits);
            }
          }
          if (intermediateHits.size() == searchWords.size() - 2) {
            hitSet.add(aFirstHit);
            for (List<HitWithTokenNumAndSearchTerm> curIntermediateHits : intermediateHits) {
              for (HitWithTokenNumAndSearchTerm aIntermediateHit : curIntermediateHits) {
                hitSet.add(aIntermediateHit);
              }
            }
            hitSet.add(aLastHit);
          }
        }
      }
    }
    return hitSetToOrderedList(hitSet);
  }

  private static Map<String, List<HitWithTokenNumAndSearchTerm>> tokenizeText(String text, List<String> searchWords) {
    try {
      Map<String, List<HitWithTokenNumAndSearchTerm>> resultMap = new HashMap<>();

      TokenStream tokenStream = analyzer.tokenStream(null, text);
      OffsetAttribute offsetAttribute = tokenStream.addAttribute(OffsetAttribute.class);
      CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);

      tokenStream.reset();
      int tokenNum = 0;
      while (tokenStream.incrementToken()) {
        int startOffset = offsetAttribute.startOffset();
        int endOffset = offsetAttribute.endOffset();
        String term = charTermAttribute.toString();
        String matchingSearchWord = getMatchingSearchWordForTerm(term, searchWords);
        if (matchingSearchWord != null) {
          if (!resultMap.containsKey(matchingSearchWord)) {
            resultMap.put(matchingSearchWord, new ArrayList<>());
          }
          resultMap.get(matchingSearchWord)
                  .add(new HitWithTokenNumAndSearchTerm(term, startOffset, endOffset, tokenNum, matchingSearchWord));
        }
        tokenNum++;
      }

      tokenStream.close();

      return resultMap;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  private static String getMatchingSearchWordForTerm(String term, List<String> searchWords) {
    for (String aSearchWord : searchWords) {
      if (term.matches(aSearchWord)) {
        return aSearchWord;
      }
    }
    return null;
  }

}
