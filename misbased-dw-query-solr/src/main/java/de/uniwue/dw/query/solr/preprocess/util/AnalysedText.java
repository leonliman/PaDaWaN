package de.uniwue.dw.query.solr.preprocess.util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class AnalysedText {

  private List<Segment> segments = new ArrayList<>();

  public List<Segment> getSegments() {
    return segments;
  }

  public void add(Segment s) {
    segments.add(s);
  }

  public void setSegments(List<Segment> segments) {
    this.segments = segments;
  }

  public List<Segment> getNegatedSegments() {
    return segments.stream().filter(Segment::isNegated).collect(Collectors.toList());
  }

  public List<Segment> getNotNegatedSegments() {
    return segments.stream().filter(Segment::isNotNegated).collect(Collectors.toList());
  }

  public void add(String segmentText, int start, NegationType negationType) {
    Segment segment = new Segment(segmentText, start, negationType);
    add(segment);
  }

  public String getAsHtml() {
    StringBuffer html = new StringBuffer();
    html.append("<html><head><style>span.neg {color: red;}</style></head><body>");
    for (Segment s : segments) {
      if (s.isNegated())
        html.append("<span class=\"neg\">");
      html.append(s.getText());
      if (s.isNegated())
        html.append("</span>");
    }
    html.append("</body></html>");
    String result = html.toString().replace("\n", "<br>");

    return result;
  }

}
