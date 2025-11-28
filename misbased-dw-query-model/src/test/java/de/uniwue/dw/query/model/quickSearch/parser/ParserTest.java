package de.uniwue.dw.query.model.quickSearch.parser;

import de.uniwue.dw.query.model.quickSearch.QuickSearchAPI;
import org.junit.Test;

import java.text.ParseException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ParserTest {

  // @Test
  // public void testCatalogEntriesWithoutDomains() {
//    // @formatter:off
//    assertEquals("(expression (queryAttribute (boolAttribute (catalogEntry a))))", p("a"));
//    assertEquals("(expression (queryAttribute (boolAttribute (catalogEntry A))))", p("A"));
//    assertEquals("(expression (queryAttribute (boolAttribute (catalogEntry ABadfAad))))", p("ABadfAad"));
//    assertEquals("(expression (queryAttribute (boolAttribute (catalogEntry 12:asdfaADSF))))", p("12:asdfaADSF"));
//    assertEquals("(expression (queryAttribute (boolAttribute (catalogEntry 12:adadsf/+#-.²³$§adf))))", p("12:adadsf/+#-.²³$§adf"));
//
//    assertEquals("(expression (queryAttribute (boolAttribute (catalogEntry 'a'))))", p("'a'"));
//    assertEquals("(expression (queryAttribute (boolAttribute (catalogEntry 'A'))))", p("'A'"));
//    assertEquals("(expression (queryAttribute (boolAttribute (catalogEntry 'ABadfAad'))))", p("'ABadfAad'"));
//    assertEquals("(expression (queryAttribute (boolAttribute (catalogEntry '12:asdfaADSF'))))", p("'12:asdfaADSF'"));
//    assertEquals("(expression (queryAttribute (boolAttribute (catalogEntry '12:adadsf/+#-.°^²³$§adf'))))", p("'12:adadsf/+#-.°^²³$§adf'"));
////    assertEquals("(expression (queryAttribute (boolAttribute (catalogEntry 'andund<>='))))", p("'andund<>='"));
//
//    assertEquals("(expression (queryAttribute (boolAttribute (catalogEntry 'a adf'))))", p("'a adf'"));
//    assertEquals("(expression (queryAttribute (boolAttribute (catalogEntry 'A ADF'))))", p("'A ADF'"));
//    assertEquals("(expression (queryAttribute (boolAttribute (catalogEntry '12: asdfaADSF'))))", p("'12: asdfaADSF'"));
//    assertEquals("(expression (queryAttribute (boolAttribute (catalogEntry '12: ad adsf/+#-., ° ^² ³$§adf'))))", p("'12: ad adsf/+#-., ° ^² ³$§adf'"));
//    assertEquals("(expression (queryAttribute (boolAttribute (catalogEntry 'and und < > ='))))", p("'and und < > ='"));
//    // @formatter:on
  // }

  @Test
  public void testCatalogEntries() {
    t("a");
    t("A");
    t("ABadfAad");
    t("12:asdfaADSF");
    t("12:12:adadsf/+#-.²³$§adf");
    t("12:12:adadsf/+#-.²³$§adf");
    t("ABadfAad");
    f("A{BadfAad");
    f("{ABadfAad");
    f("ABad}fAad");
  }

  @Test
  public void testArugments() {
    t("a = 10");
    t("a < 10");
    t("a > 10");
    t("a >= 10");
  }

  @Test
  public void testGroup() {
    t("{a}");
    t("{a AND b}");
    t("{a OR b}");
    t("abc AND {a}");
    t("abc AND {a} AND b");
    t("abc AND {a OR dx} AND b");
    t("{a = 50}");
    t("{a > 50}");
    t("{a <= 50}");
    t("{in:a text}");
  }

  @Test
  public void testAlias() {
    t("a AS 1");
    t("a AS a");
    t("a AS a1");

    t("a as 1");
    t("a as a");
    t("a as a1");

    t("a ALS 1");
    t("a ALS a");
    t("a ALS a1");

    t("a Als 1");
    t("a Als a");
    t("a Als a1");

    t("a als 1");
    t("a als a");
    t("a als a1");

    t("A");
    t("ABadfAad");
    t("12:asdfaADSF");
    t("12:12:adadsf/+#-.²³$§adf");
    t("12:12:adadsf/+#-.²³$§adf");
    t("ABadfAad");

    t("a = 10");
    t("a < 10");
    t("a > 10");
    t("a >= 10");

    t("{a}");
    t("{a AND b}");
    t("{a OR b}");
    t("abc AND {a}");
    t("abc AND {a} AND b");
    t("abc AND {a OR dx} AND b");
    t("{a = 50}");
    t("{a > 50}");
    t("{a <= 50}");
    t("{in:a text}");

  }

  //
  // @Test
  // public void testCatalogEntriesWithDomains() {
//    // @formatter:off
//    assertEquals("(expression (queryAttribute (catalogEntry a @ (domain Labor))))", p("a@Labor"));
//    assertEquals("(expression (queryAttribute (catalogEntry A @ (domain Labor))))", p("A@Labor"));
//    assertEquals("(expression (queryAttribute (catalogEntry ABadfAad @ (domain Labor))))", p("ABadfAad@Labor"));
//    assertEquals("(expression (queryAttribute (catalogEntry 12:asdfaADSF @ (domain Labor))))", p("12:asdfaADSF@Labor"));
//    assertEquals("(expression (queryAttribute (catalogEntry 12:adadsf/+#-.²³$§adf @ (domain Labor))))", p("12:adadsf/+#-.²³$§adf@Labor"));
//
//    assertEquals("(expression (queryAttribute (catalogEntry 'a' @ (domain Labor))))", p("'a'@Labor"));
//    assertEquals("(expression (queryAttribute (catalogEntry 'A' @ (domain Labor))))", p("'A'@Labor"));
//    assertEquals("(expression (queryAttribute (catalogEntry 'ABadfAad' @ (domain Labor))))", p("'ABadfAad'@Labor"));
//    assertEquals("(expression (queryAttribute (catalogEntry '12:asdfaADSF' @ (domain Labor))))", p("'12:asdfaADSF'@Labor"));
//    assertEquals("(expression (queryAttribute (catalogEntry '12:adadsf/+#-.°^²³$§adf' @ (domain Labor))))",
//            p("'12:adadsf/+#-.°^²³$§adf'@Labor"));
//    assertEquals("(expression (queryAttribute (catalogEntry 'andund<>=' @ (domain Labor))))", p("'andund<>='@Labor"));
//
//    assertEquals("(expression (queryAttribute (catalogEntry 'a adf' @ (domain Labor))))", p("'a adf'@Labor"));
//    assertEquals("(expression (queryAttribute (catalogEntry 'A ADF' @ (domain Labor))))", p("'A ADF'@Labor"));
//    assertEquals("(expression (queryAttribute (catalogEntry '12: asdfaADSF' @ (domain Labor))))", p("'12: asdfaADSF'@Labor"));
//    assertEquals("(expression (queryAttribute (catalogEntry '12: ad adsf/+#-., ° ^² ³$§adf' @ (domain Labor))))",
//            p("'12: ad adsf/+#-., ° ^² ³$§adf'@Labor"));
//    assertEquals("(expression (queryAttribute (catalogEntry 'and und < > =' @ (domain Labor))))", p("'and und < > ='@Labor"));
//    // @formatter:on
  // }
  //
  // @Test
  // public void testAnd() {
  // assertEquals(
  // "(expression (expression (queryAttribute (catalogEntry a))) and (expression (queryAttribute
  // (catalogEntry b))))",
  // p("a and b"));
  // assertEquals(
  // "(expression (expression (queryAttribute (catalogEntry 'a'))) and (expression (queryAttribute
  // (catalogEntry b @ (domain Labor)))))",
  // p("'a' and b@Labor"));
  // assertEquals(
  // "(expression (expression (queryAttribute (catalogEntry 'und' @ (domain Labor)))) and
  // (expression (queryAttribute (catalogEntry 'and'))))",
  // p("'und'@Labor and 'and'"));
  // assertEquals(
  // "(expression (expression (expression (expression (queryAttribute (catalogEntry a))) ,
  // (expression (queryAttribute (catalogEntry b)))) , (expression (queryAttribute (catalogEntry
  // c)))) , (expression (queryAttribute (catalogEntry d))))",
  // p("a, b,c,d"));
  // assertEquals(
  // "(expression (expression (expression (expression (expression (expression (expression
  // (queryAttribute (catalogEntry a))) and (expression (queryAttribute (catalogEntry b)))) and
  // (expression (queryAttribute (catalogEntry c)))) And (expression (queryAttribute (catalogEntry
  // d)))) UND (expression (queryAttribute (catalogEntry c)))) & (expression (queryAttribute
  // (catalogEntry d)))) , (expression (queryAttribute (catalogEntry e))))",
  // p("a and b and c And d UND c & d,e"));
  // }
  //
  // @Test
  // public void testOr() {
  // assertEquals(
  // "(expression (expression (queryAttribute (catalogEntry a))) oder (expression (queryAttribute
  // (catalogEntry b))))",
  // p("a oder b"));
  // assertEquals(
  // "(expression (expression (queryAttribute (catalogEntry 'a'))) or (expression (queryAttribute
  // (catalogEntry b @ (domain Labor)))))",
  // p("'a' or b@Labor"));
  // assertEquals(
  // "(expression (expression (queryAttribute (catalogEntry 'und' @ (domain Labor)))) Oder
  // (expression (queryAttribute (catalogEntry 'and'))))",
  // p("'und'@Labor Oder 'and'"));
  // assertEquals(
  // "(expression (expression (expression (expression (expression (expression (expression
  // (queryAttribute (catalogEntry a))) oder (expression (queryAttribute (catalogEntry b)))) Oder
  // (expression (queryAttribute (catalogEntry c)))) Or (expression (queryAttribute (catalogEntry
  // d)))) OR (expression (queryAttribute (catalogEntry c)))) | (expression (queryAttribute
  // (catalogEntry d)))) || (expression (queryAttribute (catalogEntry e))))",
  // p("a oder b Oder c Or d OR c | d || e"));
  // }
  //
  // @Test
  // public void testNumericOperatorsAndArguments() {
  // assertEquals(
  // "(expression (queryAttribute (catalogEntry a) (numericCondition (numericOperator <) 5)))",
  // p("a < 5"));
  // assertEquals(
  // "(expression (queryAttribute (catalogEntry a) (numericCondition (numericOperator <) 5)))",
  // p("a <5"));
  // assertEquals(
  // "(expression (queryAttribute (catalogEntry a) (numericCondition (numericOperator <) 5,9)))",
  // p("a < 5,9"));
  // assertEquals(
  // "(expression (queryAttribute (catalogEntry a) (numericCondition (numericOperator <) 5,0)))",
  // p("a <5,0"));
  // assertEquals(
  // "(expression (queryAttribute (catalogEntry a) (numericCondition (numericOperator >) 5)))",
  // p("a > 5"));
  // assertEquals(
  // "(expression (queryAttribute (catalogEntry a) (numericCondition (numericOperator >) 5)))",
  // p("a >5"));
  // assertEquals(
  // "(expression (queryAttribute (catalogEntry a) (numericCondition (lowerBound 1) ... (upperBound
  // 10))))",
  // p("a 1 ... 10"));
  // assertEquals(
  // "(expression (queryAttribute (catalogEntry a) (numericCondition (lowerBound 1) ... (upperBound
  // 10))))",
  // p("a 1...10"));
  // assertEquals(
  // "(expression (queryAttribute (catalogEntry a) (numericCondition (lowerBound 1,9) ...
  // (upperBound 10,4))))",
  // p("a 1,9...10,4"));
  // assertEquals(
  // "(expression (queryAttribute (catalogEntry a) (numericCondition (lowerBound -1,9) ...
  // (upperBound -10,4))))",
  // p("a -1,9...-10,4"));
  // }
  //
  // @Test
  // public void testUnfinishedAttributeInput() {
  // assertEquals("abc", u("abc"));
  // assertEquals("'abc'", u("'abc'"));
  // assertEquals("xyz", u("(a und b) || c and d & xyz"));
  // assertEquals("xyz", u("((a <5 und b 11,4...9,3)) || c@Labor and 'd adf' & xyz"));
  // assertEquals("ee fff ggg", u(" 'aaa and ccc' and abc and e,ee fff ggg"));
  // }
  //
  private static String u(String input) {
    String unfinished = QuickSearchAPI.getUnfinishedAttributeInput(input);
    System.out.println(unfinished);
    return unfinished;
  }

  private static String pwe(String input) throws ParseException {
    String stringTree = QuickSearchAPI.parseAndThrowExecptionOnError(input);
    System.out.println(stringTree);
    return stringTree;
  }

  public static void main(String[] args) {
    QuickSearchAPI.parseAndPrintTree("a als a");
    System.out.println();
    QuickSearchAPI.parseAndPrintTree("(a)");
    System.out.println();
    QuickSearchAPI.parseAndPrintTree("{a}");
  }

  private static String p(String input) {
    String stringTree = QuickSearchAPI.parseToStringTree(input);
    System.out.println(stringTree);
    return stringTree;
  }

  private static void t(String input) {
    assertEquals(0, errors(input));
  }

  private static void f(String input) {
    assertTrue(0 < errors(input));
  }

  private static int errors(String input) {
    return QuickSearchAPI.parseAndCheckErrors(input);
  }

}
