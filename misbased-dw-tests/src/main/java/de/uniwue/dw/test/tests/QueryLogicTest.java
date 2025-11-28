package de.uniwue.dw.test.tests;

import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.tests.QueryTest;
import de.uniwue.dw.tests.AbstractTest;
import de.uniwue.dw.tests.SystemTestParameters;
import de.uniwue.misc.sql.DBType;
import org.junit.Test;

import javax.security.auth.login.AccountException;
import java.io.File;
import java.util.ArrayList;

public abstract class QueryLogicTest extends AbstractTest {

  protected static File testPropertiesFile = SystemTestParameters.getInstance()
          .getQUERY_LOGIC_TEST_FILE();

  public QueryLogicTest(DBType aType, QueryEngineType engineType) {
    super(aType, engineType);
  }

  public static ArrayList<QueryTest> getTests() throws Exception {
    initialize(DBType.MSSQL, QueryEngineType.SQL, testPropertiesFile, ImportDumpMode.ExchangeFormat,
            QueryLogicTest.class);
    return getTests("QueryLogicTests");
  }

  public static void doTests(QueryLogicTest test) throws Exception {
    test.testDisplayCaseID();
    test.testDisplayCaseID2();
    test.testDisplayPID();
    test.testOneAttributeTest();
    test.testIDFilterPatient();
    test.testIDFilterCase();
    test.testIDFilterPatientOnlyCount();
    test.testIDFilterCaseOnlyCount();
    test.testMoreThanDate();
    test.testAnd();
    test.testCount();
    test.testMultipleAttributes();
    test.testMultipleValuesFalse();
    test.testMultipleValuesTrue();
    test.testOptional();
    test.testOr();
    test.testDesiredContent();
    test.testDesiredContentHighlight();
    test.testTemporalAbsWindowTest();
    test.testInfoDate();
    test.testCaseID();
    test.testDocID();
    test.testDisplayValue();
    test.testInfoDateWithCaseIDAndDocID();
    test.testSearchSubEntries();
    test.testMultipleRows();
    test.testTemporalOpsRel1();
    test.testTemporalOpsRel2();
    test.testExistingCaseIDsWithDate();
    test.testExistingCaseIDsWithDateAndOr();
    test.testExistingPIDsWithTime();
    test.testExistingPIDsWithDate();
    test.testExistingPIDsWithCaseAndOr();
    test.testExistingPIDsWithOr();
    test.testExistingPIDsWithoutTime();
    test.testExistingYears();
    test.testDistinct1();
    test.testDistinct2();
    test.testDistinct3();
    test.testDistinct4();
    test.testValueCompare1();
    test.testSingleChoice();
    test.testValueCompare1();
    test.relativeOnlyCountTest();
    test.relativeTest1();
    test.relativeTest2();
    test.relativeTest3();
    test.relativeTest4();
    test.relativeTest5();
    test.relativeTest6();
    test.testTemporalOpsRel1();
    test.testTemporalOpsRel2();
    test.testTemporalOpsRel3();
    test.testTemporalOpsRel4();
    test.testTemporalOpsRel5();
    test.testTemporalOpsRel6();
    test.testTemporalOpsRel7();
    test.testTemporalOpsRel8();
    test.testTemporalOpsRel9();
    test.testTemporalOpsRel10();
    test.testTemporalOpsRel11();
    test.testTemporalOpsRel12();
    finish();
  }

  @Test
  public void testDisplayPID() throws QueryException, AccountException {
    startTest("QueryLogicTests/DisplayPIDTest", queryClient);
  }

  @Test
  public void testDisplayCaseID() throws QueryException, AccountException {
    startTest("QueryLogicTests/DisplayCaseIDTest", queryClient);
  }

  @Test
  public void testDisplayCaseID2() throws QueryException, AccountException {
    startTest("QueryLogicTests/DisplayCaseIDTest2", queryClient);
  }

  @Test
  public void testOneAttributeTest() throws QueryException, AccountException {
    startTest("QueryLogicTests/OneAttributeTest", queryClient);
  }

  @Test
  public void testIDFilterPatient() throws QueryException, AccountException {
    startTest("QueryLogicTests/IDFilterPatientTest", queryClient);
  }

  @Test
  public void testIDFilterPatientOnlyCount() throws QueryException, AccountException {
    startTest("QueryLogicTests/IDFilterPatientOnlyCountTest", queryClient);
  }

  @Test
  public void testIDFilterCase() throws QueryException, AccountException {
    startTest("QueryLogicTests/IDFilterCaseTest", queryClient);
  }

  @Test
  public void testIDFilterGroup() throws QueryException, AccountException {
    startTest("QueryLogicTests/IDFilterGroupTest", queryClient);
  }

  @Test
  public void testIDFilterCaseOnlyCount() throws QueryException, AccountException {
    startTest("QueryLogicTests/IDFilterCaseOnlyCountTest", queryClient);
  }

  @Test
  public void testPropagate() throws QueryException, AccountException {
    startTest("QueryLogicTests/PropagateTest", queryClient);
  }

  @Test
  public void testMoreThanDate() throws QueryException, AccountException {
    startTest("QueryLogicTests/MoreThanDateTest", queryClient);
  }

  @Test
  public void testMoreThanWithOptional() throws QueryException, AccountException {
    startTest("QueryLogicTests/MoreThanWithOptionalTest", queryClient);
  }

  @Test
  public void testOptional() throws QueryException, AccountException {
    startTest("QueryLogicTests/OptionalTest", queryClient);
  }

  @Test
  public void testAnd() throws QueryException, AccountException {
    startTest("QueryLogicTests/AndTest", queryClient);
  }

  @Test
  public void testOr() throws QueryException, AccountException {
    startTest("QueryLogicTests/OrTest", queryClient);
  }

  @Test
  public void testMultipleAttributes() throws QueryException, AccountException {
    startTest("QueryLogicTests/MultipleAttributesTest", queryClient);
  }

  @Test
  public void testDesiredContent() throws QueryException, AccountException {
    startTest("QueryLogicTests/DesiredContentTest", queryClient);
  }

  @Test
  public void testDesiredContentHighlight() throws QueryException, AccountException {
    startTest("QueryLogicTests/DesiredContentHighlightTest", queryClient);
  }

  @Test
  public void testMultipleValuesFalse() throws QueryException, AccountException {
    startTest("QueryLogicTests/MultipleValuesFalseTest", queryClient);
  }

  @Test
  public void testMultipleValuesTrue() throws QueryException, AccountException {
    startTest("QueryLogicTests/MultipleValuesTrueTest", queryClient);
  }

  @Test
  public void testCount() throws QueryException, AccountException {
    startTest("QueryLogicTests/CountTest", queryClient);
  }

  @Test
  public void testTemporalAbsWindowTest() throws QueryException, AccountException {
    startTest("QueryLogicTests/TemporalOpAbsWindowTest", queryClient);
  }

  @Test
  public void testInfoDate() throws QueryException, AccountException {
    startTest("QueryLogicTests/InfoDateTest", queryClient);
  }

  @Test
  public void testCaseID() throws QueryException, AccountException {
    startTest("QueryLogicTests/CaseIDTest", queryClient);
  }

  @Test
  public void testDocID() throws QueryException, AccountException {
    startTest("QueryLogicTests/DocIDTest", queryClient);
  }

  @Test
  public void testDisplayValue() throws QueryException, AccountException {
    startTest("QueryLogicTests/DisplayValueTest", queryClient);
  }

  @Test
  public void testInfoDateWithCaseIDAndDocID() throws QueryException, AccountException {
    startTest("QueryLogicTests/InfoDateWithCaseIDAndDocIDTest", queryClient);
  }

  @Test
  public void testTemporalOpsRel1() throws QueryException, AccountException {
    startTest("QueryLogicTests/TemporalOpsRelTest1", queryClient);
  }

  @Test
  public void testTemporalOpsRel2() throws QueryException, AccountException {
    startTest("QueryLogicTests/TemporalOpsRelTest2", queryClient);
  }

  @Test
  public void testSearchSubEntries() throws QueryException, AccountException {
    startTest("QueryLogicTests/SearchSubEntriesTest", queryClient);
  }

  @Test
  public void testMultipleRows() throws QueryException, AccountException {
    startTest("QueryLogicTests/MultipleRowsTest", queryClient);
  }

  @Test
  public void testExistingCaseIDsWithDate() throws QueryException, AccountException {
    startTest("QueryLogicTests/ExistingCaseIDsWithDateTest", queryClient);
  }

  @Test
  public void testExistingCaseIDsWithDateAndOr() throws QueryException, AccountException {
    startTest("QueryLogicTests/ExistingCaseIDsWithDateAndOrTest", queryClient);
  }

  @Test
  public void testExistingPIDsWithTime() throws QueryException, AccountException {
    startTest("QueryLogicTests/ExistingPIDsWithTimeTest", queryClient);
  }

  @Test
  public void testExistingPIDsWithDate() throws QueryException, AccountException {
    startTest("QueryLogicTests/ExistingPIDsWithDateTest", queryClient);
  }

  @Test
  public void testExistingPIDsWithCaseAndOr() throws QueryException, AccountException {
    startTest("QueryLogicTests/ExistingPIDsWithCaseAndOrTest", queryClient);
  }

  @Test
  public void testExistingPIDsWithOr() throws QueryException, AccountException {
    startTest("QueryLogicTests/ExistingPIDsWithOrTest", queryClient);
  }

  @Test
  public void testExistingPIDsWithoutTime() throws QueryException, AccountException {
    startTest("QueryLogicTests/ExistingPIDsWithoutTimeTest", queryClient);
  }

  @Test
  public void testExistingYears() throws QueryException, AccountException {
    startTest("QueryLogicTests/ExistingYearsTest", queryClient);
  }

  @Test
  public void testDistinct1() throws QueryException, AccountException {
    startTest("QueryLogicTests/DistinctTest1", queryClient);
  }

  @Test
  public void testDistinct2() throws QueryException, AccountException {
    startTest("QueryLogicTests/DistinctTest2", queryClient);
  }

  @Test
  public void testDistinct3() throws QueryException, AccountException {
    startTest("QueryLogicTests/DistinctTest3", queryClient);
  }

  @Test
  public void testDistinct4() throws QueryException, AccountException {
    startTest("QueryLogicTests/DistinctTest4", queryClient);
  }

  @Test
  public void testValueCompare1() throws QueryException, AccountException {
    startTest("QueryLogicTests/ValueCompareTest1", queryClient);
  }

  @Test
  public void testSingleChoice() throws QueryException, AccountException {
    startTest("QueryLogicTests/SingleChoiceTest", queryClient);
  }

  @Test
  public void relativeTest1() throws QueryException, AccountException {
    startTest("QueryLogicTests/relativeTests/NatriumCalciumLess", queryClient);
  }

  @Test
  public void relativeTest2() throws QueryException, AccountException {
    startTest("QueryLogicTests/relativeTests/NatriumCalciumLessEqual", queryClient);
  }

  @Test
  public void relativeTest3() throws QueryException, AccountException {
    startTest("QueryLogicTests/relativeTests/NatriumCalciumMore", queryClient);
  }

  @Test
  public void relativeTest4() throws QueryException, AccountException {
    startTest("QueryLogicTests/relativeTests/NatriumCalciumMoreEqual", queryClient);
  }

  @Test
  public void relativeTest5() throws QueryException, AccountException {
    startTest("QueryLogicTests/relativeTests/NatriumEqual", queryClient);
  }

  @Test
  public void relativeTest6() throws QueryException, AccountException {
    startTest("QueryLogicTests/relativeTests/NatriumCaseIDMore", queryClient);
  }

  @Test
  public void testTemporalOpsRel3() throws QueryException, AccountException {
    startTest("QueryLogicTests/relativeTests/TemporalOpsRelTest3", queryClient);
  }

  @Test
  public void testTemporalOpsRel4() throws QueryException, AccountException {
    startTest("QueryLogicTests/relativeTests/TemporalOpsRelTest4", queryClient);
  }

  @Test
  public void testTemporalOpsRel5() throws QueryException, AccountException {
    startTest("QueryLogicTests/relativeTests/TemporalOpsRelTest5", queryClient);
  }

  @Test
  public void testTemporalOpsRel6() throws QueryException, AccountException {
    startTest("QueryLogicTests/relativeTests/TemporalOpsRelTest6", queryClient);
  }

  @Test
  public void testTemporalOpsRel7() throws QueryException, AccountException {
    startTest("QueryLogicTests/relativeTests/TemporalOpsRelTest7", queryClient);
  }

  @Test
  public void testTemporalOpsRel8() throws QueryException, AccountException {
    startTest("QueryLogicTests/relativeTests/TemporalOpsRelTest8", queryClient);
  }

  @Test
  public void testTemporalOpsRel9() throws QueryException, AccountException {
    startTest("QueryLogicTests/relativeTests/TemporalOpsRelTest9", queryClient);
  }

  @Test
  public void testTemporalOpsRel10() throws QueryException, AccountException {
    startTest("QueryLogicTests/relativeTests/TemporalOpsRelTest10", queryClient);
  }

  @Test
  public void testTemporalOpsRel11() throws QueryException, AccountException {
    startTest("QueryLogicTests/relativeTests/TemporalOpsRelTest11", queryClient);
  }

  @Test
  public void testTemporalOpsRel12() throws QueryException, AccountException {
    startTest("QueryLogicTests/relativeTests/TemporalOpsRelTest12", queryClient);
  }

  @Test
  public void relativeOnlyCountTest() throws QueryException, AccountException {
    startTest("QueryLogicTests/relativeTests/onlyCount", queryClient);
  }
}
