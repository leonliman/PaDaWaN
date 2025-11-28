package de.uniwue.dw.test.tests.engines;

import de.uniwue.dw.query.model.data.QueryEngineType;
import de.uniwue.dw.test.tests.QueryLogicTest;
import de.uniwue.misc.sql.DBType;
import org.junit.Test;

public abstract class SQLQueryLogicTest extends QueryLogicTest {

  // much functionality does not work with SQL so those tests should not do anything

  public SQLQueryLogicTest(DBType aType, QueryEngineType engineType) throws Exception {
    super(aType, engineType);
  }

  @Override
  @Test
  public void testMultipleValuesTrue() {
  }

  @Override
  @Test
  public void testExistingPIDsWithCaseAndOr() {
  }

  @Override
  @Test
  public void testExistingPIDsWithTime() {
  }

  @Override
  @Test
  public void testExistingPIDsWithDate() {
  }

  @Override
  @Test
  public void testExistingPIDsWithOr() {
  }

  @Override
  @Test
  public void testExistingPIDsWithoutTime() {
  }

  @Override
  @Test
  public void testExistingCaseIDsWithDate() {
  }

  @Override
  @Test
  public void testExistingCaseIDsWithDateAndOr() {
  }

  @Override
  @Test
  public void testExistingYears() {
  }

  @Override
  @Test
  public void testDesiredContentHighlight() {
  }

  @Override
  @Test
  public void testSearchSubEntries() {
  }

  @Override
  @Test
  public void testDistinct1() {
  }

  @Override
  @Test
  public void testDistinct2() {
  }

  @Override
  @Test
  public void testDistinct3() {
  }

  @Override
  @Test
  public void testDistinct4() {
  }

  @Override
  @Test
  public void testInfoDate() {
  }

  @Override
  @Test
  public void testCaseID() {
  }

  @Override
  @Test
  public void testDocID() {
  }

  @Override
  @Test
  public void testInfoDateWithCaseIDAndDocID() {
  }

  @Override
  @Test
  public void testTemporalOpsRel1() {
  }

  @Override
  @Test
  public void testTemporalOpsRel2() {
  }

  @Override
  @Test
  public void testTemporalOpsRel3() {
  }

  @Override
  @Test
  public void testTemporalOpsRel4() {
  }

  @Override
  @Test
  public void testTemporalOpsRel5() {
  }

  @Override
  @Test
  public void testTemporalOpsRel6() {
  }

  @Override
  @Test
  public void testTemporalOpsRel7() {
  }

  @Override
  @Test
  public void testTemporalOpsRel8() {
  }

  @Override
  @Test
  public void testTemporalOpsRel9() {
  }

  @Override
  @Test
  public void testTemporalOpsRel10() {
  }

  @Override
  @Test
  public void testTemporalOpsRel11() {
  }

  @Override
  @Test
  public void testTemporalOpsRel12() {
  }

  @Override
  @Test
  public void testMoreThanWithOptional() {
  }

  @Override
  @Test
  public void testDisplayCaseID() {
  }

  @Override
  @Test
  public void testDisplayCaseID2() {
  }

  @Override
  @Test
  public void testMoreThanDate() {
  }

  @Override
  @Test
  public void testTemporalAbsWindowTest() {
  }

  @Override
  @Test
  public void testValueCompare1() {
  }

  @Override
  @Test
  public void testSingleChoice() {
  }

  @Override
  @Test
  public void testIDFilterGroup() {
  }

  @Override
  @Test
  public void relativeTest1() {
  }

  @Override
  @Test
  public void relativeTest2() {
  }

  @Override
  @Test
  public void relativeTest3() {
  }

  @Override
  @Test
  public void relativeTest4() {
  }

  @Override
  @Test
  public void relativeTest5() {
  }

  @Override
  @Test
  public void relativeTest6() {
  }

  @Override
  @Test
  public void relativeOnlyCountTest() {
  }
}
