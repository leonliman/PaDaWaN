package de.uniwue.dw.core.model.manager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import javax.security.auth.login.AccountException;

import org.junit.Before;
import org.junit.Test;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.CountType;
import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.client.authentication.group.AuthManager;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.query.model.TestEnvironmentDataLoader;
import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.index.IndexException;
import de.uniwue.misc.util.ConfigException;

public abstract class CatalogClientManagerTest {

  private ICatalogClientManager manager;

  private AuthManager authManager;

  public abstract TestEnvironmentDataLoader getTestEnvironmentDataLoader()
          throws DataSourceException;

  @Before
  public void prepare() throws IOException, IndexException, SQLException, URISyntaxException,
          NoSuchAlgorithmException, GUIClientException, DataSourceException, ConfigException {
    TestEnvironmentDataLoader dataLoader = getTestEnvironmentDataLoader();
    dataLoader.loadDataIfNecessary();
    manager = dataLoader.getCompleteCatalogClientManager();
    authManager = DwClientConfiguration.getInstance().getAuthManager();
  }

  private User getAdminUser() throws AccountException, ConfigException {
    User demo = authManager.authenticate("demo", "demo", null);
    return demo;
  }

  private User getNonAdminUser() throws AccountException, ConfigException {
    User demo = authManager.authenticate("demo1", "demo", null);
    return demo;
  }

  @Test
  public void testRoot() throws DataSourceException {
    assertEquals(0, manager.getRoot().getAttrId());
  }

  @Test
  public void testEntryByRefID()
          throws DataSourceException, AccountException, SQLException, ConfigException {
    User demo = getAdminUser();
    assertEquals("B18 : Chronische Virushepatitis",
            manager.getEntryByRefID("b18", "diagnose", demo).getName());
  }

  @Test(expected = DataSourceException.class)
  public void testEntryByRefIDNotExist()
          throws DataSourceException, AccountException, SQLException, ConfigException {
    User demo = getAdminUser();
    assertEquals(null, manager.getEntryByRefID("z30", "diagnose", demo));
  }

  @Test(expected = RuntimeException.class)
  public void testEntryByRefIDNullExtID()
          throws DataSourceException, AccountException, SQLException, ConfigException {
    User demo = getAdminUser();
    assertEquals(null, manager.getEntryByRefID(null, "diagnose", demo));
  }

  @Test(expected = RuntimeException.class)
  public void testEntryByRefIDNullProject()
          throws DataSourceException, AccountException, SQLException, ConfigException {
    User demo = getAdminUser();
    assertEquals(null, manager.getEntryByRefID("b18", null, demo));
  }

  @Test
  public void testEntryByID()
          throws DataSourceException, AccountException, SQLException, ConfigException {
    User demo = getAdminUser();
    CatalogEntry entry = manager.getEntryByRefID("b18", "diagnose", demo);
    assertEquals(entry, manager.getEntryByID(entry.getAttrId(), demo));
  }

  @Test
  public void testEntryByIDDifferent()
          throws DataSourceException, AccountException, SQLException, ConfigException {
    User demo = getAdminUser();
    CatalogEntry entry = manager.getEntryByRefID("b18", "diagnose", demo);
    assertNotSame(entry, manager.getEntryByID(entry.getAttrId() + 1, demo));
  }

  @Test
  public void testEntryByIDNotExist()
          throws DataSourceException, AccountException, SQLException, ConfigException {
    User demo = getAdminUser();
    assertEquals(null, manager.getEntryByID(-1, demo));
  }

  @Test
  public void testEntryByRefIDWithUser()
          throws DataSourceException, SQLException, AccountException, ConfigException {
    User demo = getAdminUser();
    assertEquals("B18 : Chronische Virushepatitis",
            manager.getEntryByRefID("b18", "diagnose", demo).getName());
  }

  @Test
  public void testEntryByRefIDWithUserNull()
          throws DataSourceException, SQLException, AccountException {
    assertEquals("B18 : Chronische Virushepatitis",
            manager.getEntryByRefID("b18", "diagnose", null).getName());
  }

  @Test(expected = DataSourceException.class)
  public void testEntryByRefIDWithUserNotEntitled()
          throws SQLException, DataSourceException, AccountException, ConfigException {
    User demo = getNonAdminUser();
    assertEquals("B18 : Chronische Virushepatitis",
            manager.getEntryByRefID("b18", "diagnose", demo).getName());
  }

  @Test(expected = DataSourceException.class)
  public void testEntryByRefIDWithUserThrowException()
          throws AccountException, SQLException, DataSourceException, ConfigException {
    User demo = getAdminUser();
    assertEquals(null, manager.getEntryByRefID("z31", "diagnose", demo, true));
  }

  @Test
  public void testEntryByRefIDWithUserNullThrowNoException()
          throws AccountException, SQLException, DataSourceException {
    assertEquals(null, manager.getEntryByRefID("z36", "diagnose", null, false));
  }

  @Test
  public void testChildsOfRootWithUser()
          throws AccountException, SQLException, DataSourceException, ConfigException {
    User demo = getAdminUser();
    ArrayList<CatalogEntry> list = new ArrayList<>();
    list.add(manager.getEntryByRefID("stammdaten", "untersuchung", demo));
    list.add(manager.getEntryByRefID("labor", "untersuchung", demo));
    list.add(manager.getEntryByRefID("metadaten", "untersuchung", demo));
    list.add(manager.getEntryByRefID("diagnose", "untersuchung", demo));
    list.add(manager.getEntryByRefID("procedures", "untersuchung", demo));
    list.add(manager.getEntryByRefID("arztbriefe", "untersuchung", demo));
    assertEquals(list, manager.getChildsOf(manager.getRoot(), demo));
  }

  @Test(expected = DataSourceException.class)
  public void testChildsOfRootWithUserNotEntitled()
          throws AccountException, SQLException, DataSourceException, ConfigException {
    User demo = getNonAdminUser();
    ArrayList<CatalogEntry> list = new ArrayList<>();
    list.add(manager.getEntryByRefID("stammdaten", "untersuchung", demo));
    list.add(manager.getEntryByRefID("labor", "untersuchung", demo));
    list.add(manager.getEntryByRefID("metadaten", "untersuchung", demo));
    list.add(manager.getEntryByRefID("diagnose", "untersuchung", demo));
    list.add(manager.getEntryByRefID("procedures", "untersuchung", demo));
    list.add(manager.getEntryByRefID("arztbriefe", "untersuchung", demo));
    assertEquals(list, manager.getChildsOf(manager.getRoot(), demo));
  }

  @Test(expected = NullPointerException.class)
  public void testChildsOfRootWithUserNull()
          throws AccountException, SQLException, DataSourceException {
    User demo = null;
    ArrayList<CatalogEntry> list = new ArrayList<>();
    list.add(manager.getEntryByRefID("stammdaten", "untersuchung", demo));
    list.add(manager.getEntryByRefID("labor", "untersuchung", demo));
    list.add(manager.getEntryByRefID("metadaten", "untersuchung", demo));
    list.add(manager.getEntryByRefID("diagnose", "untersuchung", demo));
    list.add(manager.getEntryByRefID("procedures", "untersuchung", demo));
    list.add(manager.getEntryByRefID("arztbriefe", "untersuchung", demo));
    assertEquals(list, manager.getChildsOf(manager.getRoot(), demo));
  }

  @Test
  public void testChildsOfEntry()
          throws AccountException, SQLException, DataSourceException, ConfigException {
    User demo = getAdminUser();
    CatalogEntry entry = manager.getEntryByRefID("5_42___5_54", "procedures", demo);
    ArrayList<CatalogEntry> list = new ArrayList<>();
    list.add(manager.getEntryByRefID("5_50", "procedures", demo));
    list.add(manager.getEntryByRefID("5_51", "procedures", demo));
    list.add(manager.getEntryByRefID("5_52", "procedures", demo));
    list.add(manager.getEntryByRefID("5_53", "procedures", demo));
    list.add(manager.getEntryByRefID("5_54", "procedures", demo));
    assertEquals(list, manager.getChildsOf(entry, demo));
  }

  @Test
  public void testChildsOfLeaf()
          throws AccountException, SQLException, DataSourceException, ConfigException {
    User demo = getAdminUser();
    CatalogEntry entry = manager.getEntryByRefID("1_49", "procedures", demo);
    ArrayList<CatalogEntry> list = new ArrayList<>();
    assertEquals(list, manager.getChildsOf(entry, demo));
  }

  @Test(expected = NullPointerException.class)
  public void testChildsOfNoEntry()
          throws AccountException, SQLException, DataSourceException, ConfigException {
    User demo = getAdminUser();
    CatalogEntry entry = null;
    ArrayList<CatalogEntry> list = new ArrayList<>();
    assertEquals(list, manager.getChildsOf(entry, demo));
  }

  // Doesn't work because count absolute always is 0
  // @Test
  // public void testChildsOfWithCountAbsolute() throws AccountException,
  // SQLException, DataSourceException {
  // User demo = getAdminUser();
  // CatalogEntry entry = manager.getEntryByRefID("labor", "untersuchung",
  // demo);
  // ArrayList<CatalogEntry> list = new ArrayList<>();
  // list.add(manager.getEntryByRefID("monozyten__", "labor", demo));
  // list.add(manager.getEntryByRefID("kalium_mmol_l", "labor", demo));
  // list.add(manager.getEntryByRefID("protein__u__", "labor", demo));
  // assertEquals(list, manager.getChildsOf(entry, demo, CountType.absolute,
  // 34));
  // }

  @Test
  public void testChildsOfWithCountCases()
          throws AccountException, SQLException, DataSourceException, ConfigException {
    User demo = getAdminUser();
    CatalogEntry entry = manager.getEntryByRefID("labor", "untersuchung", demo);
    ArrayList<CatalogEntry> list = new ArrayList<>();
    list.add(manager.getEntryByRefID("monozyten__", "labor", demo));
    list.add(manager.getEntryByRefID("natrium_mmol_l", "labor", demo));
    list.add(manager.getEntryByRefID("kalium_mmol_l", "labor", demo));
    list.add(manager.getEntryByRefID("protein__u__", "labor", demo));
    assertEquals(list, manager.getChildsOf(entry, demo, CountType.distinctCaseID, 16));
  }

  // Doesn't work because countDistinctPID is always 0
  // @Test
  // public void testChildsOfWithCountPID() throws AccountException,
  // SQLException, DataSourceException {
  // User demo = getAdminUser();
  // CatalogEntry entry = manager.getEntryByRefID("labor", "untersuchung",
  // demo);
  // ArrayList<CatalogEntry> list = new ArrayList<>();
  // list.add(manager.getEntryByRefID("kalium_mmol_l", "labor", demo));
  // list.add(manager.getEntryByRefID("protein__u__", "labor", demo));
  // assertEquals(list, manager.getChildsOf(entry, demo,
  // CountType.distinctPID, 9));
  // }

  @Test
  public void testEntriesByWordFilter()
          throws DataSourceException, AccountException, SQLException, ConfigException {
    User demo = getAdminUser();
    List<CatalogEntry> list = new ArrayList<>();
    list.add(manager.getEntryByRefID("caseid", "metadaten", demo));
    list.add(manager.getEntryByRefID("pid", "metadaten", demo));
    List<CatalogEntry> entries = manager.getEntriesByWordFilter("id", demo, 0);
    list.sort(getComparator());
    entries.sort(getComparator());
    for (int i = 0; i < list.size(); i++) {
      assertEquals(list.get(i).getUniqueName(), entries.get(i).getUniqueName());
    }
  }

  @Test
  public void testEntriesByWordFilter2()
          throws DataSourceException, AccountException, SQLException, ConfigException {
    User demo = getAdminUser();
    List<CatalogEntry> list = new ArrayList<>();
    list.add(manager.getEntryByRefID("geschlecht", "stammdaten", demo));
    list.add(manager.getEntryByRefID("1_46", "procedures", demo));
    List<CatalogEntry> entries = manager.getEntriesByWordFilter("geschlecht", demo, 0);
    list.sort(getComparator());
    entries.sort(getComparator());
    assertEquals(list.size(), entries.size());
    for (int i = 0; i < list.size(); i++) {
      assertEquals(list.get(i).getUniqueName(), entries.get(i).getUniqueName());
    }
  }

  @Test
  public void testEntriesByWordFilterWithLimit()
          throws DataSourceException, AccountException, SQLException, ConfigException {
    User demo = getAdminUser();
    List<CatalogEntry> entries = manager.getEntriesByWordFilter("id", demo, 1);
    assertTrue(entries.size() == 1);
    List<CatalogEntry> entries2 = manager.getEntriesByWordFilter("id", demo, 3);
    assertTrue(entries2.size() == 2);
  }

  @Test
  public void testEntriesByWordFilterWithLimit0()
          throws DataSourceException, AccountException, SQLException, ConfigException {
    User demo = getAdminUser();
    List<CatalogEntry> entries = manager.getEntriesByWordFilter("geschlecht", demo, 0);
    assertEquals(2,entries.size() );
  }

  @Test
  public void testEntriesByWordFilterWithUser()
          throws DataSourceException, AccountException, SQLException, ConfigException {
    User demo = getAdminUser();
    List<CatalogEntry> list = new ArrayList<>();
    list.add(manager.getEntryByRefID("natrium_mmol_l", "labor", demo));
    list.add(manager.getEntryByRefID("calcium_mmol_l", "labor", demo));
    list.add(manager.getEntryByRefID("kalium_mmol_l", "labor", demo));
    List<CatalogEntry> entries = manager.getEntriesByWordFilter("ium", demo, 0);
    list.sort(getComparator());
    entries.sort(getComparator());
    for (int i = 0; i < list.size(); i++) {
      assertEquals(list.get(i).getUniqueName(), entries.get(i).getUniqueName());
    }
  }

  @Test
  public void testEntriesByWordFilterWithUserNotEntitled()
          throws DataSourceException, AccountException, SQLException, ConfigException {
    User demo = getNonAdminUser();
    List<CatalogEntry> list = new ArrayList<>();
    list.add(manager.getEntryByRefID("natrium_mmol_l", "labor", demo));
    list.add(manager.getEntryByRefID("calcium_mmol_l", "labor", demo));
    list.add(manager.getEntryByRefID("kalium_mmol_l", "labor", demo));
    List<CatalogEntry> entries = manager.getEntriesByWordFilter("natrium", demo, 0);
    List<CatalogEntry> calcium = manager.getEntriesByWordFilter("calcium", demo, 0);
    List<CatalogEntry> kalium = manager.getEntriesByWordFilter("kalium", demo, 0);
    entries.addAll(calcium);
    entries.addAll(kalium);
    list.sort(getComparator());
    entries.sort(getComparator());
    for (int i = 0; i < list.size(); i++) {
      assertEquals(list.get(i).getUniqueName(), entries.get(i).getUniqueName());
    }
  }

  @Test
  public void testEntriesByWordFilterCountCasesMin0()
          throws DataSourceException, AccountException, SQLException, ConfigException {
    User demo = getAdminUser();
    List<CatalogEntry> list = new ArrayList<>();
    list.add(manager.getEntryByRefID("natrium_mmol_l", "labor", demo));
    list.add(manager.getEntryByRefID("calcium_mmol_l", "labor", demo));
    list.add(manager.getEntryByRefID("kalium_mmol_l", "labor", demo));
    List<CatalogEntry> entries = manager.getEntriesByWordFilter("ium", demo,
            CountType.distinctCaseID, 0, 0);
    list.sort(getComparator());
    entries.sort(getComparator());
    assertEquals(list.size(), entries.size());
    for (int i = 0; i < list.size(); i++) {
      assertEquals(list.get(i).getUniqueName(), entries.get(i).getUniqueName());
    }
  }

  @Test
  public void testEntriesByWordFilterCountCases()
          throws DataSourceException, AccountException, SQLException, ConfigException {
    User demo = getAdminUser();
    List<CatalogEntry> list = new ArrayList<>();
    list.add(manager.getEntryByRefID("natrium_mmol_l", "labor", demo));
    list.add(manager.getEntryByRefID("kalium_mmol_l", "labor", demo));
    // list.add(manager.getEntryByRefID("calcium_mmol_l", "labor", demo));
    List<CatalogEntry> entries = manager.getEntriesByWordFilter("ium", demo,
            CountType.distinctCaseID, 15, 0);
    list.sort(getComparator());
    entries.sort(getComparator());
    assertEquals(list.size(), entries.size());
    for (int i = 0; i < list.size(); i++) {
      assertEquals(list.get(i).getUniqueName(), entries.get(i).getUniqueName());
    }
  }

  @Test
  public void testTreeByWordFilterPhraseNotExists()
          throws DataSourceException, AccountException, SQLException, ConfigException {
    User demo = getAdminUser();
    CatalogEntry entry = manager.getTreeByWordFilter("fsdghsnab", demo);
    assertEquals(0, entry.getAllChildren().size());
  }

  @Test
  public void testTreeByWordFilter()
          throws DataSourceException, AccountException, SQLException, ConfigException {
    User demo = getAdminUser();
    CatalogEntry expected = manager.getRoot().copyWihtoutReferences();
    CatalogEntry e1 = manager.getEntryByRefID("diagnose", "untersuchung", demo)
            .copyWihtoutReferences();
    expected.getChildren().add(e1);
    CatalogEntry e2 = manager.getEntryByRefID("i", "diagnose", demo).copyWihtoutReferences();
    e1.getChildren().add(e2);
    CatalogEntry e3 = manager.getEntryByRefID("b15_b19", "diagnose", demo).copyWihtoutReferences();
    e2.getChildren().add(e3);
    CatalogEntry e4 = manager.getEntryByRefID("b18", "diagnose", demo).copyWihtoutReferences();
    e3.getChildren().add(e4);
    CatalogEntry actual = manager.getTreeByWordFilter("chronische", demo);
    assertTrue(areTreesEqual(expected, actual));
  }

  @Test
  public void testTreeByWordFilterWithCountCases()
          throws DataSourceException, AccountException, SQLException, ConfigException {
    User demo = getAdminUser();
    CatalogEntry root = manager.getRoot().copyWihtoutReferences();
    CatalogEntry e1 = manager.getEntryByRefID("stammdaten", "untersuchung", demo)
            .copyWihtoutReferences();
    root.getChildren().add(e1);
    CatalogEntry e2 = manager.getEntryByRefID("geschlecht", "stammdaten", demo)
            .copyWihtoutReferences();
    e1.getChildren().add(e2);
    CatalogEntry entry = manager.getTreeByWordFilter("geschlecht", demo, CountType.distinctCaseID,
            6);

    assertTrue(areTreesEqual(root, entry));
  }

  @Test
  public void testAncestersAndSiblings()
          throws AccountException, SQLException, DataSourceException, ConfigException {
    User demo = getAdminUser();
    CatalogEntry root = manager.getRoot().copyWihtoutReferences();
    root.getChildren().add(
            manager.getEntryByRefID("stammdaten", "untersuchung", demo).copyWihtoutReferences());
    root.getChildren()
            .add(manager.getEntryByRefID("labor", "untersuchung", demo).copyWihtoutReferences());
    root.getChildren().add(
            manager.getEntryByRefID("metadaten", "untersuchung", demo).copyWihtoutReferences());
    root.getChildren()
            .add(manager.getEntryByRefID("diagnose", "untersuchung", demo).copyWihtoutReferences());
    root.getChildren().add(
            manager.getEntryByRefID("procedures", "untersuchung", demo).copyWihtoutReferences());
    root.getChildren().add(
            manager.getEntryByRefID("arztbriefe", "untersuchung", demo).copyWihtoutReferences());
    CatalogEntry e = manager.getEntryByRefID("labor", "untersuchung", demo);
    CatalogEntry entry = manager.getAllAncestorsAndSiblings(e, demo, CountType.distinctCaseID, 0);
    assertTrue(areTreesEqual(root, entry));
  }

  @Test
  public void testAncestersAndSiblings2()
          throws AccountException, SQLException, DataSourceException, ConfigException {
    User demo = getAdminUser();
    CatalogEntry root = manager.getRoot().copyWihtoutReferences();
    root.getChildren().add(
            manager.getEntryByRefID("stammdaten", "untersuchung", demo).copyWihtoutReferences());
    root.getChildren()
            .add(manager.getEntryByRefID("labor", "untersuchung", demo).copyWihtoutReferences());
    CatalogEntry meta = manager.getEntryByRefID("metadaten", "untersuchung", demo)
            .copyWihtoutReferences();
    meta.getChildren()
            .add(manager.getEntryByRefID("pid", "metadaten", demo).copyWihtoutReferences());
    meta.getChildren()
            .add(manager.getEntryByRefID("caseid", "metadaten", demo).copyWihtoutReferences());
    root.getChildren().add(meta);
    root.getChildren()
            .add(manager.getEntryByRefID("diagnose", "untersuchung", demo).copyWihtoutReferences());
    root.getChildren().add(
            manager.getEntryByRefID("procedures", "untersuchung", demo).copyWihtoutReferences());
    root.getChildren().add(
            manager.getEntryByRefID("arztbriefe", "untersuchung", demo).copyWihtoutReferences());
    CatalogEntry e = manager.getEntryByRefID("pid", "metadaten", demo);
    CatalogEntry entry = manager.getAllAncestorsAndSiblings(e, demo, CountType.distinctCaseID, 0);
    assertTrue(areTreesEqual(root, entry));
  }

  @Test
  public void testAncestersAndSiblingsWithCountCases()
          throws AccountException, SQLException, DataSourceException, ConfigException {
    User demo = getAdminUser();
    CatalogEntry root = manager.getRoot().copyWihtoutReferences();
    root.getChildren().add(
            manager.getEntryByRefID("stammdaten", "untersuchung", demo).copyWihtoutReferences());
    root.getChildren()
            .add(manager.getEntryByRefID("labor", "untersuchung", demo).copyWihtoutReferences());
    root.getChildren()
            .add(manager.getEntryByRefID("diagnose", "untersuchung", demo).copyWihtoutReferences());
    root.getChildren().add(
            manager.getEntryByRefID("procedures", "untersuchung", demo).copyWihtoutReferences());
    root.getChildren().add(
            manager.getEntryByRefID("arztbriefe", "untersuchung", demo).copyWihtoutReferences());
    CatalogEntry e = manager.getEntryByRefID("diagnose", "untersuchung", demo);
    CatalogEntry entry = manager.getAllAncestorsAndSiblings(e, demo, CountType.distinctCaseID, 11);

    assertTrue(areTreesEqual(root, entry));
  }

  // prints entry hierarchy to console
  public void print(CatalogEntry entry, String space) {
    System.out.println(space + entry.getName() + " " + entry.getCountDistinctCaseID());
    for (CatalogEntry e : entry.getChildren()) {
      print(e, space + "  ");
    }
  }

  // tests if to entries with tree structure are equal
  private boolean areTreesEqual(CatalogEntry expected, CatalogEntry actual) {
    System.out.println("expected:");
    System.out.println(expected.toStringTreeVerbose());
    System.out.println("actual:");
    System.out.println(actual.toStringTreeVerbose());
    boolean equal = expected.getUniqueName().equals(actual.getUniqueName());
    equal = equal && expected.getChildren().size() == actual.getChildren().size();
    if (!equal) {
      return false;
    } else {
      expected.getChildren().sort(getComparator());
      actual.getChildren().sort(getComparator());
      for (int i = 0; i < expected.getChildren().size(); i++) {
        if (!areTreesEqual(expected.getChildren().get(i), actual.getChildren().get(i))) {
          return false;
        }
      }
      return true;
    }
  }

  // returns comparator for catalogentries
  private Comparator<CatalogEntry> getComparator() {
    return new Comparator<CatalogEntry>() {
      @Override
      public int compare(CatalogEntry o1, CatalogEntry o2) {
        return o1.getAttrId() - o2.getAttrId();
      }
    };
  }
}
