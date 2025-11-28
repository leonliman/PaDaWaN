package de.uniwue.dw.query.model.client;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.core.client.authentication.group.Group;
import de.uniwue.dw.query.model.exception.QueryException;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.result.Result;

public class QueryCache {

  private static final int RESULT_CASH_CAPACITY = 100;

  private static final int NUM_FOUND_QUERY_CASH_CAPACITY = 1000;

  private static final QueryCache instance = new QueryCache();

  private static final Logger logger = LogManager.getLogger(QueryCache.class);

  private static final String EMPTY_LIST_TOKEN = "__empty group list__";

  private LinkedHashMap<Integer, Result> queryRootID2Result = new LinkedHashMap<Integer, Result>() {

    private static final long serialVersionUID = 1L;

    protected boolean removeEldestEntry(Map.Entry<Integer, Result> eldest) {
      return size() > RESULT_CASH_CAPACITY;
    }

  };

  private LinkedHashMap<String, Long> queryString2NumFound = new LinkedHashMap<String, Long>() {

    private static final long serialVersionUID = 1L;

    protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
      return size() > NUM_FOUND_QUERY_CASH_CAPACITY;
    }

  };

  private QueryCache() {
  }

  public static QueryCache getInstance() {
    return instance;
  }

  public void clear() {
    queryRootID2Result.clear();
  }

  public boolean contains(QueryRoot query) {
    return contains(query, null);
  }

  public boolean contains(QueryRoot query, List<Group> groups) {
    if (query != null) {
      try {
        int id = query2id(query, groups);
        return contains(id);
      } catch (QueryException e) {
        logger.debug(e);
      }
    }
    return false;
  }

  private static final class GroupComperator implements Comparator<Group> {

    @Override
    public int compare(Group g1, Group g2) {
      return g1.getId() - g2.getId();
    }

  }

  private static GroupComperator groupComperator = new GroupComperator();

  private static String groupList2String(List<Group> groups) {
    if (groups.isEmpty())
      return EMPTY_LIST_TOKEN;
    Collections.sort(groups, groupComperator);
    String groupAsString = groups.stream().map(g -> g.getId() + "")
            .collect(Collectors.joining(" "));
    return groupAsString;
  }

  public boolean contains(int id) {
    return queryRootID2Result.containsKey(id);
  }

  public boolean put(QueryRoot query, Result result) {
    return put(query, null, result);
  }

  public boolean put(QueryRoot query, List<Group> groups, Result result) {
    if (query != null && result != null) {
      try {
        int id = query2id(query, groups);
        return put(id, result);
      } catch (QueryException e) {
        logger.debug(e);
      }
    }
    return false;
  }

  public boolean put(int id, Result result) {
    logger.debug("adding query to cash with id" + id);
    return queryRootID2Result.put(id, result) != null;
  }

  public static int query2id(QueryRoot query) throws QueryException {
    return query2id(query, null);
  }

  public static int query2id(QueryRoot query, List<Group> groups) throws QueryException {
    String xml = query.toXML();
    String groupsString = "";
    if (groups != null)
      groupsString = groupList2String(groups);
    String stringId = xml + groupsString;
    int id = stringId.hashCode();
    return id;
  }

  public Result get(QueryRoot query) {
    return get(query, null);
  }

  public Result get(QueryRoot query, List<Group> groups) {
    try {
      int id = query2id(query, groups);
      return get(id);
    } catch (QueryException e) {
      logger.debug(e);
    }
    return null;
  }

  public Long getNumFound(String queryString) {
    logger.debug("getNumFound for queryString " + queryString);
    return queryString2NumFound.get(queryString);
  }

  public Result get(int id) {
    logger.debug("get result for query id " + id);
    return queryRootID2Result.get(id);
  }

  public static void main(String[] args) {
    QueryCache.getInstance().put(1, null);
    QueryCache.getInstance().put(2, null);
    QueryCache.getInstance().put(3, null);
    QueryCache.getInstance().put(4, null);
    QueryCache.getInstance().put(5, null);
    QueryCache.getInstance().put(6, null);
    Set<Integer> keySet = instance.queryRootID2Result.keySet();
    System.out.println(keySet);
  }

}
