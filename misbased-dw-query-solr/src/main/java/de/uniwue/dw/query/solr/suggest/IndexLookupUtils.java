package de.uniwue.dw.query.solr.suggest;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.data.CatalogEntryType;
import de.uniwue.dw.core.model.manager.DataSourceException;
import de.uniwue.dw.core.model.manager.ICatalogAndTextSuggester;
import de.uniwue.dw.query.solr.SolrUtil;
import de.uniwue.dw.query.solr.client.SolrManager;
import de.uniwue.dw.query.solr.model.manager.CatalogFilter;
import de.uniwue.dw.query.solr.model.manager.SolrCatalogClientManager;
import de.uniwue.dw.solr.api.DWSolrConfig;
import de.uniwue.misc.util.ConfigException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.TermsResponse;
import org.apache.solr.client.solrj.response.TermsResponse.Term;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class IndexLookupUtils implements ICatalogAndTextSuggester {

  private static final Logger logger = LogManager.getLogger(IndexLookupUtils.class);

  private SolrManager solrManager;

  private HashMap<String, List<String>> query2terms = new HashMap<>();

  public IndexLookupUtils(SolrManager solrManager) {
    this.solrManager = solrManager;
  }

  @Override
  public List<String> suggestTextTokens(String token, int numberOfSuggestions) {
    return suggestTextTokens(token, null, numberOfSuggestions);
    // Not usable for a generic installation
    // SolrQuery query = new SolrQuery(token.toLowerCase());
    // query.setRequestHandler("/suggest");
    // QueryResponse response;
    // try {
    // response = solrManager.getServer().query(query);
    // SpellCheckResponse spellCheckResponse = response.getSpellCheckResponse();
    // if (spellCheckResponse != null) {
    // if (!spellCheckResponse.getSuggestions().isEmpty()) {
    // return spellCheckResponse.getSuggestions().get(0).getAlternatives();
    // }
    // }
    // } catch (SolrServerException e) {
    // e.printStackTrace();
    // }
    // return new ArrayList<>();
  }

  @Override
  public List<CatalogEntry> suggestCatalogEntries(String token, User user,
          CatalogEntryType dataType, int numberOfSuggestions) {
    return SolrCatalogClientManager.getInst().getEntriesByWordAndDataTypeFilter(token, user,
            dataType, numberOfSuggestions);
  }

  @Override
  public List<CatalogEntry> suggestCatalogEntries(String token, User user,
          int numberOfSuggestions) {
    return suggestCatalogEntries(token, user, null, numberOfSuggestions);
  }

  @Override
  public CatalogEntry getCatalogEntryByName(String name, String domain, User user)
          throws DataSourceException {
    CatalogFilter filter;
    try {
      filter = new CatalogFilter().setNamePrefix(name).setProject(domain).setUser(user);
      Optional<CatalogEntry> firstEntries = SolrCatalogClientManager.getInst()
              .getFirstEntries(filter);
      return firstEntries
              .orElseThrow(() -> buildExecption("name", name, "domain", domain, "user", user));
    } catch (Exception e) {
      throw new DataSourceException(e);
    }
  }

  private DataSourceException buildExecption(Object... os) {
    List<String> strings = new ArrayList<>();
    if (os.length % 2 == 0) {
      for (int i = 0; i <= os.length - 1; i += 2) {
        strings.add(os[i] + "=" + os[i + 1]);
      }
      String arguments = strings.stream().collect(Collectors.joining(", "));
      String message = "CatalogEntry does not exist with: " + arguments;
      return new DataSourceException(message);
    } else
      return new DataSourceException("Requested contend not available");

  }

  @Override
  public CatalogEntry getCatalogEntryByName(String name, User user) throws DataSourceException {
    return getCatalogEntryByName(name, null, user);
  }

  @Override
  public CatalogEntry getCatalogEntryByExtid(String extId, String project, User user)
          throws DataSourceException {
    return SolrCatalogClientManager.getInst().getEntryByRefID(extId, project, user);
  }

  @Override
  public CatalogEntry getCatalogEntryByUniqueName(String uniqueName, User user)
          throws DataSourceException {
    CatalogFilter filter = new CatalogFilter().setUniqueName(uniqueName).setUser(user);
    try {
      Optional<CatalogEntry> entry = SolrCatalogClientManager.getInst().getFirstEntries(filter);
      return entry.orElseThrow(() -> buildExecption("uniqueName", uniqueName, "user", user));
    } catch (SolrServerException | IOException e) {
      throw new DataSourceException(e);
    }
    // int attrID = getAttrIDByUniqueName(uniqueName, user);
    // if (attrID != -1) {
    // try {
    // return DwClientConfiguration.getInstance().getCatalogManager().getEntryByID(attrID);
    // } catch (SQLException e) {
    // e.printStackTrace();
    // }
    // }
    // return null;
  }

  public static void main(String[] args) {
    String s = "Aufnahme-Datum";
    System.out.println(s.toLowerCase());
  }

  @Override
  public CatalogEntry getCatalogEntryByNameOrUniqueName(String name, User user)
          throws DataSourceException {
    try {
      return SolrCatalogClientManager.getInst().getCatalogEntryByNameOrUniqueName(name, user);
    } catch (SolrServerException | IOException e) {
      throw new DataSourceException(e);
    }

  }

  @Override
  public List<String> suggestTextTokens(String token, CatalogEntry catalogEntry,
          int numberOfTokens) {
    if (token.isEmpty())
      return getMostFrequentToken(catalogEntry, numberOfTokens);
    else
      return suggestTextTokensImpl(token, catalogEntry, numberOfTokens);
  }

  private List<String> suggestTextTokensImpl(String token, CatalogEntry catalogEntry,
          int numberOfTokens) {
    try {
      if (catalogEntry == null)
        catalogEntry = getDefaultSuggestComponentCatalogEntry();
      if (token == null)
        token = "";
      // see method comment
      // if (!token.isEmpty() && catalogEntry.equals(getDefaultSuggestComponentCatalogEntry()))
      // return suggestTextTokens(token, numberOfTokens);

      String solrField = SolrUtil.getSolrFieldName(catalogEntry);

      SolrQuery query = new SolrQuery();
      query.setRequestHandler("/terms");
      query.addTermsField(solrField);
      if (!token.isEmpty())
        query.setTermsPrefix(token);
      String queryString = query.toString();
      List<String> cashResult = query2terms.get(queryString);
      if (cashResult != null)
        return cashResult.stream().limit(numberOfTokens).collect(Collectors.toList());
      QueryResponse response;
      query.setTermsLimit(numberOfTokens);
      response = solrManager.getServer().query(query, DWSolrConfig.getSolrMethodToUse());
      TermsResponse termsResponse = response.getTermsResponse();
      List<Term> terms = termsResponse.getTerms(solrField);
      if (!terms.isEmpty()) {
        List<String> termList = terms.stream().map(Term::getTerm).collect(Collectors.toList());
        query2terms.put(queryString, termList);
        return termList;

      }
    } catch (SolrServerException | ConfigException | IOException e) {
      e.printStackTrace();
    }
    return new ArrayList<>();

  }

  private List<String> getMostFrequentToken(CatalogEntry catalogEntry, int numberOfTokens) {
    return suggestTextTokensImpl("", catalogEntry, numberOfTokens);
  }

  @Override
  public List<CatalogEntry> getMostCommonTextFields(User user) throws ConfigException {
    return DwClientConfiguration.getInstance().getSpecialCatalogEntries().getMostCommonTextFields();
  }

  public CatalogEntry getDischargeLetter() throws ConfigException {
    return DwClientConfiguration.getInstance().getSpecialCatalogEntries().getMostCommonTextFields()
            .get(0);
  }

  public CatalogEntry getDefaultSuggestComponentCatalogEntry() throws ConfigException {
    return DwClientConfiguration.getInstance().getSpecialCatalogEntries().getSuggester();
  }

  @Override
  public CatalogEntry getDocumentIDCatalogEntry() throws ConfigException {
    return DwClientConfiguration.getInstance().getSpecialCatalogEntries().getDocumentID();
  }

  @Override
  public CatalogEntry getDocumentGroupIDCatalogEntry() throws ConfigException {
    return DwClientConfiguration.getInstance().getSpecialCatalogEntries().getDocumentGroupId();
  }

  @Override
  public CatalogEntry getDocumentTimeCatalogEntry() throws ConfigException {
    return DwClientConfiguration.getInstance().getSpecialCatalogEntries().getDocumentTime();
  }

  @Override
  public List<CatalogEntry> getCatalogEntrySons(CatalogEntry entry, User user,
          int numberOfCatalogSuggestions) {
    CatalogFilter filter = new CatalogFilter().setParentID(entry.getAttrId()).setUser(user)
            .setLimitResuls(numberOfCatalogSuggestions);
    try {
      return SolrCatalogClientManager.getInst().getEntries(filter);
    } catch (SolrServerException | IOException e) {
      e.printStackTrace();
      logger.debug(e);
    }
    return new ArrayList<>();
  }

}
