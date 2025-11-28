package de.uniwue.dw.query.solr.client;

import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthState;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.SolrPing;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;

import java.io.IOException;

public class SolrManager {

  private static final Logger logger = LogManager.getLogger(SolrManager.class);

  private String user, password, serverURL, patientLayerServerURL;

  private HttpSolrClient server;

  private HttpSolrClient patientLayerServer;

  public SolrManager(String user, String password, String serverURL) {
    this(user, password, serverURL, null);
  }

  public SolrManager(String user, String password, String serverURL, String patientLayerServerURL) {
    this.user = user;
    this.password = password;
    this.serverURL = serverURL;
    this.patientLayerServerURL = patientLayerServerURL;
    initServer();
  }

  protected void initServer() {
    int timeout = 86400000; // = one day in millis; needed for index optimization which could take multiple hours
    if ((user != null && password != null) && !(user.isEmpty() && password.isEmpty())) {
      server = new HttpSolrClient.Builder(serverURL).withHttpClient(getHttpClient())
              .withSocketTimeout(timeout).withConnectionTimeout(timeout).build();
      if (patientLayerServerURL != null) {
        patientLayerServer = new HttpSolrClient.Builder(patientLayerServerURL).withHttpClient(getHttpClient())
                .withSocketTimeout(timeout).withConnectionTimeout(timeout).build();
      }
    } else {
      server = new HttpSolrClient.Builder(serverURL).withSocketTimeout(timeout).withConnectionTimeout(timeout).build();
      if (patientLayerServerURL != null) {
        patientLayerServer = new HttpSolrClient.Builder(patientLayerServerURL).withSocketTimeout(timeout)
                .withConnectionTimeout(timeout).build();
      }
    }
    testConnection();
  }

  private HttpClient getHttpClient() {
    CredentialsProvider provider = new BasicCredentialsProvider();
    UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(this.user,
            this.password);
    provider.setCredentials(AuthScope.ANY, credentials);
    return HttpClientBuilder.create().setDefaultCredentialsProvider(provider)
            .addInterceptorFirst(new PreemptiveAuthInterceptor()).build();
  }

  private void testConnection() {
    try {
      doPing(serverURL, server);
      if (patientLayerServerURL != null) {
        doPing(patientLayerServerURL, patientLayerServer);
      }
    } catch (SolrServerException | IOException e) {
      e.printStackTrace();
    }
  }

  private void doPing(String currentServerURL, HttpSolrClient currentServer) throws SolrServerException, IOException {
    logger.debug("Pinging server at " + currentServerURL + " ...");
    SolrPing ping = new SolrPing();
    SolrPingResponse pingResp = ping.process(currentServer);
    logger.debug("ping response time " + pingResp.getQTime());
    logger.info("Connection with Solr-Server at " + currentServerURL + " established");
  }

  @Deprecated
  public void shutdown() {
    logger.warn("Not supported.");
  }

  public static void printQueryResponse(QueryResponse rsp) {
    System.out.println("hits: " + rsp.getResults().getNumFound());
    System.out.println(rsp);
  }

  public SolrClient getServer() {
    if (server == null)
      initServer();
    return server;
  }

  public SolrClient getPatientLayerServer() {
    if (patientLayerServer == null && patientLayerServerURL != null)
      initServer();
    return patientLayerServer;
  }

  public String getServerURL() {
    return serverURL;
  }

  public String getPatientLayerServerURL() {
    return patientLayerServerURL;
  }

  private static class PreemptiveAuthInterceptor implements HttpRequestInterceptor {
    @Override
    public void process(final HttpRequest request, final HttpContext context) throws HttpException {
      final AuthState authState = (AuthState) context
              .getAttribute(HttpClientContext.TARGET_AUTH_STATE);

      if (authState.getAuthScheme() == null) {
        final CredentialsProvider credsProvider = (CredentialsProvider) context
                .getAttribute(HttpClientContext.CREDS_PROVIDER);
        final HttpHost targetHost = (HttpHost) context
                .getAttribute(HttpCoreContext.HTTP_TARGET_HOST);
        final Credentials creds = credsProvider
                .getCredentials(new AuthScope(targetHost.getHostName(), targetHost.getPort()));
        if (creds == null) {
          throw new HttpException("No creds provided for preemptive auth.");
        }
        authState.update(new BasicScheme(), creds);
      }
    }

  }
}
