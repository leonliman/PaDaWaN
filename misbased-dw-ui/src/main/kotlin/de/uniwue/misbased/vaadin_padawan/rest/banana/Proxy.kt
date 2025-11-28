package de.uniwue.misbased.vaadin_padawan.rest.banana

import de.uniwue.dw.solr.api.DWSolrConfig
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import jakarta.servlet.http.HttpServletRequest
import org.apache.commons.codec.EncoderException
import org.apache.commons.io.IOUtils
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.impl.client.HttpClients
import java.io.IOException

object Proxy {

    private var solrURL = ""

    private var password: String

    private var user: String

    init {
        PaDaWaNConnector.getCatalogManager()
        user = DWSolrConfig.getSolrUser()
        password = DWSolrConfig.getSolrPassword()
        solrURL = DWSolrConfig.getSolrServerUrl()
        if (!solrURL.endsWith("/"))
            solrURL += "/"
    }

    @Throws(IOException::class, ClientProtocolException::class, EncoderException::class)
    fun requestGET(subResources: String, query: String?): ByteArray {
        val client = HttpClients.createDefault()
        var url = solrURL + subResources
        if (query != null && !query.isEmpty()) {
            url = "$url?$query"
        }

        val request = HttpGet(url)
        val response = client.execute(request)
        val `is` = response.entity.content
        return IOUtils.toByteArray(`is`)
    }

    @Throws(IOException::class)
    fun requestPOST(subResources: String, body: String?, request: HttpServletRequest): ByteArray {
        val provider = BasicCredentialsProvider()
        val credentials = UsernamePasswordCredentials(user, password)
        provider.setCredentials(AuthScope.ANY, credentials)

        val httpclient = HttpClientBuilder.create().setDefaultCredentialsProvider(provider)
            .build()

        val url = solrURL + subResources
        val httpPost = HttpPost(url)
        if (body != null && !body.isEmpty()) {
            val entity = ByteArrayEntity(body.toByteArray(charset("UTF-8")))
            httpPost.entity = entity
        }

        for (headerName in request.headerNames) {
            if (!headerName.isEmpty()) {
                val value = request.getHeader(headerName)
                if (!headerName.equals("content-length", ignoreCase = true))
                    httpPost.setHeader(headerName, value)
            }
        }

        val response = httpclient.execute(httpPost)
        val `is` = response.entity.content
        return IOUtils.toByteArray(`is`)
    }
}