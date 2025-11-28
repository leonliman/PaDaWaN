package de.uniwue.misbased.vaadin_padawan.rest.banana

import de.uniwue.dw.core.client.authentication.User
import de.uniwue.dw.core.model.manager.DataSourceException
import de.uniwue.dw.core.model.manager.UnauthorizedException
import de.uniwue.dw.query.model.client.GUIClientException
import de.uniwue.dw.solr.api.DWSolrConfig
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import jakarta.servlet.http.HttpServletRequest
import org.apache.solr.client.solrj.SolrServerException
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.io.IOException
import java.sql.SQLException
import java.util.*
import java.util.regex.Pattern

@RestController
@RequestMapping("/rest/banana")
class Banana {

    companion object {
        const val SUB_RESOURCES_REGEX = "{*subResources}"
    }

    @GetMapping("/$SUB_RESOURCES_REGEX")
    fun get(@PathVariable(name = "subResources") subResources: String, request: HttpServletRequest): ByteArray {
        val query = request.queryString
        val user = PaDaWaNConnector.getUser()

        val rewrittenSubResources = rewrite(subResources)
        val result =
            filterAndHandleSpecialRequests(rewrittenSubResources, query, null, user, request.requestURL.toString())
        return result?.toByteArray() ?: Proxy.requestGET(rewrittenSubResources, query)
    }

    @PostMapping("/$SUB_RESOURCES_REGEX")
    fun post(
        @PathVariable(name = "subResources") subResources: String,
        @RequestBody body: String, request: HttpServletRequest
    ): ByteArray {
        val query = request.queryString
        val user = PaDaWaNConnector.getUser()

        val rewrittenSubResources = rewrite(subResources)
        val result =
            filterAndHandleSpecialRequests(rewrittenSubResources, query, body, user, request.requestURL.toString())
        return result?.toByteArray() ?: Proxy.requestPOST(rewrittenSubResources, body, request)
    }

    private fun rewrite(subResources: String) = subResources.substring(subResources.indexOf("/") + 1)

    /**
     * if this method returns null, the request can be proxied. A String is returned, if a special
     * resource is requested. The returned string is the response. An exception is thrown, if the
     * requested url is not allowed.
     *
     * @param subResources
     * @param query
     * @param body
     * @return
     * @throws SQLException
     * @throws SolrServerException
     * @throws IOException
     * @throws GUIClientException
     * @throws DataSourceException
     */
    @Throws(
        IOException::class, SolrServerException::class, SQLException::class, DataSourceException::class,
        GUIClientException::class
    )
    private fun filterAndHandleSpecialRequests(
        subResources: String, query: String?, body: String?, user: User, requestURL: String
    ): String? {
        val resources = subResources.lowercase(Locale.getDefault())

        if (resources.startsWith("banana-int/select")) {
            val attrIDString = extractFieldName(query)
            val attrID = Integer.valueOf(attrIDString!!)
            val catalogEntry = SecurityManager.getCatalogEntry(attrID, user)
            val serverURL = requestURL.substring(0, requestURL.indexOf("rest/banana"))
            return TemplateCreator.getDashboard(catalogEntry, "${serverURL}rest/banana")
        }

        if (resources.startsWith("banana"))
            return null
        else if (resources.contains("admin/luke"))
            return "{}"
        else if (resources.contains("admin/cores")) {
            val url = DWSolrConfig.getSolrServerUrl()
            val collection = url.substring(url.lastIndexOf("/"), url.length)
            return ("{\"defaultCoreName\":\"collection1\",\"initFailures\":{},\"status\":{\"banana-int\":{\"name\":\"banana-int\"},\""
                    + collection + "\":{\"name\":\"" + collection + "\"}}}")
        } else if (resources.contains("select")) {
            if (SecurityManager.userIsAllowedToDoSelectQuery(body!!))
                return null
            else
                UnauthorizedException()
        }
        throw UnauthorizedException()
    }

    private fun extractFieldName(query: String?): String? {
        if (query != null && !query.isEmpty()) {
            val regex = "q=title(%3A|:)%22(.*?)%22"
            val pattern = Pattern.compile(regex)
            val matcher = pattern.matcher(query)
            if (matcher.find()) {
                return matcher.group(2)
            }
        }
        return null
    }
}