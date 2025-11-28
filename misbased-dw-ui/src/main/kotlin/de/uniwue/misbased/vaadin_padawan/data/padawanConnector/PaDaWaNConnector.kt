package de.uniwue.misbased.vaadin_padawan.data.padawanConnector

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration
import de.uniwue.dw.core.client.authentication.User
import de.uniwue.dw.core.client.authentication.group.AuthManager
import de.uniwue.dw.core.model.manager.CatalogManager
import de.uniwue.dw.core.model.manager.ICatalogClientManager
import de.uniwue.dw.query.model.GUIClientFactory
import de.uniwue.dw.query.model.IQueryKeys.PARAM_HIT_HIGHLIGHT_STYLE
import de.uniwue.dw.query.model.client.IGUIClient
import de.uniwue.misbased.vaadin_padawan.data.pacsConnector.PACSConnector
import de.uniwue.misbased.vaadin_padawan.views.result.ResultTable
import org.springframework.security.authentication.AnonymousAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.stereotype.Component
import java.io.File
import java.io.IOException
import java.net.URLDecoder
import java.nio.charset.StandardCharsets

@Component
object PaDaWaNConnector {

    private lateinit var authManager: AuthManager
    private lateinit var userMap: MutableMap<String, User>
    private lateinit var client: IGUIClient
    private lateinit var ccm: ICatalogClientManager
    private lateinit var cm: CatalogManager

    fun setup(propertiesFileLocation: String) {
        require(propertiesFileLocation.isNotBlank()) {
            "No value for the property 'misbased.dw.properties-file-location' has been set."
        }
        DwClientConfiguration.loadProperties(
            File(propertiesFileLocation)
        )
        DwClientConfiguration.getInstance().setProperty(PARAM_HIT_HIGHLIGHT_STYLE, ResultTable.getTextHighlightStyle())

        try {
            val sqlType = DwClientConfiguration.getInstance().getParameter("sql.db_type")
            if (sqlType.equals("MySQL", true))
                Class.forName("com.mysql.cj.jdbc.Driver").getDeclaredConstructor().newInstance()
            else if (sqlType.equals("MSSQL", true))
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").getDeclaredConstructor().newInstance()
        } catch (e: Exception) {
            e.printStackTrace()
        }

        authManager = DwClientConfiguration.getInstance().authManager
        userMap = mutableMapOf()
        cm = DwClientConfiguration.getInstance().catalogManager
    }

    fun getResourceFile(filename: String): File {
        val classUrl =
            PaDaWaNExportConnector::class.java.getResource("/") ?: throw IOException("Could not find resources folder")

        var decodedUrl = URLDecoder.decode(classUrl.file, StandardCharsets.UTF_8)
        if (decodedUrl.startsWith("/")) {
            decodedUrl = decodedUrl.replaceFirst("/".toRegex(), "")
        }

        var resourceFile = File(decodedUrl, filename)
        if (!resourceFile.exists())
            resourceFile = File("/$decodedUrl", filename)
        return resourceFile
    }

    fun getDWClientConfiguration(): DwClientConfiguration {
        return DwClientConfiguration.getInstance()
    }

    fun getAuthManager(): AuthManager {
        return authManager
    }

    fun addAuthenticatedUser(username: String, user: User) {
        userMap[username] = user
    }

    fun removeAuthenticatedUser(username: String) {
        userMap.remove(username)
        PACSConnector.removeSessionKeys(username)
    }

    fun getUser(): User {
        val authentication = SecurityContextHolder.getContext().authentication
        if (authentication != null && authentication !is AnonymousAuthenticationToken) {
            val username = authentication.name
            if (!userMap.containsKey(username)) {
                val user = User(username, "", "", "")
                val groupNames = authentication.authorities.map { it.authority.substring(5) }
                val groups = authManager.getGroups(ArrayList(groupNames))
                user.groups = groups
                val userSettings = authManager.loadUserSettings(user)
                user.settings = userSettings
                userMap[username] = user
            }
            return userMap[username]!!
        }
        throw IllegalStateException("No user currently authenticated")
    }

    fun getGUIClient(): IGUIClient {
        if (!this::client.isInitialized)
            try {
                client = GUIClientFactory.getInstance().guiClient
            } catch (e: Exception) {
                e.printStackTrace()
            }
        return client
    }

    fun getCatalogClientManager(): ICatalogClientManager {
        if (!this::ccm.isInitialized)
            try {
                ccm = getGUIClient().catalogClientProvider
            } catch (e: Exception) {
                e.printStackTrace()
            }
        return ccm
    }

    fun getCatalogManager(): CatalogManager {
        return cm
    }

    fun dispose() {
        cm.dispose()
        if (this::ccm.isInitialized)
            ccm.dispose()
        if (this::client.isInitialized)
            client.dispose()
    }
}