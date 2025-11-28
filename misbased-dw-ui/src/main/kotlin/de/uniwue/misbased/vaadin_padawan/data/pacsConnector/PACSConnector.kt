package de.uniwue.misbased.vaadin_padawan.data.pacsConnector

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration
import de.uniwue.dw.core.client.authentication.User
import de.uniwue.dw.core.model.data.CatalogEntry
import de.uniwue.dw.query.model.lang.DisplayStringVisitor
import de.uniwue.dw.query.model.lang.QueryAttribute
import de.uniwue.dw.query.model.lang.QueryRoot
import de.uniwue.dw.query.model.result.Result
import de.uniwue.misbased.vaadin_padawan.data.PARAM_PACS_CATALOG_ENTRY_EXTERNAL_ID
import de.uniwue.misbased.vaadin_padawan.data.PARAM_PACS_CATALOG_ENTRY_PROJECT
import de.uniwue.misbased.vaadin_padawan.data.PARAM_PACS_PASSWORD
import de.uniwue.misbased.vaadin_padawan.data.PARAM_PACS_URL
import de.uniwue.misbased.vaadin_padawan.data.model.PACSModality
import de.uniwue.misbased.vaadin_padawan.data.model.PACSStatus
import de.uniwue.misbased.vaadin_padawan.data.model.PACSUserSettings
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import org.slf4j.LoggerFactory
import java.net.URI
import java.nio.file.Files

object PACSConnector {

    private val logger = LoggerFactory.getLogger(PACSConnector::class.java)

    private const val ID_TYPE_URL_APPENDIX = "&idtype=ACCESSIONNR"

    var latestPACSStatus: PACSStatus? = null

    private val sessionKeysForUsers: MutableMap<String, MutableSet<String>> = mutableMapOf()
    private val settingsStore: MutableMap<String, PACSUserSettings> = mutableMapOf()
    private val dicomModalitiesStore: Pair<List<PACSModality>, Set<PACSModality>> = getDicomModalities()

    private fun getDicomModalities(): Pair<List<PACSModality>, Set<PACSModality>> {
        val allModalities = mutableListOf<PACSModality>()
        val preselectedModalities = mutableSetOf<PACSModality>()
        val modalitiesFromFile = Files.readAllLines(PaDaWaNConnector.getResourceFile("dicom-modalities.txt").toPath())
        for (aModalityLine in modalitiesFromFile) {
            val lineSplit = aModalityLine.split("#")
            val shortText = lineSplit[0]
            val preselect = lineSplit.size > 1 && lineSplit[1] == "preselect"
            allModalities.add(PACSModality(shortText))
            if (preselect)
                preselectedModalities.add(PACSModality(shortText))
        }
        return Pair(allModalities.sortedBy { it.shortText }, preselectedModalities)
    }

    private fun getPACSUrl(): String? {
        return DwClientConfiguration.getInstance().getParameter(PARAM_PACS_URL)
    }

    private fun getPACSPassword(): String? {
        return DwClientConfiguration.getInstance().getParameter(PARAM_PACS_PASSWORD)
    }

    private fun getPACSCatalogEntry(): CatalogEntry? {
        val project = DwClientConfiguration.getInstance().getParameter(PARAM_PACS_CATALOG_ENTRY_PROJECT)
        val extID = DwClientConfiguration.getInstance().getParameter(PARAM_PACS_CATALOG_ENTRY_EXTERNAL_ID)
        if (project == null || extID == null)
            return null
        return try {
            PaDaWaNConnector.getCatalogManager().getEntryByRefID(extID, project, false)
        } catch (e: Exception) {
            e.printStackTrace()
            null
        }

    }

    fun getPACSStatus(user: User): PACSStatus {
        val result = if (getPACSUrl() == null || getPACSPassword() == null || getPACSCatalogEntry() == null)
            PACSStatus.CONFIGURATION_ERROR
        else {
            try {
                getSessionKey(user.username, listOf("2112"))
                PACSStatus.ONLINE
            } catch (e: Exception) {
                e.printStackTrace()
                PACSStatus.OFFLINE
            }
        }
        latestPACSStatus = result
        return result
    }

    fun getPACSSettings(user: User): PACSUserSettings {
        if (!settingsStore.containsKey(user.username))
            settingsStore[user.username] =
                PACSUserSettings(dicomModalitiesStore.first, dicomModalitiesStore.second.toMutableSet())
        return settingsStore[user.username]!!
    }

    fun savePACSSettings(user: User, settings: PACSUserSettings) {
        settingsStore[user.username] = settings
    }

    fun getDownloadAllURL(user: User, query: QueryRoot): Pair<Boolean, Any> {
        val pacsUserSettings = getPACSSettings(user)
        query.limitResult = 0

        val queryRunner = PaDaWaNConnector.getGUIClient().queryRunner
        val queryID = queryRunner.createQuery(query, user)
        val result = queryRunner.runQueryBlocking(queryID)

        if (!resultContainsPACSColumn(result))
            return Pair(false, "web-padawan.pacs.error.noAccessionNumberEntry")

        var sessionKey: String? = null
        val accessionNumbers = mutableListOf<String>()
        if (latestPACSStatus == PACSStatus.ONLINE) {
            val resultAndAccessionNumbers = getSessionKeyAndAccessionNumbersForQueryResult(result, user)
            sessionKey = resultAndAccessionNumbers.first
            accessionNumbers.addAll(resultAndAccessionNumbers.second)
        } else
            return Pair(false, "web-padawan.pacs.error.offline")

        if (sessionKey == null)
            return Pair(false, "web-padawan.pacs.error.sessionKey")

        val resultURL = getDownloadURL(user.username, accessionNumbers, sessionKey, pacsUserSettings)
        return Pair(true, PACSDownloadAllData(sessionKey, accessionNumbers, resultURL))
    }

    data class PACSDownloadAllData(val sessionKey: String, val accessionNumbers: List<String>, val url: String)

    private fun resultContainsPACSColumn(result: Result): Boolean {
        val pacsColumnHeader = getPACSColumnHeader()
        for (aHeader in result.header)
            if (aHeader == pacsColumnHeader)
                return true
        return false
    }

    fun getSessionKeyAndAccessionNumbersForQueryResult(result: Result, user: User): Pair<String?, List<String>> {
        val accessionNumbers = mutableListOf<String>()
        if (latestPACSStatus != PACSStatus.ONLINE || !resultContainsPACSColumn(result))
            return Pair(null, emptyList())
        val pacsColumnHeader = getPACSColumnHeader()
        for (aRow in result.rows)
            for (aCell in aRow.cells)
                if (aCell.columnName == pacsColumnHeader)
                    accessionNumbers.addAll(aCell.valueAsString.split(" | "))
        try {
            return Pair(getSessionKey(user.username, accessionNumbers), accessionNumbers)
        } catch (e: Exception) {
            e.printStackTrace()
            return Pair(null, emptyList())
        }
    }

    fun getPACSColumnHeader() = QueryAttribute(QueryRoot(), getPACSCatalogEntry()).accept(DisplayStringVisitor())!!

    fun performPACSDownload(url: String): Pair<Boolean, String?> {
        return try {
            val response = URI(url).toURL().readText()
            Pair(true, response)
        } catch (e: Exception) {
            e.printStackTrace()
            Pair(false, e.message)
        }
    }

    private fun getSessionKey(userName: String, accessionNumbers: List<String>): String {
        if (!sessionKeysForUsers.containsKey(userName))
            sessionKeysForUsers[userName] = mutableSetOf<String>()
        var url = "${getBaseURL("GENSKEY")}&pass=${getPACSPassword()}$ID_TYPE_URL_APPENDIX&id="
        url += accessionNumbers.joinToString(separator = ",")
        val sessionKey = URI(url).toURL().readText()
        sessionKeysForUsers[userName]!!.add(sessionKey)
        logger.trace(
            "Added session-key $sessionKey for user $userName and the following accession numbers: " +
                    accessionNumbers.joinToString(separator = ",")
        )
        return sessionKey
    }

    fun removeSessionKeys(userName: String) {
        if (!sessionKeysForUsers[userName].isNullOrEmpty()) {
            val url = "${getBaseURL("DELSKEY")}&skey="
            for (aSessionKey in sessionKeysForUsers[userName]!!) {
                try {
                    URI(url + aSessionKey).toURL().readText()
                    logger.trace("Successfully deleted session-key $aSessionKey for user $userName")
                } catch (e: Exception) {
                    logger.trace("Error while deleting session-key $aSessionKey for user $userName", e)
                }
            }
            sessionKeysForUsers.remove(userName)
        }
    }

    fun getDownloadURL(
        userName: String,
        accessionNumbers: List<String>,
        sessionKey: String,
        pacsUserSettings: PACSUserSettings
    ): String {
        var url = "${getBaseURL("IMAGE")}&desttype=DIR&skey=$sessionKey$ID_TYPE_URL_APPENDIX" +
                "${pacsUserSettings.getExportTypeURLAppendix()}${pacsUserSettings.getModalitiesURLAppendix()}&id="
        url += accessionNumbers.joinToString(separator = ",")
        url += "&destusername=$userName"
        return url
    }

    fun getViewURL(
        accessionNumbers: List<String>,
        sessionKey: String,
        pacsUserSettings: PACSUserSettings
    ): String {
        var url = "${getBaseURL("DCMWEBVIEWER")}&skey=$sessionKey$ID_TYPE_URL_APPENDIX" +
                "${pacsUserSettings.getModalitiesURLAppendix()}&id="
        url += accessionNumbers.joinToString(separator = ",")
        return url
    }

    private fun getBaseURL(taskName: String): String {
        return "${getPACSUrl()}?task=$taskName"
    }
}