package de.uniwue.misbased.vaadin_padawan.data.padawanConnector

import de.uniwue.dw.core.client.authentication.User
import de.uniwue.dw.query.model.QueryReader
import de.uniwue.dw.query.model.lang.*
import de.uniwue.dw.query.model.result.Result
import de.uniwue.dw.query.model.result.export.ExportConfiguration
import de.uniwue.misbased.vaadin_padawan.data.DEFAULT_DISABLE_QUERY_SAVING
import de.uniwue.misbased.vaadin_padawan.data.PARAM_DISABLE_QUERY_SAVING
import de.uniwue.misbased.vaadin_padawan.data.model.QueryExecutionState
import de.uniwue.misbased.vaadin_padawan.data.model.QueryState
import de.uniwue.misbased.vaadin_padawan.data.model.SavedQuery
import de.uniwue.misbased.vaadin_padawan.data.model.SavedQueryType

object PaDaWaNQueryConnector {

    private val userNameToQueryIDsMap: MutableMap<String, MutableSet<Int>> = mutableMapOf()

    const val CORRELATION_ANALYSIS_ATTRIBUTE_PREFIX = "#N4JCorr#"
    private const val CORRELATION_ANALYSIS_QUERY_PREFIX = "#CorrelationAnalysis#"

    fun doQueryCancellation(queryID: Int): QueryExecutionState {
        val progress = PaDaWaNConnector.getGUIClient().queryRunner.getProgress(queryID)
        return when {
            progress.isDone -> QueryExecutionState.FINISHED_SUCCESSFULLY
            progress.queryStoppedWithErrors() -> QueryExecutionState.ERROR
            else -> {
                PaDaWaNConnector.getGUIClient().queryRunner.cancelQuery(queryID)
                QueryExecutionState.CANCELLED
            }
        }
    }

    fun saveQuery(
        query: QueryRoot,
        queryName: String,
        user: User,
        overwriteExistingEntry: Boolean = false
    ): GUIQuerySaveResult {
        if (querySavingIsDisabled())
            return GUIQuerySaveResult.SAVING_DISABLED

        val client = PaDaWaNConnector.getGUIClient()
        val queryClientIOManager = client.queryClientIOManager

        var queryNameToUse = queryName
        while (queryNameToUse.startsWith("/"))
            queryNameToUse = queryNameToUse.substring(1)
        if (queryNameToUse.startsWith("${user.username}/"))
            queryNameToUse = queryNameToUse.substring(user.username.length + 1)
        while (queryNameToUse.startsWith("/"))
            queryNameToUse = queryNameToUse.substring(1)

        val queryNameForDatabase = "${user.username}/$queryNameToUse"
        val rawQuery = queryClientIOManager.getQuery(queryNameForDatabase)
        if (rawQuery != null) {
            return if (overwriteExistingEntry) {
                queryClientIOManager.updateQuery(queryNameForDatabase, query.generateXML(), user.username)
                GUIQuerySaveResult.SUCCESSFUL
            } else
                GUIQuerySaveResult.ALREADY_EXISTS
        }

        queryClientIOManager.saveQuery(queryNameToUse, query.generateXML(), "", user.username)
        return GUIQuerySaveResult.SUCCESSFUL
    }

    enum class GUIQuerySaveResult {
        SUCCESSFUL, ALREADY_EXISTS, SAVING_DISABLED
    }

    fun querySavingIsDisabled(): Boolean {
        val dwClientConfiguration = PaDaWaNConnector.getDWClientConfiguration()
        return dwClientConfiguration.getBooleanParameter(PARAM_DISABLE_QUERY_SAVING, DEFAULT_DISABLE_QUERY_SAVING)
    }

    fun loadQueryTree(user: User): Pair<List<SavedQuery>, SavedQuery?> {
        val client = PaDaWaNConnector.getGUIClient()

        val storedQueryTreeForUser = client.queryClientIOManager.getStoredQueryTreeForUser(user)
        val result = SavedQuery(
            storedQueryTreeForUser,
            user.username,
            !user.isAllowedToUseCaseQuery && !user.isAdmin,
            SavedQueryType.FOLDER
        ).children
        val rootForCurrentUser = result.find { it.isRootForCurrentUser }
        return Pair(result, rootForCurrentUser)
    }

    fun loadSingleQuery(path: String, user: User): QueryRoot {
        val client = PaDaWaNConnector.getGUIClient()
        val storedQuery = client.queryClientIOManager.getStoredQueryForUser(path, user)
        require(storedQuery.isPresent && !storedQuery.get().isStructure) {
            "There exists no saved query with the provided path for the current user"
        }
        return QueryReader.read(storedQuery.get().query.xml)
    }

    fun startQuery(query: QueryRoot, user: User, exportConfiguration: ExportConfiguration): QueryState {
        try {
            if (!query.isStatisticQuery && !user.isAllowedToUseCaseQuery && !user.isAdmin)
                return QueryState(
                    queryState = QueryExecutionState.ERROR,
                    errorMessageTranslationKey = "web-padawan.errorDialog.userMayOnlyRunStatisticQueries"
                )

            checkQueryUsableForCorrelationAnalysis(query)

            val queryRunner = PaDaWaNConnector.getGUIClient().queryRunner
            val queryID = queryRunner.createQuery(query, user, exportConfiguration)
            queryRunner.runQuery(queryID)

            if (!userNameToQueryIDsMap.containsKey(user.username))
                userNameToQueryIDsMap[user.username] = mutableSetOf()
            userNameToQueryIDsMap[user.username]!!.add(queryID)

            return QueryState(queryID = queryID)
        } catch (e: Exception) {
            e.printStackTrace()
            return QueryState(
                queryState = QueryExecutionState.ERROR,
                errorMessage = e.localizedMessage
            )
        }
    }

    private fun checkQueryUsableForCorrelationAnalysis(query: QueryRoot) {
        if (query.comment != null && query.comment.startsWith(CORRELATION_ANALYSIS_QUERY_PREFIX))
            query.comment = query.comment.substring(CORRELATION_ANALYSIS_QUERY_PREFIX.length)
        if (query.isStatisticQuery) {
            val queryRows = mutableListOf<QueryStructureElem>()
            val queryColumns = mutableListOf<QueryStructureElem>()
            for (sibling in query.siblings) {
                if (sibling is QueryStatisticRow)
                    queryRows.addAll(sibling.children)
                else if (sibling is QueryStatisticColumn)
                    queryColumns.addAll(sibling.children)
            }
            if (queryRows.isNotEmpty() && queryColumns.isNotEmpty()) {
                val rowAttributes = mutableListOf<QueryAttribute>()
                for (row in queryRows) {
                    rowAttributes.addAll(row.attributesRecursive)
                }
                if (rowAttributes.isEmpty() || !containsAttributeConfiguredForCorrelationAnalysis(rowAttributes))
                    return

                val columnAttributes = mutableListOf<QueryAttribute>()
                for (column in queryColumns) {
                    columnAttributes.addAll(column.attributesRecursive)
                }
                if (columnAttributes.isEmpty() ||
                    !containsAttributeConfiguredForCorrelationAnalysis(columnAttributes)
                )
                    return
                if (query.comment == null)
                    query.comment = CORRELATION_ANALYSIS_QUERY_PREFIX
                else
                    query.comment = "$CORRELATION_ANALYSIS_QUERY_PREFIX${query.comment}"
            }
        }
    }

    private fun containsAttributeConfiguredForCorrelationAnalysis(queryAttributes: List<QueryAttribute>): Boolean {
        for (queryAttribute in queryAttributes)
            if (queryAttribute.comment != null &&
                queryAttribute.comment.startsWith(CORRELATION_ANALYSIS_ATTRIBUTE_PREFIX)
            )
                return true
        return false
    }

    fun queryStatus(queryID: Int, user: User): QueryState {
        try {
            userNameToQueryIDsMap[user.username]?.contains(queryID)
                ?: return QueryState(
                    queryID = queryID,
                    queryState = QueryExecutionState.ERROR,
                    errorMessageTranslationKey = "web-padawan.errorDialog.queryStatusUnavailable"
                )

            val progress = PaDaWaNConnector.getGUIClient().queryRunner.getProgress(queryID)

            val queryState = QueryState(queryID = queryID)
            when {
                progress.queryStoppedWithErrors() -> {
                    queryState.queryState = QueryExecutionState.ERROR
                    queryState.errorMessage = progress.errorMessage
                }

                progress.isDone -> {
                    queryState.queryState = QueryExecutionState.FINISHED_SUCCESSFULLY
                    queryState.progress = 1.0
                }

                else -> queryState.progress = progress.getProgressPercentageAsFraction
            }

            return queryState
        } catch (e: Exception) {
            e.printStackTrace()
            return QueryState(
                queryID = queryID,
                queryState = QueryExecutionState.ERROR,
                errorMessage = e.localizedMessage
            )
        }
    }

    fun cancelQuery(queryID: Int, user: User): QueryState {
        try {
            userNameToQueryIDsMap[user.username]?.contains(queryID)
                ?: return QueryState(
                    queryID = queryID,
                    queryState = QueryExecutionState.ERROR,
                    errorMessageTranslationKey = "web-padawan.errorDialog.queryStatusUnavailable"
                )

            userNameToQueryIDsMap[user.username]!!.remove(queryID)

            val cancellationResultStatus = doQueryCancellation(queryID)
            return QueryState(
                queryID = queryID,
                queryState = cancellationResultStatus
            )
        } catch (e: Exception) {
            e.printStackTrace()
            return QueryState(
                queryID = queryID,
                queryState = QueryExecutionState.ERROR,
                errorMessage = e.localizedMessage
            )
        }
    }

    fun queryResult(queryID: Int, user: User): Result? {
        userNameToQueryIDsMap[user.username]?.contains(queryID)
            ?: return null

        return PaDaWaNConnector.getGUIClient().queryRunner.getResult(queryID)
    }
}