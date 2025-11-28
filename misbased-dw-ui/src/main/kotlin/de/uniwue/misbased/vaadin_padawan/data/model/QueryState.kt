package de.uniwue.misbased.vaadin_padawan.data.model

data class QueryState(
    val queryID: Int = -1,
    var queryState: QueryExecutionState = QueryExecutionState.RUNNING,
    var progress: Double = 0.0,
    val nextStatusQueryIntervalInMS: Int = 100,
    var errorMessage: String? = null,
    var errorMessageTranslationKey: String? = null,
    var statusMessage: String? = null
)

enum class QueryExecutionState {
    RUNNING, FINISHED_SUCCESSFULLY, ERROR, CANCELLED
}
