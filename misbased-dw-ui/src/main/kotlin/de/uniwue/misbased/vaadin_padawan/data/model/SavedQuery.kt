package de.uniwue.misbased.vaadin_padawan.data.model

import de.uniwue.dw.query.model.data.StoredQueryTreeEntry

data class SavedQuery(
    val path: String,
    val name: String,
    val type: SavedQueryType,
    val children: MutableList<SavedQuery> = mutableListOf(),
    var isRootForCurrentUser: Boolean = false
) {

    constructor(
        storedQueryTreeEntry: StoredQueryTreeEntry,
        username: String,
        limitToStatisticQueries: Boolean,
        queryType: SavedQueryType
    ) : this(
        path = storedQueryTreeEntry.path,
        name = storedQueryTreeEntry.label,
        type = queryType
    ) {
        if (storedQueryTreeEntry.hasChilds()) {
            for (aChild in storedQueryTreeEntry.childs) {
                if (aChild.isStructure) {
                    children.add(SavedQuery(aChild, username, limitToStatisticQueries, SavedQueryType.FOLDER))
                } else {
                    try {
//                        val queryRoot = QueryReader.read(aChild.query.xml) // TODO improve this because it takes very long for a huge amount of queries; possible improvement: dynamic loading of children
//                        val newQueryIsStatisticQuery = queryRoot.isStatisticQuery
                        val xml = aChild.query.xml
                        val newQueryIsStatisticQuery = xml.contains("<DistributionFilter", true) ||
                                xml.contains("<DistributionRow", true) ||
                                xml.contains("<DistributionColumn", true)
                        if (!newQueryIsStatisticQuery && limitToStatisticQueries)
                            continue
                        val newQueryType = if (newQueryIsStatisticQuery)
                            SavedQueryType.STATISTICS
                        else
                            SavedQueryType.INDIVIDUAL_DATA
                        children.add(SavedQuery(aChild, username, limitToStatisticQueries, newQueryType))
                    } catch (_: Exception) {
                        children.add(SavedQuery(aChild, username, limitToStatisticQueries, SavedQueryType.ERROR))
                    }
                }
            }
        }
        isRootForCurrentUser = storedQueryTreeEntry.path.startsWith(username, true)
                && storedQueryTreeEntry.path.count { c: Char -> c == '/' } == 0
    }
}

enum class SavedQueryType {
    FOLDER, STATISTICS, INDIVIDUAL_DATA, ERROR
}