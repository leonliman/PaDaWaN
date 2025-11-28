package de.uniwue.misbased.vaadin_padawan.data.model

import de.uniwue.dw.query.model.lang.QueryAnd
import de.uniwue.dw.query.model.lang.QueryAttribute
import de.uniwue.dw.query.model.lang.QueryOr
import de.uniwue.dw.query.model.lang.QueryStructureContainingElem
import java.util.*

data class QueryAttributesRow(
    val attributes: MutableList<QueryAttributeContainer> = mutableListOf(),
    var header: String = "",
    val rowID: UUID = UUID.randomUUID()
) {
    fun getNonConnectingItems(): List<QueryAttributeContainer> {
        return attributes.filter { it.connectionType == QueryAttributeConnectionType.NONE }
    }

    fun getSingleConnectionType(): QueryAttributeConnectionType? {
        val connectionTypes = attributes
            .filter { it.connectionType != QueryAttributeConnectionType.NONE }
            .map { it.connectionType }
            .toSet()
        return if (connectionTypes.size == 1) connectionTypes.first() else null
    }

    companion object {
        fun fromMXQLQuery(queryAttribute: QueryAttribute): QueryAttributesRow {
            val attributes = mutableListOf<QueryAttributeContainer>()
            attributes.add(QueryAttributeContainer.fromMXQLQuery(queryAttribute))
            val header = queryAttribute.displayName ?: ""
            return QueryAttributesRow(attributes, header)
        }

        fun fromMXQLQuery(query: QueryStructureContainingElem): QueryAttributesRow {
            val attributes = mutableListOf<QueryAttributeContainer>()
            val header = when (query) {
                is QueryAnd -> query.name ?: ""
                is QueryOr -> query.name ?: ""
                else -> ""
            }
            val connectionType = when (query) {
                is QueryAnd -> QueryAttributeConnectionType.AND
                is QueryOr -> QueryAttributeConnectionType.OR
                else -> throw IllegalStateException(
                    "Loading a QueryAttributesRow " +
                            "from a QueryStructureContainingElem is only possible for QueryAnd and QueryOr."
                )
            }
            for (i in 0 until query.children.size) {
                val curChild = query.children[i]
                if (curChild is QueryAttribute)
                    attributes.add(QueryAttributeContainer.fromMXQLQuery(curChild))
                else if (curChild is QueryAnd) {
                    for (j in 0 until curChild.children.size) {
                        val curSubChild = curChild.children[j]
                        if (curSubChild is QueryAttribute)
                            attributes.add(QueryAttributeContainer.fromMXQLQuery(curSubChild))
                        if (j < curChild.children.size - 1)
                            attributes.add(QueryAttributeContainer(QueryAttributeConnectionType.AND))
                    }
                }
                if (i < query.children.size - 1)
                    attributes.add(QueryAttributeContainer(connectionType))
            }
            return QueryAttributesRow(attributes, header)
        }
    }
}
