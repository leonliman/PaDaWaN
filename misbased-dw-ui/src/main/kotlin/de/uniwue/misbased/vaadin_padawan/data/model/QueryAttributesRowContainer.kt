package de.uniwue.misbased.vaadin_padawan.data.model

import de.uniwue.dw.query.model.lang.QueryAnd
import de.uniwue.dw.query.model.lang.QueryAttribute
import de.uniwue.dw.query.model.lang.QueryOr
import de.uniwue.dw.query.model.lang.QueryStructureContainingElem

data class QueryAttributesRowContainer(
    val attributeRows: MutableList<QueryAttributesRow> = mutableListOf(QueryAttributesRow())
) {
    fun fillMXQLQuery(parent: QueryStructureContainingElem) {
        attributeRows.forEach { attributeRow ->
            val nonConnectionItems = attributeRow.getNonConnectingItems()
            if (nonConnectionItems.size == 1) {
                nonConnectionItems.first().addMXQLAttribute(parent, attributeRow.header.ifBlank { null })
            } else if (nonConnectionItems.size > 1) {
                val singleConnectionType = attributeRow.getSingleConnectionType()
                if (singleConnectionType != null) {
                    val connection = when (singleConnectionType) {
                        QueryAttributeConnectionType.OR -> QueryOr(parent)
                        QueryAttributeConnectionType.AND -> QueryAnd(parent)
                        else -> throw IllegalStateException("The returned single connection type has to be either AND or OR.")
                    }
                    nonConnectionItems.forEach { it.addMXQLAttribute(connection) }
                    if (attributeRow.header.isNotBlank())
                        if (connection is QueryAnd) connection.name = attributeRow.header
                        else if (connection is QueryOr) connection.name = attributeRow.header
                } else {
                    val outerOr = QueryOr(parent)
                    var lastAndConnection: QueryAnd? = null
                    for (i in 1..<attributeRow.attributes.size - 1 step 2) {
                        val curConnectionType = attributeRow.attributes[i].connectionType
                        when (curConnectionType) {
                            QueryAttributeConnectionType.OR -> {
                                if (lastAndConnection != null)
                                    lastAndConnection = null
                                else
                                    attributeRow.attributes[i - 1].addMXQLAttribute(outerOr)
                                if (i == attributeRow.attributes.size - 2)
                                    attributeRow.attributes[i + 1].addMXQLAttribute(outerOr)
                            }

                            QueryAttributeConnectionType.AND -> {
                                if (lastAndConnection != null)
                                    attributeRow.attributes[i + 1].addMXQLAttribute(lastAndConnection)
                                else {
                                    lastAndConnection = QueryAnd(outerOr)
                                    attributeRow.attributes[i - 1].addMXQLAttribute(lastAndConnection)
                                    attributeRow.attributes[i + 1].addMXQLAttribute(lastAndConnection)
                                }
                            }

                            else -> throw IllegalStateException("Between two non-connecting items there has to be a connecting item.")
                        }
                    }
                    if (attributeRow.header.isNotBlank())
                        outerOr.name = attributeRow.header
                }
            }
        }
    }

    companion object {
        fun fromMXQLQuery(query: QueryStructureContainingElem): QueryAttributesRowContainer {
            val container = QueryAttributesRowContainer()
            container.attributeRows.clear()

            for (child in query.children)
                if (child is QueryAttribute)
                    container.attributeRows.add(QueryAttributesRow.fromMXQLQuery(child))
                else if (child is QueryAnd || child is QueryOr)
                    container.attributeRows.add(QueryAttributesRow.fromMXQLQuery(child))

            container.attributeRows.add(QueryAttributesRow())
            return container
        }
    }
}
