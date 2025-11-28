package de.uniwue.misbased.vaadin_padawan.views.query

import com.vaadin.flow.component.orderedlayout.Scroller
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.theme.lumo.LumoUtility
import de.uniwue.misbased.vaadin_padawan.data.SESSION_ATTRIBUTE_QUERY_COLUMNS
import de.uniwue.misbased.vaadin_padawan.data.SESSION_ATTRIBUTE_QUERY_FILTERS
import de.uniwue.misbased.vaadin_padawan.data.SESSION_ATTRIBUTE_QUERY_ROWS
import de.uniwue.misbased.vaadin_padawan.data.SESSION_ATTRIBUTE_QUERY_ROWS_INDIVIDUAL_DATA
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributesRow
import de.uniwue.misbased.vaadin_padawan.views.catalog.CatalogTree

class QueryViewAttributesContainer(
    queryViewTopSettingsRow: QueryViewTopSettingsRow,
    catalogTree: CatalogTree
) : Scroller() {

    private lateinit var rowsSection: QueryAttributesCustomField
    private lateinit var columnsSection: QueryAttributesCustomField
    private lateinit var filtersSection: QueryAttributesCustomField
    private lateinit var individualDataSection: QueryAttributesCustomField

    init {
        scrollDirection = ScrollDirection.VERTICAL
        addClassNames(
            LumoUtility.Border.ALL,
            LumoUtility.BorderColor.CONTRAST_50,
            LumoUtility.BoxSizing.BORDER
        )

        val queryAttributesFieldsContainer = createQueryAttributesFieldsContainer(queryViewTopSettingsRow, catalogTree)
        content = queryAttributesFieldsContainer
    }

    private fun createQueryAttributesFieldsContainer(
        queryViewTopSettingsRow: QueryViewTopSettingsRow,
        catalogTree: CatalogTree
    ): VerticalLayout {
        val queryAttributesFieldsContainer = VerticalLayout()
        queryAttributesFieldsContainer.setWidthFull()
        queryAttributesFieldsContainer.isPadding = true
        queryAttributesFieldsContainer.addClassName(LumoUtility.Gap.Row.LARGE)

        val initialQueryMode = queryViewTopSettingsRow.getCurrentQueryMode()

        rowsSection = getQueryAttributesSection(
            getTranslation("web-padawan.querySection.rows"),
            true,
            initialQueryMode,
            queryViewTopSettingsRow,
            SESSION_ATTRIBUTE_QUERY_ROWS,
            catalogTree
        )
        columnsSection = getQueryAttributesSection(
            getTranslation("web-padawan.querySection.columns"),
            true,
            initialQueryMode,
            queryViewTopSettingsRow,
            SESSION_ATTRIBUTE_QUERY_COLUMNS,
            catalogTree
        )
        filtersSection = getQueryAttributesSection(
            getTranslation("web-padawan.querySection.filters"),
            false,
            initialQueryMode,
            queryViewTopSettingsRow,
            SESSION_ATTRIBUTE_QUERY_FILTERS,
            catalogTree
        )

        individualDataSection = getQueryAttributesSection(
            null,
            false,
            initialQueryMode,
            queryViewTopSettingsRow,
            SESSION_ATTRIBUTE_QUERY_ROWS_INDIVIDUAL_DATA,
            catalogTree
        )

        queryAttributesFieldsContainer.add(
            rowsSection,
            columnsSection,
            filtersSection,
            individualDataSection
        )
        return queryAttributesFieldsContainer
    }

    private fun getQueryAttributesSection(
        title: String?,
        withHeaderButton: Boolean,
        initialQueryMode: QueryViewTopSettingsRow.QueryMode,
        queryViewTopSettingsRow: QueryViewTopSettingsRow,
        attributesSessionKey: String,
        catalogTree: CatalogTree
    ): QueryAttributesCustomField {
        val queryAttributesCustomField =
            QueryAttributesCustomField(withHeaderButton, attributesSessionKey, catalogTree, queryViewTopSettingsRow)
        if (title != null)
            queryAttributesCustomField.label = title
        queryAttributesCustomField.setWidthFull()
        queryAttributesCustomField.addClassNames(
            LumoUtility.Border.ALL,
            LumoUtility.BorderColor.PRIMARY_50,
            LumoUtility.Padding.MEDIUM,
            LumoUtility.BoxSizing.BORDER
        )
        if (
            (title == null && initialQueryMode == QueryViewTopSettingsRow.QueryMode.STATISTICS) ||
            (title != null && initialQueryMode == QueryViewTopSettingsRow.QueryMode.INDIVIDUAL_DATA)
        )
            queryAttributesCustomField.addClassName(LumoUtility.Display.HIDDEN)
        queryViewTopSettingsRow.addQueryModeChangedListener {
            when (it.queryMode) {
                QueryViewTopSettingsRow.QueryMode.STATISTICS -> {
                    if (title == null)
                        queryAttributesCustomField.addClassName(LumoUtility.Display.HIDDEN)
                    else
                        queryAttributesCustomField.removeClassName(LumoUtility.Display.HIDDEN)
                }

                QueryViewTopSettingsRow.QueryMode.INDIVIDUAL_DATA -> {
                    if (title == null)
                        queryAttributesCustomField.removeClassName(LumoUtility.Display.HIDDEN)
                    else
                        queryAttributesCustomField.addClassName(LumoUtility.Display.HIDDEN)
                }
            }
        }
        return queryAttributesCustomField
    }

    fun resetAttributeSectionsForQueryMode(
        queryMode: QueryViewTopSettingsRow.QueryMode,
        queryAttributeRows: List<List<QueryAttributesRow>>? = null
    ) {
        when (queryMode) {
            QueryViewTopSettingsRow.QueryMode.STATISTICS -> {
                if (queryAttributeRows != null && queryAttributeRows.size == 3) {
                    rowsSection.resetAttributes(queryAttributeRows[0])
                    columnsSection.resetAttributes(queryAttributeRows[1])
                    filtersSection.resetAttributes(queryAttributeRows[2])
                } else {
                    rowsSection.resetAttributes()
                    columnsSection.resetAttributes()
                    filtersSection.resetAttributes()
                }
            }

            QueryViewTopSettingsRow.QueryMode.INDIVIDUAL_DATA -> {
                if (queryAttributeRows != null && queryAttributeRows.size == 1)
                    individualDataSection.resetAttributes(queryAttributeRows[0])
                else
                    individualDataSection.resetAttributes()
            }
        }
    }
}