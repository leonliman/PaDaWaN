package de.uniwue.misbased.vaadin_padawan.views.query

import com.vaadin.flow.component.html.H4
import com.vaadin.flow.component.html.Span
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import de.uniwue.misbased.vaadin_padawan.views.catalog.CatalogTree
import de.uniwue.misbased.vaadin_padawan.views.query.images.QueryViewImagesActionButtonsContainer
import de.uniwue.misbased.vaadin_padawan.views.result.ResultView


class QueryView(catalogTree: CatalogTree, resultView: ResultView) : VerticalLayout() {

    init {
        val title = H4(getTranslation("web-padawan.queryTitle"))

        val queryViewTopSettingsRow = QueryViewTopSettingsRow()
        queryViewTopSettingsRow.setWidthFull()

        val queryViewAttributesContainer = QueryViewAttributesContainer(queryViewTopSettingsRow, catalogTree)
        queryViewAttributesContainer.setWidthFull()

        val queryBottomRowContainer = HorizontalLayout()
        val queryViewImagesActionButtonsContainer = QueryViewImagesActionButtonsContainer(queryViewTopSettingsRow)
        val queryViewBottomActionButtonsRow =
            QueryViewBottomActionButtonsRow(
                queryViewTopSettingsRow,
                queryViewAttributesContainer,
                queryViewImagesActionButtonsContainer,
                resultView
            )
        val spacer = Span()
        queryBottomRowContainer.add(queryViewBottomActionButtonsRow, spacer, queryViewImagesActionButtonsContainer)
        expand(spacer)
        queryBottomRowContainer.setWidthFull()

        add(
            title,
            queryViewTopSettingsRow,
            queryViewAttributesContainer,
            queryBottomRowContainer
        )
        expand(queryViewAttributesContainer)
    }
}