package de.uniwue.misbased.vaadin_padawan.views.query

import com.vaadin.flow.component.ComponentEvent
import com.vaadin.flow.component.UI
import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.button.ButtonVariant
import com.vaadin.flow.component.customfield.CustomField
import com.vaadin.flow.component.dnd.DropEffect
import com.vaadin.flow.component.dnd.DropTarget
import com.vaadin.flow.component.icon.FontIcon
import com.vaadin.flow.component.orderedlayout.FlexComponent
import com.vaadin.flow.component.orderedlayout.FlexLayout
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.server.VaadinSession
import com.vaadin.flow.shared.Registration
import com.vaadin.flow.theme.lumo.LumoUtility
import de.uniwue.dw.core.model.data.CatalogEntry
import de.uniwue.misbased.vaadin_padawan.data.FONT_ICON_FAMILY
import de.uniwue.misbased.vaadin_padawan.data.SESSION_ATTRIBUTE_QUERY_ROWS
import de.uniwue.misbased.vaadin_padawan.data.SESSION_ATTRIBUTE_QUERY_ROWS_INDIVIDUAL_DATA
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributeConnectionType
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributeContainer
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributesRow
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributesRowContainer
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNUserSettingsConnector
import de.uniwue.misbased.vaadin_padawan.views.catalog.CatalogTree
import de.uniwue.misbased.vaadin_padawan.views.query.attributePopup.QueryAttributePopup

class QueryAttributesCustomField(
    private val withHeaderButton: Boolean,
    private val attributesSessionKey: String,
    private val catalogTree: CatalogTree,
    private val queryViewTopSettingsRow: QueryViewTopSettingsRow
) :
    CustomField<String>() {

    init {
        val queryAttributesRowContainer = getCurrentAttributesRowContainer()
        for (queryAttributesRow in queryAttributesRowContainer.attributeRows) {
            val singleAttributesRow = createSingleAttributesRow(queryAttributesRow)
            add(singleAttributesRow)
        }

        addQueryAttributesChangedListener {
            val currentAttributesRowContainer = getCurrentAttributesRowContainer()
            val queryAttributesRow = it.queryAttributesRow
            if (currentAttributesRowContainer.attributeRows.last() == queryAttributesRow && queryAttributesRow.attributes.isNotEmpty()) {
                val newAttributesRow = QueryAttributesRow()
                currentAttributesRowContainer.attributeRows.add(newAttributesRow)
                setCurrentAttributesRowContainer(currentAttributesRowContainer)
                val singleAttributesRow = createSingleAttributesRow(newAttributesRow)
                add(singleAttributesRow)
                forceUIResize()
            }
            if (queryAttributesRow.attributes.isEmpty() && currentAttributesRowContainer.attributeRows.size > 1)
                deleteQueryAttributesRow(it.rowLayout, queryAttributesRow)
        }
    }

    fun resetAttributes(queryAttributesRows: List<QueryAttributesRow>? = null) {
        val queryAttributesRowContainer = getCurrentAttributesRowContainer()
        queryAttributesRowContainer.attributeRows.clear()
        for (component in children)
            if (component is HorizontalLayout)
                remove(component)
        if (queryAttributesRows != null)
            for (queryAttributesRow in queryAttributesRows)
                queryAttributesRowContainer.attributeRows.add(queryAttributesRow)
        else {
            val newAttributesRow = QueryAttributesRow()
            queryAttributesRowContainer.attributeRows.add(newAttributesRow)
        }
        setCurrentAttributesRowContainer(queryAttributesRowContainer)
        for (queryAttributesRow in queryAttributesRowContainer.attributeRows) {
            val singleAttributesRow = createSingleAttributesRow(queryAttributesRow)
            add(singleAttributesRow)
        }
    }

    companion object {
        fun forceUIResize(waitTimeToRefresh: Int = 1) {
            // Workaround for an issue in Safari causing icons of buttons of the attribute rows to be shown very large
            // (and for some other weird display issues in Safari)
            UI.getCurrent().page.executeJs(getUIResizeJavascript(waitTimeToRefresh))
        }

        fun getUIResizeJavascript(waitTimeToRefresh: Int = 1): String {
            if (UI.getCurrent().session.browser.isSafari)
                return "document.body.style.zoom = 1.0000001;" +
                        "setTimeout(function(){document.body.style.zoom = 1;},$waitTimeToRefresh);"
            return ""
        }

        fun resetUITextSelection() {
            UI.getCurrent().page.executeJs(
                """
                    if (window.getSelection) {
                      if (window.getSelection().empty) {  // Chrome
                        window.getSelection().empty();
                      } else if (window.getSelection().removeAllRanges) {  // Firefox, Edge, Safari
                        window.getSelection().removeAllRanges();
                      }
                    }
                """.trimIndent()
            )
        }

        fun getAttributesRowContainer(
            attributesSessionKey: String,
            queryViewTopSettingsRow: QueryViewTopSettingsRow
        ): QueryAttributesRowContainer {
            val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
            return if (userSettings.userInterfaceUseSessionStorage) {
                if (VaadinSession.getCurrent().getAttribute(attributesSessionKey) == null)
                    setAttributesRowContainer(
                        attributesSessionKey,
                        QueryAttributesRowContainer(),
                        queryViewTopSettingsRow
                    )
                VaadinSession.getCurrent().getAttribute(attributesSessionKey) as QueryAttributesRowContainer
            } else {
                if (queryViewTopSettingsRow.getQueryAttributesRowContainerForNonSessionStorage(attributesSessionKey) == null)
                    setAttributesRowContainer(
                        attributesSessionKey,
                        QueryAttributesRowContainer(),
                        queryViewTopSettingsRow
                    )
                queryViewTopSettingsRow.getQueryAttributesRowContainerForNonSessionStorage(attributesSessionKey)!!
            }

        }

        private fun setAttributesRowContainer(
            attributesSessionKey: String,
            queryAttributesRowContainer: QueryAttributesRowContainer,
            queryViewTopSettingsRow: QueryViewTopSettingsRow
        ) {
            val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
            if (userSettings.userInterfaceUseSessionStorage)
                VaadinSession.getCurrent().setAttribute(attributesSessionKey, queryAttributesRowContainer)
            else
                queryViewTopSettingsRow.updateQueryAttributesRowContainersForNonSessionStorage(
                    attributesSessionKey,
                    queryAttributesRowContainer
                )
        }
    }

    fun getCurrentAttributesRowContainer(): QueryAttributesRowContainer {
        return getAttributesRowContainer(attributesSessionKey, queryViewTopSettingsRow)
    }

    fun setCurrentAttributesRowContainer(queryAttributesRowContainer: QueryAttributesRowContainer) {
        setAttributesRowContainer(attributesSessionKey, queryAttributesRowContainer, queryViewTopSettingsRow)
    }

    private class QueryAttributesChangedEvent(
        val queryAttributesRow: QueryAttributesRow,
        val rowLayout: HorizontalLayout,
        source: QueryAttributesCustomField?
    ) :
        ComponentEvent<QueryAttributesCustomField>(source, false)

    private fun addQueryAttributesChangedListener(listener: (QueryAttributesChangedEvent) -> Unit): Registration =
        eventBus.addListener(QueryAttributesChangedEvent::class.java, listener)

    fun fireQueryAttributesChangedEvent(queryAttributesRow: QueryAttributesRow, rowLayout: HorizontalLayout) {
        eventBus.fireEvent(QueryAttributesChangedEvent(queryAttributesRow, rowLayout, this))
    }

    private fun createSingleAttributesRow(queryAttributesRow: QueryAttributesRow): HorizontalLayout {
        val singleAttributesRow = HorizontalLayout()
        if (getCurrentAttributesRowContainer().attributeRows.first() != queryAttributesRow)
            singleAttributesRow.addClassName(LumoUtility.Padding.Top.XSMALL)
        singleAttributesRow.setWidthFull()

        val attributesFlexLayout = createAttributesFlexLayout(singleAttributesRow, queryAttributesRow)
        singleAttributesRow.addAndExpand(attributesFlexLayout)

        if (withHeaderButton) {
            val headerButton = createHeaderButton(queryAttributesRow, singleAttributesRow)
            singleAttributesRow.add(headerButton)
        }
        val deleteButton = createDeleteButton(singleAttributesRow, queryAttributesRow)
        singleAttributesRow.add(deleteButton)

        return singleAttributesRow
    }

    private fun createAttributesFlexLayout(
        parentLayout: HorizontalLayout,
        queryAttributesRow: QueryAttributesRow
    ): FlexLayout {
        val attributesFlexLayout = FlexLayout()
        attributesFlexLayout.flexWrap = FlexLayout.FlexWrap.WRAP
        attributesFlexLayout.alignItems = FlexComponent.Alignment.START
        attributesFlexLayout.justifyContentMode = FlexComponent.JustifyContentMode.START
        attributesFlexLayout.addClassNames(
            LumoUtility.Border.ALL,
            LumoUtility.BorderColor.CONTRAST_30,
            LumoUtility.Padding.XSMALL,
            LumoUtility.Gap.SMALL
        )

        for (attributeContainer in queryAttributesRow.attributes) {
            addAttributesButton(attributeContainer, queryAttributesRow, attributesFlexLayout, parentLayout)
        }

        val dropTarget = DropTarget.create(attributesFlexLayout)
        dropTarget.dropEffect = DropEffect.COPY
        dropTarget.addDropListener { event ->
            if (event.dropEffect === DropEffect.COPY) {
                event.dragSourceComponent.ifPresent {
                    event.dragData.ifPresent { data ->
                        if (data is List<*>) {
                            val firstEntry = data.first()
                            if (firstEntry is CatalogEntry) {
                                addORConnectionToRowIfNecessary(queryAttributesRow, attributesFlexLayout, parentLayout)

                                val attributeContainer = QueryAttributeContainer(catalogEntry = firstEntry)
                                queryAttributesRow.attributes.add(attributeContainer)
                                addAttributesButton(
                                    attributeContainer,
                                    queryAttributesRow,
                                    attributesFlexLayout,
                                    parentLayout
                                )

                                fireQueryAttributesChangedEvent(queryAttributesRow, parentLayout)
                            }
                        }
                    }
                }
            }
        }

        if (attributesSessionKey == SESSION_ATTRIBUTE_QUERY_ROWS || attributesSessionKey == SESSION_ATTRIBUTE_QUERY_ROWS_INDIVIDUAL_DATA) {
            catalogTree.addItemDoubleClickListener {
                if ((attributesSessionKey == SESSION_ATTRIBUTE_QUERY_ROWS && queryViewTopSettingsRow.getCurrentQueryMode() == QueryViewTopSettingsRow.QueryMode.STATISTICS) ||
                    (attributesSessionKey == SESSION_ATTRIBUTE_QUERY_ROWS_INDIVIDUAL_DATA && queryViewTopSettingsRow.getCurrentQueryMode() == QueryViewTopSettingsRow.QueryMode.INDIVIDUAL_DATA)
                ) {
                    val catalogEntry = it.item
                    if (catalogEntry != null && queryAttributesRow.attributes.isEmpty()) {
                        resetUITextSelection()
                        catalogTree.select(catalogEntry)
                        val attributeContainer = QueryAttributeContainer(catalogEntry = catalogEntry)
                        queryAttributesRow.attributes.add(attributeContainer)
                        addAttributesButton(
                            attributeContainer,
                            queryAttributesRow,
                            attributesFlexLayout,
                            parentLayout
                        )
                        fireQueryAttributesChangedEvent(queryAttributesRow, parentLayout)
                    }
                }
            }
        }

        return attributesFlexLayout
    }

    private fun addAttributesButton(
        attributeContainer: QueryAttributeContainer,
        queryAttributesRow: QueryAttributesRow,
        attributesFlexLayout: FlexLayout,
        parentLayout: HorizontalLayout,
        insertPosition: Int = -1
    ) {
        val attributeButton = QueryAttributeButton(attributeContainer)
        val id = "attributeButton${attributeContainer.containerID}"
        attributeButton.setId(id)
        attributeButton.addClickListener {
            if (attributeContainer.connectionType == QueryAttributeConnectionType.NONE) {
                val popup = QueryAttributePopup(
                    attributeButton,
                    attributeContainer,
                    attributesFlexLayout,
                    parentLayout,
                    this,
                    queryAttributesRow.rowID,
                    attributesSessionKey,
                    catalogTree
                )
                popup.`for` = id
                attributesFlexLayout.add(popup)
                popup.open()
            } else {
                if (attributeContainer.connectionType == QueryAttributeConnectionType.AND)
                    attributeContainer.connectionType = QueryAttributeConnectionType.OR
                else
                    attributeContainer.connectionType = QueryAttributeConnectionType.AND
                attributeButton.text = attributeContainer.getButtonTitle()
            }
        }

        val dropTarget = DropTarget.create(attributeButton)
        dropTarget.dropEffect = DropEffect.COPY
        dropTarget.addDropListener { event ->
            if (event.dropEffect === DropEffect.COPY) {
                event.dragSourceComponent.ifPresent {
                    event.dragData.ifPresent { data ->
                        if (data is List<*>) {
                            val firstEntry = data.first()
                            if (firstEntry is CatalogEntry) {
                                val newInsertPosition = queryAttributesRow.attributes.indexOf(attributeContainer)
                                val connectionAttributeContainer = QueryAttributeContainer(
                                    connectionType = QueryAttributeConnectionType.OR
                                )
                                val newAttributeContainer = QueryAttributeContainer(catalogEntry = firstEntry)

                                if (attributeContainer.connectionType == QueryAttributeConnectionType.NONE) {
                                    queryAttributesRow.attributes.add(newInsertPosition, newAttributeContainer)
                                    addAttributesButton(
                                        newAttributeContainer,
                                        queryAttributesRow,
                                        attributesFlexLayout,
                                        parentLayout,
                                        newInsertPosition
                                    )
                                    queryAttributesRow.attributes.add(
                                        newInsertPosition + 1,
                                        connectionAttributeContainer
                                    )
                                    addAttributesButton(
                                        connectionAttributeContainer,
                                        queryAttributesRow,
                                        attributesFlexLayout,
                                        parentLayout,
                                        newInsertPosition + 1
                                    )
                                } else {
                                    queryAttributesRow.attributes.add(newInsertPosition, connectionAttributeContainer)
                                    addAttributesButton(
                                        connectionAttributeContainer,
                                        queryAttributesRow,
                                        attributesFlexLayout,
                                        parentLayout,
                                        newInsertPosition
                                    )
                                    queryAttributesRow.attributes.add(newInsertPosition + 1, newAttributeContainer)
                                    addAttributesButton(
                                        newAttributeContainer,
                                        queryAttributesRow,
                                        attributesFlexLayout,
                                        parentLayout,
                                        newInsertPosition + 1
                                    )
                                }
                                fireQueryAttributesChangedEvent(queryAttributesRow, parentLayout)
                            }
                        }
                    }
                }
            }
        }

        if (insertPosition >= 0)
            attributesFlexLayout.addComponentAtIndex(insertPosition, attributeButton)
        else
            attributesFlexLayout.add(attributeButton)
        if (parentLayout.alignItems != FlexComponent.Alignment.CENTER)
            parentLayout.alignItems = FlexComponent.Alignment.CENTER
    }

    private fun addORConnectionToRowIfNecessary(
        queryAttributesRow: QueryAttributesRow,
        attributesFlexLayout: FlexLayout,
        parentLayout: HorizontalLayout
    ) {
        if (queryAttributesRow.attributes.isNotEmpty()) {
            val connectionAttributeContainer = QueryAttributeContainer(
                connectionType = QueryAttributeConnectionType.OR
            )
            queryAttributesRow.attributes.add(connectionAttributeContainer)
            addAttributesButton(
                connectionAttributeContainer,
                queryAttributesRow,
                attributesFlexLayout,
                parentLayout
            )
        }
    }

    private fun createHeaderButton(queryAttributesRow: QueryAttributesRow, parentLayout: HorizontalLayout): Button {
        val headerIcon = FontIcon(FONT_ICON_FAMILY, "fa-header")
        val headerButton = Button(headerIcon)

        val id = "headerButton${queryAttributesRow.rowID}"
        headerButton.setId(id)
        headerButton.addClickListener {
            val popup = QueryAttributesHeaderPopup(queryAttributesRow, this, parentLayout)
            popup.`for` = id
            parentLayout.add(popup)
            popup.open()
        }

        if (queryAttributesRow.attributes.isEmpty())
            headerButton.isEnabled = false
        addQueryAttributesChangedListener {
            if (it.queryAttributesRow.rowID == queryAttributesRow.rowID)
                headerButton.isEnabled = it.queryAttributesRow.attributes.isNotEmpty()
        }

        return headerButton
    }

    private fun createDeleteButton(parentLayout: HorizontalLayout, queryAttributesRow: QueryAttributesRow): Button {
        val deleteIcon = FontIcon(FONT_ICON_FAMILY, "fa-times")
        val deleteButton = Button(deleteIcon)
        deleteButton.addThemeVariants(ButtonVariant.LUMO_ERROR)

        deleteButton.addClickListener {
            deleteQueryAttributesRow(parentLayout, queryAttributesRow)
        }

        if (queryAttributesRow.attributes.isEmpty())
            deleteButton.isEnabled = false
        addQueryAttributesChangedListener {
            if (it.queryAttributesRow.rowID == queryAttributesRow.rowID)
                deleteButton.isEnabled = it.queryAttributesRow.attributes.isNotEmpty()
        }
        return deleteButton
    }

    private fun deleteQueryAttributesRow(rowLayout: HorizontalLayout, queryAttributesRow: QueryAttributesRow) {
        val queryAttributesRowContainer = getCurrentAttributesRowContainer()
        queryAttributesRowContainer.attributeRows.remove(queryAttributesRow)
        setCurrentAttributesRowContainer(queryAttributesRowContainer)
        rowLayout.removeFromParent()
        forceUIResize()
    }

    override fun generateModelValue(): String {
        return "" // not used
    }

    override fun setPresentationValue(newPresentationValue: String?) {
        // not used
    }
}