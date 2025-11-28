package de.uniwue.misbased.vaadin_padawan.views.query.attributePopup

import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.icon.FontIcon
import com.vaadin.flow.component.orderedlayout.FlexLayout
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.data.binder.Binder
import de.uniwue.misbased.vaadin_padawan.data.FONT_ICON_FAMILY
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributeConnectionType
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributeContainer
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributeData
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributesRow
import de.uniwue.misbased.vaadin_padawan.views.query.QueryAttributeButton
import de.uniwue.misbased.vaadin_padawan.views.query.QueryAttributesCustomField
import java.util.*

class QueryAttributePopupBottomButtonsLayout(
    binder: Binder<QueryAttributeData>,
    attributeButton: QueryAttributeButton,
    attributesFlexLayout: FlexLayout,
    parentLayout: HorizontalLayout,
    queryAttributesCustomField: QueryAttributesCustomField,
    rowID: UUID,
    containerID: UUID,
    queryAttributePopup: QueryAttributePopup
) : HorizontalLayout() {
    init {
        val buttonsToUse = mutableListOf<Button>()

        val saveButton = createSaveButton(
            binder,
            queryAttributesCustomField,
            rowID,
            containerID,
            attributeButton,
            queryAttributePopup
        )
        buttonsToUse.add(saveButton)

        val deleteButton = createDeleteButton(
            attributeButton,
            attributesFlexLayout,
            parentLayout,
            queryAttributesCustomField,
            rowID,
            containerID,
            queryAttributePopup
        )
        buttonsToUse.add(deleteButton)

        val cancelButton = createCancelButton(queryAttributePopup)
        buttonsToUse.add(cancelButton)

        addAndExpand(*buttonsToUse.toTypedArray())
    }

    private fun createSaveButton(
        binder: Binder<QueryAttributeData>,
        queryAttributesCustomField: QueryAttributesCustomField,
        rowID: UUID,
        containerID: UUID,
        attributeButton: QueryAttributeButton,
        queryAttributePopup: QueryAttributePopup
    ): Button {
        val saveIcon = FontIcon(FONT_ICON_FAMILY, "fa-check")
        val saveButton = Button(getTranslation("web-padawan.settings.save"), saveIcon)
        saveButton.addClickListener {
            if (binder.validate().isOk) {
                val queryAttributesRowContainer = queryAttributesCustomField.getCurrentAttributesRowContainer()
                for (queryAttributesRowInContainer in queryAttributesRowContainer.attributeRows) {
                    if (queryAttributesRowInContainer.rowID == rowID) {
                        for (queryAttributeContainer in queryAttributesRowInContainer.attributes) {
                            if (queryAttributeContainer.containerID == containerID) {
                                queryAttributeContainer.queryAttributeData = binder.bean
                                queryAttributesCustomField.setCurrentAttributesRowContainer(queryAttributesRowContainer)
                                attributeButton.text = queryAttributeContainer.getButtonTitle()
                                break
                            }
                        }
                        break
                    }
                }
                queryAttributePopup.close()
            }
        }
        return saveButton
    }

    private fun createDeleteButton(
        attributeButton: QueryAttributeButton,
        attributesFlexLayout: FlexLayout,
        parentLayout: HorizontalLayout,
        queryAttributesCustomField: QueryAttributesCustomField,
        rowID: UUID,
        containerID: UUID,
        queryAttributePopup: QueryAttributePopup
    ): Button {
        val deleteIcon = FontIcon(FONT_ICON_FAMILY, "fa-times")
        val deleteButton = Button(getTranslation("web-padawan.queryActions.delete"), deleteIcon)
        deleteButton.addClickListener {
            val queryAttributesRowContainer = queryAttributesCustomField.getCurrentAttributesRowContainer()
            for (queryAttributesRowInContainer in queryAttributesRowContainer.attributeRows) {
                if (queryAttributesRowInContainer.rowID == rowID) {
                    for (queryAttributeContainer in queryAttributesRowInContainer.attributes) {
                        if (queryAttributeContainer.containerID == containerID) {
                            performAttributeDeletion(
                                attributeButton,
                                queryAttributeContainer,
                                queryAttributesRowInContainer,
                                attributesFlexLayout
                            )
                            queryAttributesCustomField.setCurrentAttributesRowContainer(queryAttributesRowContainer)
                            queryAttributesCustomField.fireQueryAttributesChangedEvent(
                                queryAttributesRowInContainer,
                                parentLayout
                            )
                            break
                        }
                    }
                    break
                }
            }
            queryAttributePopup.close()
        }
        return deleteButton
    }

    private fun performAttributeDeletion(
        attributeButton: QueryAttributeButton,
        attributeContainer: QueryAttributeContainer,
        queryAttributesRow: QueryAttributesRow,
        attributesFlexLayout: FlexLayout
    ) {
        queryAttributesRow.attributes.remove(attributeContainer)
        attributesFlexLayout.remove(attributeButton)
        var previousContainer: QueryAttributeContainer? = null
        var previousButton: QueryAttributeButton? = null
        val buttonsToRemove = mutableListOf<QueryAttributeButton>()
        for (children in attributesFlexLayout.children) {
            if (children is QueryAttributeButton) {
                val currentContainer = children.queryAttributeContainer
                if (currentContainer.connectionType != QueryAttributeConnectionType.NONE &&
                    (previousContainer == null ||
                            previousContainer.connectionType != QueryAttributeConnectionType.NONE)
                ) {
                    buttonsToRemove.add(children)
                    queryAttributesRow.attributes.remove(currentContainer)
                }
                previousContainer = currentContainer
                previousButton = children
            }
        }
        if (previousContainer != null && previousContainer.connectionType != QueryAttributeConnectionType.NONE) {
            queryAttributesRow.attributes.remove(previousContainer)
            buttonsToRemove.add(previousButton!!)
        }
        for (button in buttonsToRemove)
            attributesFlexLayout.remove(button)
    }

    private fun createCancelButton(queryAttributePopup: QueryAttributePopup): Button {
        val cancelIcon = FontIcon(FONT_ICON_FAMILY, "fa-ban")
        val cancelButton = Button(getTranslation("web-padawan.settings.cancel"), cancelIcon)
        cancelButton.addClickListener {
            queryAttributePopup.close()
        }
        return cancelButton
    }
}