package de.uniwue.misbased.vaadin_padawan.views.query

import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.html.H3
import com.vaadin.flow.component.icon.FontIcon
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.component.popover.Popover
import com.vaadin.flow.component.popover.PopoverVariant
import com.vaadin.flow.component.textfield.TextField
import com.vaadin.flow.data.binder.Binder
import com.vaadin.flow.theme.lumo.LumoUtility
import de.uniwue.misbased.vaadin_padawan.data.FONT_ICON_FAMILY
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributesRow
import java.util.*

class QueryAttributesHeaderPopup(
    queryAttributesRow: QueryAttributesRow,
    queryAttributesCustomField: QueryAttributesCustomField,
    parentLayout: HorizontalLayout
) : Popover() {

    init {
        isAutofocus = true
        isCloseOnEsc = false
        isCloseOnOutsideClick = true
        isModal = true
        isBackdropVisible = true
        addThemeVariants(PopoverVariant.ARROW)

        add(createPopupContent(queryAttributesRow, queryAttributesCustomField))

        addOpenedChangeListener {
            if (!it.isOpened) {
                parentLayout.remove(this)
            }
            QueryAttributesCustomField.forceUIResize()
        }
    }

    private fun createPopupContent(
        queryAttributesRow: QueryAttributesRow,
        queryAttributesCustomField: QueryAttributesCustomField
    ): VerticalLayout {
        val contentLayout = VerticalLayout()
        contentLayout.isPadding = true
        contentLayout.isSpacing = true

        val header = H3(getTranslation("web-padawan.settings.title"))
        header.setWidthFull()

        val binder = Binder(QueryAttributesRow::class.java)
        val queryAttributesRowCopy =
            QueryAttributesRow(header = queryAttributesRow.header, rowID = queryAttributesRow.rowID)
        binder.bean = queryAttributesRowCopy

        val titleField = createTitleField(binder)
        titleField.setWidthFull()

        titleField.addClassNames(
            LumoUtility.Border.TOP,
            LumoUtility.BorderColor.CONTRAST_30,
            LumoUtility.Padding.Top.MEDIUM
        )

        val bottomButtonsLayout = createBottomButtonsLayout(queryAttributesRow, binder, queryAttributesCustomField)
        bottomButtonsLayout.setWidthFull()
        bottomButtonsLayout.addClassNames(
            LumoUtility.Border.TOP,
            LumoUtility.BorderColor.CONTRAST_30,
            LumoUtility.Padding.Top.MEDIUM
        )

        contentLayout.add(header, titleField, bottomButtonsLayout)
        return contentLayout
    }

    private fun createTitleField(binder: Binder<QueryAttributesRow>): TextField {
        val titleField = TextField(getTranslation("web-padawan.settings.titleEntry"))

        val errorMessage = getTranslation("web-padawan.settings.titleEntryError")
        binder.forField(titleField)
            .asRequired(errorMessage)
            .bind(QueryAttributesRow::header::get, QueryAttributesRow::header::set)
        return titleField
    }

    private fun createBottomButtonsLayout(
        queryAttributesRow: QueryAttributesRow,
        binder: Binder<QueryAttributesRow>,
        queryAttributesCustomField: QueryAttributesCustomField
    ): HorizontalLayout {
        val bottomButtonsLayout = HorizontalLayout()

        val buttonsToUse = mutableListOf<Button>()
        val saveButton = createSaveButton(binder, queryAttributesCustomField)
        buttonsToUse.add(saveButton)
        if (queryAttributesRow.header.isNotEmpty()) {
            val deleteButton = createDeleteButton(binder, queryAttributesCustomField)
            buttonsToUse.add(deleteButton)
        }
        val cancelButton = createCancelButton()
        buttonsToUse.add(cancelButton)

        bottomButtonsLayout.addAndExpand(*buttonsToUse.toTypedArray())
        return bottomButtonsLayout
    }

    private fun createSaveButton(
        binder: Binder<QueryAttributesRow>,
        queryAttributesCustomField: QueryAttributesCustomField
    ): Button {
        val saveIcon = FontIcon(FONT_ICON_FAMILY, "fa-check")
        val saveButton = Button(getTranslation("web-padawan.settings.save"), saveIcon)
        saveButton.addClickListener {
            if (binder.validate().isOk) {
                updateHeaderValue(queryAttributesCustomField, binder.bean.rowID, binder.bean.header)
                close()
            }
        }
        return saveButton
    }

    private fun updateHeaderValue(
        queryAttributesCustomField: QueryAttributesCustomField,
        rowID: UUID,
        header: String
    ) {
        val queryAttributesRowContainer = queryAttributesCustomField.getCurrentAttributesRowContainer()
        for (queryAttributesRowInContainer in queryAttributesRowContainer.attributeRows) {
            if (queryAttributesRowInContainer.rowID == rowID) {
                queryAttributesRowInContainer.header = header
                queryAttributesCustomField.setCurrentAttributesRowContainer(queryAttributesRowContainer)
                break
            }
        }
    }

    private fun createDeleteButton(
        binder: Binder<QueryAttributesRow>,
        queryAttributesCustomField: QueryAttributesCustomField
    ): Button {
        val deleteIcon = FontIcon(FONT_ICON_FAMILY, "fa-trash")
        val deleteButton = Button(getTranslation("web-padawan.settings.deleteTitleEntry"), deleteIcon)
        deleteButton.addClickListener {
            updateHeaderValue(queryAttributesCustomField, binder.bean.rowID, "")
            close()
        }
        return deleteButton
    }

    private fun createCancelButton(): Button {
        val cancelIcon = FontIcon(FONT_ICON_FAMILY, "fa-ban")
        val cancelButton = Button(getTranslation("web-padawan.settings.cancel"), cancelIcon)
        cancelButton.addClickListener {
            close()
        }
        return cancelButton
    }
}