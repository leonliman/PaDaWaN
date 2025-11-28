package de.uniwue.misbased.vaadin_padawan.views.catalog

import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.html.H3
import com.vaadin.flow.component.icon.FontIcon
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.component.popover.Popover
import com.vaadin.flow.component.popover.PopoverVariant
import com.vaadin.flow.component.textfield.TextField
import de.uniwue.dw.core.model.data.CatalogEntry
import de.uniwue.misbased.vaadin_padawan.data.FONT_ICON_FAMILY
import de.uniwue.misbased.vaadin_padawan.views.query.QueryAttributesCustomField

class CatalogEntryPropertiesPopup(catalogEntry: CatalogEntry, parentLayout: VerticalLayout) : Popover() {

    companion object {
        fun getPropertiesDialogTextField(value: String, label: String): TextField {
            val textField = TextField(label)
            textField.value = value
            textField.isReadOnly = true
            textField.setWidthFull()
            return textField
        }
    }

    init {
        isAutofocus = true
        isCloseOnEsc = false
        isCloseOnOutsideClick = true
        isModal = true
        isBackdropVisible = true
        addThemeVariants(PopoverVariant.ARROW)

        add(createPopupContent(catalogEntry))

        addOpenedChangeListener {
            if (!it.isOpened) {
                parentLayout.remove(this)
            }
            QueryAttributesCustomField.forceUIResize()
        }
    }

    private fun createCloseButton(): Button {
        val closeIcon = FontIcon(FONT_ICON_FAMILY, "fa-times")
        return Button(closeIcon) {
            close()
        }
    }

    private fun createPopupContent(catalogEntry: CatalogEntry): VerticalLayout {
        val popupContent = VerticalLayout()
        popupContent.isPadding = true
        popupContent.isSpacing = false

        val horizontalHeaderLayout = HorizontalLayout()
        val header = H3(getTranslation("web-padawan.catalog.contextMenu.properties.dialog.title"))
        horizontalHeaderLayout.addAndExpand(header)
        horizontalHeaderLayout.add(createCloseButton())

        val nameTextField = getPropertiesDialogTextField(
            catalogEntry.name,
            getTranslation("web-padawan.catalog.contextMenu.properties.dialog.entryName")
        )
        val idTextField = getPropertiesDialogTextField(
            catalogEntry.attrId.toString(),
            getTranslation("web-padawan.catalog.contextMenu.properties.dialog.entryID")
        )
        val typeTextField = getPropertiesDialogTextField(
            catalogEntry.dataType.toString(),
            getTranslation("web-padawan.catalog.contextMenu.properties.dialog.entryType")
        )
        val externalIDTextField = getPropertiesDialogTextField(
            catalogEntry.extID,
            getTranslation("web-padawan.catalog.contextMenu.properties.dialog.entryExternalID")
        )
        val projectTextField = getPropertiesDialogTextField(
            catalogEntry.project,
            getTranslation("web-padawan.catalog.contextMenu.properties.dialog.entryProject")
        )
        val creationTimeTextField = getPropertiesDialogTextField(
            catalogEntry.creationTime.toString(),
            getTranslation("web-padawan.catalog.contextMenu.properties.dialog.entryCreationTime")
        )
        popupContent.add(
            horizontalHeaderLayout,
            nameTextField,
            idTextField,
            typeTextField,
            externalIDTextField,
            projectTextField,
            creationTimeTextField
        )
        return popupContent
    }
}