package de.uniwue.misbased.vaadin_padawan.views.query.images

import com.vaadin.flow.component.UI
import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.checkbox.CheckboxGroup
import com.vaadin.flow.component.checkbox.CheckboxGroupVariant
import com.vaadin.flow.component.dialog.Dialog
import com.vaadin.flow.component.html.H4
import com.vaadin.flow.component.icon.FontIcon
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.component.select.Select
import com.vaadin.flow.data.binder.Binder
import com.vaadin.flow.theme.lumo.LumoUtility
import de.uniwue.dw.core.client.authentication.User
import de.uniwue.misbased.vaadin_padawan.data.FONT_ICON_FAMILY
import de.uniwue.misbased.vaadin_padawan.data.model.PACSExportType
import de.uniwue.misbased.vaadin_padawan.data.model.PACSModality
import de.uniwue.misbased.vaadin_padawan.data.model.PACSUserSettings
import de.uniwue.misbased.vaadin_padawan.data.pacsConnector.PACSConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.views.query.QueryAttributesCustomField

class PACSSettingsDialog : Dialog() {

    init {
        width = "460px"
        isCloseOnOutsideClick = false

        headerTitle = getTranslation("web-padawan.pacs.settings.title")

        val user = PaDaWaNConnector.getUser()
        val pacsUserSettings = PACSConnector.getPACSSettings(user)
        val contentLayout = createContentLayout(pacsUserSettings, user)
        contentLayout.setWidthFull()
        add(contentLayout)
    }

    private fun createContentLayout(pacsUserSettings: PACSUserSettings, user: User): VerticalLayout {
        val contentLayout = VerticalLayout()
        contentLayout.isPadding = false
        contentLayout.isSpacing = true

        contentLayout.addClassNames(
            LumoUtility.Border.TOP,
            LumoUtility.BorderColor.CONTRAST_30,
            LumoUtility.Padding.Top.MEDIUM
        )

        val binder = Binder(PACSUserSettings::class.java)
        val pacsUserSettingsCopy = pacsUserSettings.copy()
        binder.bean = pacsUserSettingsCopy

        val modalitiesCheckboxGroup = createModalitiesCheckboxGroup(binder)
        modalitiesCheckboxGroup.setWidthFull()
        modalitiesCheckboxGroup.addClassNames(
            LumoUtility.Border.BOTTOM,
            LumoUtility.BorderColor.CONTRAST_30,
            LumoUtility.Padding.Bottom.MEDIUM
        )

        val furtherSettingsTitle = H4(getTranslation("web-padawan.furtherSettings"))
        val exportTypeSelect = createExportTypeSelect(binder)
        exportTypeSelect.setWidthFull()

        val bottomButtonsLayout = createBottomButtonsLayout(binder, user)
        bottomButtonsLayout.setWidthFull()
        bottomButtonsLayout.addClassNames(
            LumoUtility.Border.TOP,
            LumoUtility.BorderColor.CONTRAST_30,
            LumoUtility.Padding.Top.MEDIUM
        )

        contentLayout.add(
            modalitiesCheckboxGroup,
            furtherSettingsTitle,
            exportTypeSelect,
            bottomButtonsLayout
        )

        return contentLayout
    }

    private fun createModalitiesCheckboxGroup(binder: Binder<PACSUserSettings>): CheckboxGroup<PACSModality> {
        val modalitiesCheckboxGroup = CheckboxGroup<PACSModality>()
        modalitiesCheckboxGroup.label = getTranslation("web-padawan.pacs.settings.modalities")
        modalitiesCheckboxGroup.addThemeVariants(CheckboxGroupVariant.LUMO_VERTICAL)
        modalitiesCheckboxGroup.setItems(binder.bean.allModalities)
        modalitiesCheckboxGroup.setItemLabelGenerator {
            "${getTranslation(it.longTextTranslationKey)} (${it.shortText})"
        }
        val errorMessage = getTranslation("web-padawan.pacs.settings.modalities.error")
        binder.forField(modalitiesCheckboxGroup)
            .asRequired(errorMessage)
            .bind(PACSUserSettings::enabledModalities::get, PACSUserSettings::enabledModalities::set)
        return modalitiesCheckboxGroup
    }

    private fun createExportTypeSelect(binder: Binder<PACSUserSettings>): Select<PACSExportType> {
        val exportTypeSelect = Select<PACSExportType>()
        exportTypeSelect.label = getTranslation("web-padawan.pacs.settings.furtherSettings.exportFormat")
        exportTypeSelect.isEmptySelectionAllowed = false
        exportTypeSelect.setItems(PACSExportType.entries)
        exportTypeSelect.setItemLabelGenerator { exportType ->
            when (exportType) {
                PACSExportType.DCM -> "DICOM"
                PACSExportType.JPG -> "JPEG"
            }
        }

        val errorMessage = getTranslation("web-padawan.pacs.settings.furtherSettings.exportFormat.error")
        binder.forField(exportTypeSelect)
            .asRequired(errorMessage)
            .bind(PACSUserSettings::exportType::get, PACSUserSettings::exportType::set)
        return exportTypeSelect
    }

    private fun createBottomButtonsLayout(binder: Binder<PACSUserSettings>, user: User): HorizontalLayout {
        val bottomButtonsLayout = HorizontalLayout()
        val cancelButton = createCancelButton()
        val saveButton = createSaveButton(binder, cancelButton, user)
        bottomButtonsLayout.addAndExpand(saveButton, cancelButton)
        return bottomButtonsLayout
    }

    private fun createSaveButton(binder: Binder<PACSUserSettings>, cancelButton: Button, user: User): Button {
        val saveIcon = FontIcon(FONT_ICON_FAMILY, "fa-check")
        val saveButton = Button(getTranslation("web-padawan.settings.save"), saveIcon)
        saveButton.addClickListener {
            if (binder.validate().isOk) {
                saveButton.isEnabled = false
                cancelButton.isEnabled = false
                saveButton.text = getTranslation("web-padawan.settings.save.progress")
                QueryAttributesCustomField.forceUIResize()

                val ui = UI.getCurrent()
                val saveThread = Thread {
                    PACSConnector.savePACSSettings(user, binder.bean)
                    ui.access {
                        close()
                    }
                }
                saveThread.start()
            }
        }
        return saveButton
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