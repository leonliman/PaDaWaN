package de.uniwue.misbased.vaadin_padawan.views.query

import com.vaadin.flow.component.UI
import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.checkbox.Checkbox
import com.vaadin.flow.component.dialog.Dialog
import com.vaadin.flow.component.html.H4
import com.vaadin.flow.component.icon.FontIcon
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.component.select.Select
import com.vaadin.flow.component.textfield.IntegerField
import com.vaadin.flow.data.binder.Binder
import com.vaadin.flow.theme.lumo.LumoUtility
import de.uniwue.dw.core.client.authentication.*
import de.uniwue.misbased.vaadin_padawan.data.FONT_ICON_FAMILY
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNUserSettingsConnector

class SettingsDialog : Dialog() {

    init {
        width = "400px"
        isCloseOnOutsideClick = false

        headerTitle = getTranslation("web-padawan.settings.title")

        val user = PaDaWaNConnector.getUser()
        val ui = UI.getCurrent()
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(user, ui.locale)
        val contentLayout = createContentLayout(userSettings, user)
        contentLayout.setWidthFull()
        add(contentLayout)
    }

    private fun createContentLayout(userSettings: UserSettings, user: User): VerticalLayout {
        val contentLayout = VerticalLayout()
        contentLayout.isPadding = false
        contentLayout.isSpacing = true

        contentLayout.addClassNames(
            LumoUtility.Border.TOP,
            LumoUtility.BorderColor.CONTRAST_30,
            LumoUtility.Padding.Top.MEDIUM
        )

        val catalogSectionTitle = H4(getTranslation("web-padawan.settings.catalogSectionTitle"))

        val binder = Binder(UserSettings::class.java)
        val userSettingsCopy = UserSettings(
            userSettings.catalogMinOccurrenceThreshold,
            userSettings.catalogCountType,
            userSettings.catalogSearchMaxItems,
            userSettings.catalogShowMetadata,
            userSettings.userInterfaceType,
            userSettings.userInterfaceLanguage,
            userSettings.userInterfaceUseSessionStorage,
            userSettings.userInterfaceUsePlugins
        )
        binder.bean = userSettingsCopy
        val minimumCountPerEntryField = createMinimumCountPerEntryField(binder)
        minimumCountPerEntryField.setWidthFull()
        val showCatalogEntryPropertiesCheckbox = createShowCatalogEntryPropertiesCheckbox(binder)
        showCatalogEntryPropertiesCheckbox.setWidthFull()
        val maximumNumberOfSearchResultsField = createMaximumNumberOfSearchResultsField(binder)
        maximumNumberOfSearchResultsField.setWidthFull()
        maximumNumberOfSearchResultsField.addClassNames(
            LumoUtility.Border.BOTTOM,
            LumoUtility.BorderColor.CONTRAST_30,
            LumoUtility.Padding.Bottom.MEDIUM
        )

        val interfaceSectionTitle = H4(getTranslation("web-padawan.settings.interfaceSectionTitle"))
        val interfaceTypeSelect = createInterfaceTypeSelect(binder)
        interfaceTypeSelect.setWidthFull()
        val interfaceLanguageSelect = createInterfaceLanguageSelect(binder)
        interfaceLanguageSelect.setWidthFull()
        val useSessionStorageCheckbox = createUseSessionStorageCheckbox(binder)
        useSessionStorageCheckbox.setWidthFull()
        useSessionStorageCheckbox.addClassNames(
            LumoUtility.Border.BOTTOM,
            LumoUtility.BorderColor.CONTRAST_30,
            LumoUtility.Padding.Bottom.MEDIUM
        )

        val bottomButtonsLayout = createBottomButtonsLayout(binder)
        bottomButtonsLayout.setWidthFull()

        contentLayout.add(
            catalogSectionTitle,
            minimumCountPerEntryField,
            showCatalogEntryPropertiesCheckbox
        )
        val countTypeSelect = createCountTypeSelect(binder)
        countTypeSelect.setWidthFull()
        contentLayout.add(countTypeSelect)
        if (user.isAdmin)
            contentLayout.add(maximumNumberOfSearchResultsField)
        contentLayout.add(
            interfaceSectionTitle,
            interfaceTypeSelect,
            interfaceLanguageSelect,
            useSessionStorageCheckbox
        )
        contentLayout.add(bottomButtonsLayout)

        return contentLayout
    }

    private fun createMinimumCountPerEntryField(binder: Binder<UserSettings>): IntegerField {
        val minBound = 0
        val minimumCountPerEntryField = IntegerField(getTranslation("web-padawan.settings.minimumCountPerEntry"))

        val errorMessage = getTranslation("web-padawan.settings.numberEntryError", minBound)
        binder.forField(minimumCountPerEntryField)
            .asRequired(errorMessage)
            .withValidator({ it >= minBound }, errorMessage)
            .bind(UserSettings::getCatalogMinOccurrenceThreshold, UserSettings::setCatalogMinOccurrenceThreshold)
        return minimumCountPerEntryField
    }

    private fun createShowCatalogEntryPropertiesCheckbox(binder: Binder<UserSettings>): Checkbox {
        val showCatalogEntryPropertiesCheckbox =
            Checkbox(getTranslation("web-padawan.settings.showCatalogEntryProperties"))

        binder.forField(showCatalogEntryPropertiesCheckbox)
            .bind(UserSettings::getCatalogShowMetadata, UserSettings::setCatalogShowMetadata)
        return showCatalogEntryPropertiesCheckbox
    }

    private fun createCountTypeSelect(binder: Binder<UserSettings>): Select<CountType> {
        val countTypeSelect = Select<CountType>()
        countTypeSelect.label = getTranslation("web-padawan.settings.countType")
        countTypeSelect.isEmptySelectionAllowed = false
        countTypeSelect.setItems(CountType.distinctCaseID, CountType.distinctPID)
        countTypeSelect.setItemLabelGenerator { countType ->
            when (countType) {
                CountType.distinctCaseID -> getTranslation("web-padawan.settings.countType.cases")
                CountType.distinctPID -> getTranslation("web-padawan.settings.countType.patients")
                else -> getTranslation("web-padawan.settings.countType.unknown")
            }
        }

        val errorMessage = getTranslation("web-padawan.settings.selectError")
        binder.forField(countTypeSelect)
            .asRequired(errorMessage)
            .bind(UserSettings::getCatalogCountType, UserSettings::setCatalogCountType)
        return countTypeSelect
    }

    private fun createMaximumNumberOfSearchResultsField(binder: Binder<UserSettings>): IntegerField {
        val minBound = 1
        val maximumNumberOfSearchResultsField =
            IntegerField(getTranslation("web-padawan.settings.maximumNumberOfSearchResults"))

        val errorMessage = getTranslation("web-padawan.settings.numberEntryError", minBound)
        binder.forField(maximumNumberOfSearchResultsField)
            .asRequired(errorMessage)
            .withValidator({ it >= minBound }, errorMessage)
            .bind(UserSettings::getCatalogSearchMaxItems, UserSettings::setCatalogSearchMaxItems)
        return maximumNumberOfSearchResultsField
    }

    private fun createInterfaceTypeSelect(binder: Binder<UserSettings>): Select<UserInterfaceType> {
        val interfaceTypeSelect = Select<UserInterfaceType>()
        interfaceTypeSelect.label = getTranslation("web-padawan.settings.interfaceTheme")
        interfaceTypeSelect.isEmptySelectionAllowed = false
        interfaceTypeSelect.setItems(UserInterfaceType.light, UserInterfaceType.dark)
        interfaceTypeSelect.setItemLabelGenerator { userInterfaceType ->
            when (userInterfaceType) {
                UserInterfaceType.light -> getTranslation("web-padawan.settings.interfaceTheme.light")
                UserInterfaceType.dark -> getTranslation("web-padawan.settings.interfaceTheme.dark")
                else -> getTranslation("web-padawan.settings.interfaceTheme.unknown")
            }
        }

        val errorMessage = getTranslation("web-padawan.settings.selectError")
        binder.forField(interfaceTypeSelect)
            .asRequired(errorMessage)
            .bind(UserSettings::getUserInterfaceType, UserSettings::setUserInterfaceType)
        return interfaceTypeSelect
    }

    private fun createInterfaceLanguageSelect(binder: Binder<UserSettings>): Select<UserInterfaceLanguage> {
        val interfaceLanguageSelect = Select<UserInterfaceLanguage>()
        interfaceLanguageSelect.label = getTranslation("web-padawan.settings.interfaceLanguage")
        interfaceLanguageSelect.isEmptySelectionAllowed = false
        interfaceLanguageSelect.setItems(UserInterfaceLanguage.german, UserInterfaceLanguage.english)
        interfaceLanguageSelect.setItemLabelGenerator { userInterfaceLanguage ->
            when (userInterfaceLanguage) {
                UserInterfaceLanguage.german -> getTranslation("web-padawan.settings.interfaceLanguage.german")
                UserInterfaceLanguage.english -> getTranslation("web-padawan.settings.interfaceLanguage.english")
                else -> getTranslation("web-padawan.settings.interfaceLanguage.unknown")
            }
        }

        val errorMessage = getTranslation("web-padawan.settings.selectError")
        binder.forField(interfaceLanguageSelect)
            .asRequired(errorMessage)
            .bind(UserSettings::getUserInterfaceLanguage, UserSettings::setUserInterfaceLanguage)
        return interfaceLanguageSelect
    }

    private fun createUseSessionStorageCheckbox(binder: Binder<UserSettings>): Checkbox {
        val useSessionStorageCheckbox = Checkbox(getTranslation("web-padawan.settings.useSessionStorage"))

        binder.forField(useSessionStorageCheckbox)
            .bind(UserSettings::getUserInterfaceUseSessionStorage, UserSettings::setUserInterfaceUseSessionStorage)
        return useSessionStorageCheckbox
    }

    private fun createBottomButtonsLayout(binder: Binder<UserSettings>): HorizontalLayout {
        val bottomButtonsLayout = HorizontalLayout()
        val cancelButton = createCancelButton()
        val saveButton = createSaveButton(binder, cancelButton)
        bottomButtonsLayout.addAndExpand(saveButton, cancelButton)
        return bottomButtonsLayout
    }

    private fun createSaveButton(binder: Binder<UserSettings>, cancelButton: Button): Button {
        val saveIcon = FontIcon(FONT_ICON_FAMILY, "fa-check")
        val saveButton = Button(getTranslation("web-padawan.settings.save"), saveIcon)
        saveButton.addClickListener {
            if (binder.validate().isOk) {
                saveButton.isEnabled = false
                cancelButton.isEnabled = false
                saveButton.text = getTranslation("web-padawan.settings.save.progress")
                QueryAttributesCustomField.forceUIResize()

                val ui = UI.getCurrent()
                val user = PaDaWaNConnector.getUser()
                val saveThread = Thread {
                    PaDaWaNUserSettingsConnector.saveUserSettings(binder.bean, user, ui)
                    ui.access {
                        close()
                        ui.page.reload()
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