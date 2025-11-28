package de.uniwue.misbased.vaadin_padawan.data.padawanConnector

import com.vaadin.flow.component.UI
import com.vaadin.flow.server.VaadinSession
import de.uniwue.dw.core.client.authentication.User
import de.uniwue.dw.core.client.authentication.UserInterfaceLanguage
import de.uniwue.dw.core.client.authentication.UserSettings
import de.uniwue.dw.core.client.authentication.UserSettings.*
import de.uniwue.misbased.vaadin_padawan.data.SESSION_ATTRIBUTE_DEMO_MODE_USER_SETTINGS
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.*

@Component
object PaDaWaNUserSettingsConnector {

    private val usableLogger = LoggerFactory.getLogger(PaDaWaNUserSettingsConnector::class.java)

    fun getUserSettings(user: User, defaultLocale: Locale? = null, ui: UI? = null): UserSettings {
        if (PaDaWaNQueryConnector.querySavingIsDisabled())
            return getSessionUserSettings(user, defaultLocale, ui)

        if (
            user.settings.userInterfaceType == null || user.settings.userInterfaceLanguage == null ||
            user.settings.userInterfaceUseSessionStorage == null || user.settings.userInterfaceUsePlugins == null
        ) {
            if (user.settings.userInterfaceType == null)
                user.settings.userInterfaceType = USER_INTERFACE_TYPE_DEFAULT
            if (user.settings.userInterfaceLanguage == null)
                user.settings.userInterfaceLanguage = if (defaultLocale == null)
                    USER_INTERFACE_LANGUAGE_DEFAULT
                else if (defaultLocale.language.lowercase() == "de")
                    UserInterfaceLanguage.german
                else
                    UserInterfaceLanguage.english
            if (user.settings.userInterfaceUsePlugins == null)
                user.settings.userInterfaceUseSessionStorage = USER_INTERFACE_SESSION_STORAGE_DEFAULT
            if (user.settings.userInterfaceUsePlugins == null)
                user.settings.userInterfaceUsePlugins = USER_INTERFACE_USE_PLUGINS_DEFAULT
            val guiClient = PaDaWaNConnector.getGUIClient()
            guiClient.userManager.saveUserSettings(user, user.settings)
            usableLogger.info("User settings for interface type and language have been set to default values")
        }
        return user.settings
    }

    private fun getSessionUserSettings(user: User, defaultLocale: Locale? = null, ui: UI? = null): UserSettings {
        var settingsToUse = createDefaultUserSettings(user.isAdmin)
        if (defaultLocale != null) {
            settingsToUse.userInterfaceLanguage = if (defaultLocale.language.lowercase() == "de")
                UserInterfaceLanguage.german
            else
                UserInterfaceLanguage.english
        }
        if (ui != null)
            ui.access {
                settingsToUse =
                    VaadinSession.getCurrent().getAttribute(SESSION_ATTRIBUTE_DEMO_MODE_USER_SETTINGS) as? UserSettings
                        ?: settingsToUse
            }
        else
            settingsToUse =
                VaadinSession.getCurrent().getAttribute(SESSION_ATTRIBUTE_DEMO_MODE_USER_SETTINGS) as? UserSettings
                    ?: settingsToUse
        return settingsToUse
    }

    fun saveUserSettings(userSettings: UserSettings, user: User, ui: UI) {
        if (!user.isAdmin)
            userSettings.catalogSearchMaxItems = CATALOG_SEARCH_MAX_ITEMS_DEFAULT

        if (PaDaWaNQueryConnector.querySavingIsDisabled())
            saveSessionUserSettings(userSettings, ui)
        else {
            val guiClient = PaDaWaNConnector.getGUIClient()
            guiClient.userManager.saveUserSettings(user, userSettings)
            user.settings = userSettings
        }
    }

    private fun saveSessionUserSettings(userSettings: UserSettings, ui: UI) {
        ui.access {
            VaadinSession.getCurrent().setAttribute(SESSION_ATTRIBUTE_DEMO_MODE_USER_SETTINGS, userSettings)
        }
    }
}