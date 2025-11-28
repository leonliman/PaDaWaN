package de.uniwue.misbased.vaadin_padawan

import com.vaadin.flow.server.ServiceInitEvent
import com.vaadin.flow.server.VaadinServiceInitListener
import com.vaadin.flow.theme.lumo.Lumo
import de.uniwue.dw.core.client.authentication.UserInterfaceLanguage
import de.uniwue.dw.core.client.authentication.UserInterfaceType
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNUserSettingsConnector
import org.springframework.security.authentication.AnonymousAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.stereotype.Component
import java.util.*

@Component
class ServiceInitEventListener : VaadinServiceInitListener {
    override fun serviceInit(event: ServiceInitEvent?) {
        event?.source?.addSessionInitListener { sessionInitEvent ->
            sessionInitEvent.session.setErrorHandler(DefaultErrorHandler())
        }

        event?.source?.addUIInitListener { uiInitEvent ->
            uiInitEvent.ui.addBeforeEnterListener { beforeEnterEvent ->
                val authentication = SecurityContextHolder.getContext().authentication
                if (authentication != null && authentication !is AnonymousAuthenticationToken) {
                    val user = PaDaWaNConnector.getUser()
                    val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(user, beforeEnterEvent.ui.locale)
                    if (userSettings.userInterfaceType == UserInterfaceType.dark)
                        beforeEnterEvent.ui.element.themeList.add(Lumo.DARK)
                    when (userSettings.userInterfaceLanguage) {
                        UserInterfaceLanguage.german -> beforeEnterEvent.ui.session.locale = Locale.GERMAN
                        UserInterfaceLanguage.english -> beforeEnterEvent.ui.session.locale = Locale.ENGLISH
                        else -> {}
                    }
                }
            }
        }
    }
}