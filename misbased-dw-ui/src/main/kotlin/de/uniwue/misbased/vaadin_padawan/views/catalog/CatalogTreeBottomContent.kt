package de.uniwue.misbased.vaadin_padawan.views.catalog

import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.spring.security.AuthenticationContext
import com.vaadin.flow.theme.lumo.LumoUtility
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector

class CatalogTreeBottomContent(@Transient private val authContext: AuthenticationContext) : HorizontalLayout() {

    init {
        addClassNames(
            LumoUtility.Border.TOP,
            LumoUtility.BorderColor.CONTRAST_30,
            LumoUtility.Padding.Top.MEDIUM
        )

        val logoutButton = createLogoutButton()
        logoutButton.setWidthFull()
        add(logoutButton)
    }

    private fun createLogoutButton(): Button {
        val logoutButton = Button(getTranslation("web-padawan.logout"))
        logoutButton.addClickListener {
            val username = authContext.principalName.get()
            PaDaWaNConnector.removeAuthenticatedUser(username)
            authContext.logout()
        }
        return logoutButton
    }
}