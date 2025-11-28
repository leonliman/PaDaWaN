package de.uniwue.misbased.vaadin_padawan.views

import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.html.H2
import com.vaadin.flow.component.html.H4
import com.vaadin.flow.component.login.LoginForm
import com.vaadin.flow.component.login.LoginI18n
import com.vaadin.flow.component.orderedlayout.FlexComponent
import com.vaadin.flow.component.orderedlayout.FlexComponent.JustifyContentMode
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.router.BeforeEnterEvent
import com.vaadin.flow.router.BeforeEnterObserver
import com.vaadin.flow.router.PageTitle
import com.vaadin.flow.router.Route
import com.vaadin.flow.server.auth.AnonymousAllowed
import com.vaadin.flow.spring.security.AuthenticationContext


@Route("login")
@PageTitle("Web-PaDaWaN - Login")
@AnonymousAllowed
class LoginView(@Transient private val authContext: AuthenticationContext) : VerticalLayout(), BeforeEnterObserver {

    private val login = LoginForm()

    init {
        addClassName("login-view")
        setSizeFull()

        justifyContentMode = JustifyContentMode.CENTER
        alignItems = FlexComponent.Alignment.CENTER

        login.action = "login"
        login.isForgotPasswordButtonVisible = false

        val loginI18n = LoginI18n.createDefault()
        loginI18n.form.title = getTranslation("web-padawan.login.title")
        loginI18n.form.username = getTranslation("web-padawan.login.username")
        loginI18n.form.password = getTranslation("web-padawan.login.password")
        loginI18n.form.submit = getTranslation("web-padawan.login.submit")
        loginI18n.errorMessage.username = getTranslation("web-padawan.login.username.error")
        loginI18n.errorMessage.password = getTranslation("web-padawan.login.password.error")
        loginI18n.errorMessage.title = getTranslation("web-padawan.login.error.title")
        loginI18n.errorMessage.message = getTranslation("web-padawan.login.error.message")
        login.setI18n(loginI18n)

        val title = H2(getTranslation("web-padawan.title", MainView.VERSION_STRING))

        val alreadyLoggedIn = H4(getTranslation("web-padawan.login.alreadyLoggedIn"))
        val redirectToMainPageButton = Button(getTranslation("web-padawan.login.redirectToMainPage")) {
            ui.get().navigate("")
        }

        if (authContext.isAuthenticated)
            add(title, alreadyLoggedIn, redirectToMainPageButton)
        else
            add(title, login)
    }

    override fun beforeEnter(beforeEnterEvent: BeforeEnterEvent) {
        if (beforeEnterEvent.location
                .queryParameters
                .parameters
                .containsKey("error")
        )
            login.isError = true
    }
}