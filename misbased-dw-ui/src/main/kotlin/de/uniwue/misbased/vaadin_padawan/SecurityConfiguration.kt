package de.uniwue.misbased.vaadin_padawan

import com.vaadin.flow.spring.security.VaadinWebSecurity
import de.uniwue.dw.core.client.authentication.DWAuthenticator
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.views.LoginView
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Configuration
import org.springframework.security.authentication.*
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.builders.WebSecurity
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity
import org.springframework.security.core.Authentication
import org.springframework.security.core.AuthenticationException
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.stereotype.Component
import javax.security.auth.login.AccountException


@EnableWebSecurity
@Configuration
open class SecurityConfiguration : VaadinWebSecurity() {

    @Autowired
    private val customAuthenticationProvider: CustomAuthenticationProvider = CustomAuthenticationProvider()

    @Throws(Exception::class)
    override fun configure(http: HttpSecurity) {
        http
            .authorizeHttpRequests { auth ->
                auth.requestMatchers("/rest/", "/rest/authentication/login", "/rest/authentication/checkToken")
                    .permitAll()
                auth.requestMatchers("/rest/**").authenticated()
                auth.requestMatchers("/banana/**").permitAll()
            }
            .csrf { csrf ->
                csrf.ignoringRequestMatchers("/rest/**")
            }
            .authenticationProvider(customAuthenticationProvider)

        super.configure(http)

        setLoginView(http, LoginView::class.java)
    }

    @Throws(Exception::class)
    override fun configure(web: WebSecurity?) {
        super.configure(web)
    }
}

@Component
class CustomAuthenticationProvider : AuthenticationProvider {

    @Throws(AuthenticationException::class)
    override fun authenticate(authentication: Authentication): Authentication {
        val username: String = authentication.name
        val password: String = authentication.credentials.toString()

        val user = try {
            PaDaWaNConnector.getAuthManager()
            DWAuthenticator.getUser(username, password)
        } catch (_: AccountException) {
            throw BadCredentialsException("No account found for these credentials")
        } catch (_: javax.naming.AuthenticationException) {
            throw AuthenticationServiceException("No authentication service online")
        } catch (e: Exception) {
            throw InternalAuthenticationServiceException(e.message)
        }

        if (user == null)
            throw BadCredentialsException("No account found for these credentials")
        if (user.groups.isNullOrEmpty())
            throw InsufficientAuthenticationException("The account is not in any known group")

        PaDaWaNConnector.addAuthenticatedUser(username, user)
        return UsernamePasswordAuthenticationToken(
            username,
            password,
            user.groups.map { SimpleGrantedAuthority("ROLE_${it.name}") }
        )
    }

    override fun supports(aClass: Class<*>): Boolean {
        return aClass == UsernamePasswordAuthenticationToken::class.java
    }
}