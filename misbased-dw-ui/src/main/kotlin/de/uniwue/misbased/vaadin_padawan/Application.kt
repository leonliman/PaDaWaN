package de.uniwue.misbased.vaadin_padawan

import com.vaadin.flow.component.page.AppShellConfigurator
import com.vaadin.flow.component.page.Push
import com.vaadin.flow.server.AppShellSettings
import com.vaadin.flow.theme.Theme
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.runApplication
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer

@SpringBootApplication
@Theme(value = "vaadin-padawan")
@Push
open class Application : SpringBootServletInitializer(), AppShellConfigurator {

    private val usableLogger = LoggerFactory.getLogger(Application::class.java)

    @Value("\${misbased.dw.properties-file-location}")
    private lateinit var propertiesFileLocation: String

    @PostConstruct
    fun setup() {
        PaDaWaNConnector.setup(propertiesFileLocation)
        usableLogger.info("Application has been set up successfully")
    }

    @PreDestroy
    fun dispose() {
        PaDaWaNConnector.dispose()
        usableLogger.info("Application has been disposed successfully")
    }

    override fun configurePage(settings: AppShellSettings?) {
        settings?.setPageTitle("Web-PaDaWaN")
        settings?.addFavIcon("icon", "VAADIN/themes/vaadin-padawan/icons/padawan.png", "640x512")
        settings?.addLink("shortcut icon", "VAADIN/themes/vaadin-padawan/icons/favicon.ico")
    }

    override fun configure(builder: SpringApplicationBuilder): SpringApplicationBuilder {
        return builder.sources(Application::class.java)
    }
}

fun main(args: Array<String>) {
    runApplication<Application>(*args)
}