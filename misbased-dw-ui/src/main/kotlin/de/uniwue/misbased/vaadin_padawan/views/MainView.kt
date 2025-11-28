package de.uniwue.misbased.vaadin_padawan.views

import com.vaadin.flow.component.html.H4
import com.vaadin.flow.component.orderedlayout.FlexComponent
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.component.splitlayout.SplitLayout
import com.vaadin.flow.router.PageTitle
import com.vaadin.flow.router.Route
import com.vaadin.flow.spring.security.AuthenticationContext
import com.vaadin.flow.theme.lumo.LumoUtility
import de.uniwue.misbased.vaadin_padawan.views.catalog.CatalogView
import de.uniwue.misbased.vaadin_padawan.views.query.QueryView
import de.uniwue.misbased.vaadin_padawan.views.result.ResultView
import jakarta.annotation.security.PermitAll

@Route("")
@PageTitle("Web-PaDaWaN")
@PermitAll
class MainView(@Transient private val authContext: AuthenticationContext) : VerticalLayout() {

    companion object {
        const val VERSION_STRING = "3.0.3"
    }

    init {
        val catalog = CatalogView(authContext)
        val result = ResultView()
        val query = QueryView(catalog.catalogTree, result)

        val subSplitLayout = SplitLayout(query, result)
        subSplitLayout.orientation = SplitLayout.Orientation.VERTICAL
        subSplitLayout.splitterPosition = 70.0

        val splitLayout = SplitLayout(catalog, subSplitLayout)
        splitLayout.setSizeFull()
        splitLayout.splitterPosition = 20.0

        val titleLayout = createTitleLayout()

        setSizeFull()
        add(
            titleLayout,
            splitLayout
        )
    }

    private fun createTitleLayout(): HorizontalLayout {
        val horizontalLayout = HorizontalLayout()
        horizontalLayout.setWidthFull()
        horizontalLayout.addClassNames(
            LumoUtility.Border.BOTTOM,
            LumoUtility.BorderColor.CONTRAST_50,
            LumoUtility.Padding.Bottom.MEDIUM
        )
        horizontalLayout.justifyContentMode = FlexComponent.JustifyContentMode.CENTER

        val title = H4(getTranslation("web-padawan.title", VERSION_STRING))
        horizontalLayout.add(title)

        return horizontalLayout
    }

}