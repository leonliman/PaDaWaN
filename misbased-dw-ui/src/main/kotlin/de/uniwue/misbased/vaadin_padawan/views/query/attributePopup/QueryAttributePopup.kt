package de.uniwue.misbased.vaadin_padawan.views.query.attributePopup

import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.html.H3
import com.vaadin.flow.component.html.H4
import com.vaadin.flow.component.icon.FontIcon
import com.vaadin.flow.component.orderedlayout.FlexLayout
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.component.popover.Popover
import com.vaadin.flow.component.popover.PopoverVariant
import com.vaadin.flow.data.binder.Binder
import com.vaadin.flow.theme.lumo.LumoUtility
import de.uniwue.dw.core.model.data.CatalogEntry
import de.uniwue.dw.core.model.data.CatalogEntryType
import de.uniwue.misbased.vaadin_padawan.data.*
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributeContainer
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributeData
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNUserSettingsConnector
import de.uniwue.misbased.vaadin_padawan.views.catalog.CatalogEntryPropertiesPopup
import de.uniwue.misbased.vaadin_padawan.views.catalog.CatalogTree
import de.uniwue.misbased.vaadin_padawan.views.query.QueryAttributeButton
import de.uniwue.misbased.vaadin_padawan.views.query.QueryAttributesCustomField
import java.util.*

class QueryAttributePopup(
    attributeButton: QueryAttributeButton,
    attributeContainer: QueryAttributeContainer,
    attributesFlexLayout: FlexLayout,
    parentLayout: HorizontalLayout,
    queryAttributesCustomField: QueryAttributesCustomField,
    rowID: UUID,
    private val attributesSessionKey: String,
    private val catalogTree: CatalogTree
) : Popover() {

    init {
        isAutofocus = true
        isCloseOnEsc = false
        isCloseOnOutsideClick = true
        addThemeVariants(PopoverVariant.ARROW)

        isModal = true
        isBackdropVisible = true

        val contentLayout = createContentLayout(
            attributeButton,
            attributeContainer,
            attributesFlexLayout,
            parentLayout,
            queryAttributesCustomField,
            rowID
        )
        add(contentLayout)

        addOpenedChangeListener {
            if (!it.isOpened) {
                attributesFlexLayout.remove(this)
            }
            QueryAttributesCustomField.forceUIResize()
        }
    }

    private fun createContentLayout(
        attributeButton: QueryAttributeButton,
        attributeContainer: QueryAttributeContainer,
        attributesFlexLayout: FlexLayout,
        parentLayout: HorizontalLayout,
        queryAttributesCustomField: QueryAttributesCustomField,
        rowID: UUID
    ): VerticalLayout {
        val contentLayout = VerticalLayout()
        contentLayout.isPadding = true
        contentLayout.isSpacing = true

        val headerTitle = attributeContainer.catalogEntry!!.name

        val header = H3(headerTitle)
        header.setWidthFull()
        contentLayout.add(header)

        val binder = Binder(QueryAttributeData::class.java)
        val queryAttributeDataCopy = attributeContainer.queryAttributeData.copy()
        binder.bean = queryAttributeDataCopy

        val showEntryInCatalogButtonLayout = createShowEntryInCatalogButtonLayout(attributeContainer.catalogEntry)
        showEntryInCatalogButtonLayout.addClassNames(
            LumoUtility.Border.TOP,
            LumoUtility.BorderColor.CONTRAST_30,
            LumoUtility.Padding.Top.MEDIUM
        )
        showEntryInCatalogButtonLayout.setWidthFull()
        contentLayout.add(showEntryInCatalogButtonLayout)

        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        if (userSettings.catalogShowMetadata) {
            val catalogEntryPropertiesSection = createCatalogEntryPropertiesSection(attributeContainer.catalogEntry)
            catalogEntryPropertiesSection.addClassNames(
                LumoUtility.Border.TOP,
                LumoUtility.BorderColor.CONTRAST_30,
                LumoUtility.Padding.Top.MEDIUM
            )
            catalogEntryPropertiesSection.setWidthFull()
            contentLayout.add(catalogEntryPropertiesSection)
        }

        if (getQuerySection() == QuerySection.IndividualData) {
            val showSection = QueryAttributePopupShowSection(binder)
            showSection.setWidthFull()
            showSection.addClassNames(
                LumoUtility.Border.TOP,
                LumoUtility.BorderColor.CONTRAST_30,
                LumoUtility.Padding.Top.MEDIUM
            )
            contentLayout.add(showSection)
        }

        val restrictionSection = QueryAttributePopupRestrictionSection(
            binder, getQuerySection(), attributeContainer.catalogEntry
        )
        restrictionSection.setWidthFull()
        restrictionSection.addClassNames(
            LumoUtility.Border.TOP,
            LumoUtility.Border.BOTTOM,
            LumoUtility.BorderColor.CONTRAST_30,
            LumoUtility.Padding.Top.MEDIUM,
            LumoUtility.Padding.Bottom.MEDIUM
        )
        contentLayout.add(restrictionSection)

        if ((attributeContainer.catalogEntry.dataType == CatalogEntryType.Number) &&
            setOf(QuerySection.Rows, QuerySection.Columns).contains(getQuerySection())
        ) {
            val correlationAnalysisSection = createCorrelationAnalysisSection(binder)
            correlationAnalysisSection.addClassNames(
                LumoUtility.Border.BOTTOM,
                LumoUtility.BorderColor.CONTRAST_30,
                LumoUtility.Padding.Bottom.MEDIUM
            )
            correlationAnalysisSection.setWidthFull()
            contentLayout.add(correlationAnalysisSection)
        }

        val bottomButtonsLayout = QueryAttributePopupBottomButtonsLayout(
            binder,
            attributeButton,
            attributesFlexLayout,
            parentLayout,
            queryAttributesCustomField,
            rowID,
            attributeContainer.containerID,
            this
        )
        bottomButtonsLayout.setWidthFull()
        contentLayout.add(bottomButtonsLayout)

        return contentLayout
    }

    private fun createShowEntryInCatalogButtonLayout(catalogEntry: CatalogEntry): HorizontalLayout {
        val showEntryInCatalogButtonLayout = HorizontalLayout()
        val searchIcon = FontIcon(FONT_ICON_FAMILY, "fa-search")
        val showEntryInCatalogButton =
            Button(getTranslation("web-padawan.queryAttributeDialog.showEntryInCatalog"), searchIcon)
        showEntryInCatalogButton.addClickListener {
            catalogTree.performCatalogSearch("") {
                var parentEntry = catalogEntry.parent
                val parentEntries = mutableListOf<CatalogEntry>()
                while (parentEntry != null && !parentEntry.isRoot) {
                    parentEntries.add(0, parentEntry)
                    parentEntry = parentEntry.parent
                }
                catalogTree.expand(parentEntries)
                catalogTree.select(catalogEntry)
                val entryIndices = catalogTree.getIndicesForItems(parentEntries, catalogEntry)
                catalogTree.scrollToIndex(*entryIndices)
                QueryAttributesCustomField.forceUIResize(400)
                close()
            }.start()
        }
        showEntryInCatalogButtonLayout.addAndExpand(showEntryInCatalogButton)
        return showEntryInCatalogButtonLayout
    }

    private fun createCatalogEntryPropertiesSection(catalogEntry: CatalogEntry): VerticalLayout {
        val catalogEntryPropertiesSection = VerticalLayout()
        catalogEntryPropertiesSection.isPadding = false
        catalogEntryPropertiesSection.isSpacing = false

        val sectionTitle = H4(getTranslation("web-padawan.catalog.contextMenu.properties.dialog.title"))

        val externalIDTextField = CatalogEntryPropertiesPopup.getPropertiesDialogTextField(
            catalogEntry.extID,
            getTranslation("web-padawan.catalog.contextMenu.properties.dialog.entryExternalID")
        )
        val projectTextField = CatalogEntryPropertiesPopup.getPropertiesDialogTextField(
            catalogEntry.project,
            getTranslation("web-padawan.catalog.contextMenu.properties.dialog.entryProject")
        )
        val typeTextField = CatalogEntryPropertiesPopup.getPropertiesDialogTextField(
            catalogEntry.dataType.toString(),
            getTranslation("web-padawan.catalog.contextMenu.properties.dialog.entryType")
        )
        catalogEntryPropertiesSection.add(
            sectionTitle,
            externalIDTextField,
            projectTextField,
            typeTextField
        )
        return catalogEntryPropertiesSection
    }

    private fun createCorrelationAnalysisSection(binder: Binder<QueryAttributeData>): VerticalLayout {
        val correlationAnalysisSection = VerticalLayout()
        correlationAnalysisSection.isPadding = false
        correlationAnalysisSection.isSpacing = false

        val useForCorrelationAnalysisCheckbox = QueryAttributePopupShowSection.createAttributePopupCheckbox(
            binder,
            "web-padawan.queryAttributeDialog.correlation.useAttribute",
            QueryAttributeData::useForCorrelationAnalysis::get,
            QueryAttributeData::useForCorrelationAnalysis::set
        )
        useForCorrelationAnalysisCheckbox.setWidthFull()

        correlationAnalysisSection.add(useForCorrelationAnalysisCheckbox)
        return correlationAnalysisSection
    }

    enum class QuerySection {
        Rows,
        Columns,
        Filters,
        IndividualData
    }

    private fun getQuerySection(): QuerySection {
        return when (attributesSessionKey) {
            SESSION_ATTRIBUTE_QUERY_ROWS -> QuerySection.Rows
            SESSION_ATTRIBUTE_QUERY_COLUMNS -> QuerySection.Columns
            SESSION_ATTRIBUTE_QUERY_FILTERS -> QuerySection.Filters
            SESSION_ATTRIBUTE_QUERY_ROWS_INDIVIDUAL_DATA -> QuerySection.IndividualData
            else -> throw IllegalArgumentException("Invalid attributesSessionKey: $attributesSessionKey")
        }
    }
}