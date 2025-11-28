package de.uniwue.misbased.vaadin_padawan.views.query

import com.vaadin.flow.component.ComponentEvent
import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.checkbox.CheckboxGroup
import com.vaadin.flow.component.html.Div
import com.vaadin.flow.component.icon.FontIcon
import com.vaadin.flow.component.orderedlayout.FlexComponent
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.component.radiobutton.RadioButtonGroup
import com.vaadin.flow.component.textfield.IntegerField
import com.vaadin.flow.component.textfield.TextFieldVariant
import com.vaadin.flow.server.VaadinSession
import com.vaadin.flow.shared.Registration
import com.vaadin.flow.theme.lumo.LumoUtility
import de.uniwue.misbased.vaadin_padawan.data.*
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributesRowContainer
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNUserSettingsConnector

class QueryViewTopSettingsRow : HorizontalLayout() {

    private lateinit var queryModeRadioButtonGroup: RadioButtonGroup<QueryMode>
    private lateinit var referenceSetRadioButtonGroup: RadioButtonGroup<ReferenceSet>
    private lateinit var previewRowsIntegerField: IntegerField
    private lateinit var furtherSettingsCheckboxGroup: CheckboxGroup<FurtherSettings>

    private var currentQueryModeForNonSessionStorage = QueryMode.STATISTICS
    private var currentReferenceSetForNonSessionStorage = ReferenceSet.CASES
    private var currentPreviewRowsForNonSessionStorage = 10
    private var currentFurtherSettingsForNonSessionStorage = mutableSetOf<FurtherSettings>()

    private var queryAttributesRowContainersForNonSessionStorage = mutableMapOf<String, QueryAttributesRowContainer>()

    private var lastUsedQueryTitleForNonSessionStorage: String? = null

    init {
        defaultVerticalComponentAlignment = FlexComponent.Alignment.CENTER

        val querySpecificSettingsRow = createQuerySpecificSettingsRow()
        val spacer = Div()
        val settingsButton = createSettingsButton()

        add(
            querySpecificSettingsRow,
            spacer,
            settingsButton
        )
        expand(spacer)
    }

    private fun createQuerySpecificSettingsRow(): HorizontalLayout {
        val querySpecificSettingsRow = HorizontalLayout()
        querySpecificSettingsRow.isPadding = true
        querySpecificSettingsRow.addClassNames(
            LumoUtility.Border.ALL,
            LumoUtility.BorderColor.CONTRAST_50
        )

        val initialQueryMode = getCurrentQueryMode()
        queryModeRadioButtonGroup = createQueryModeRadioButtonGroup(initialQueryMode)
        referenceSetRadioButtonGroup = createReferenceSetRadioButtonGroup(initialQueryMode)
        previewRowsIntegerField = createPreviewRowsIntegerField(initialQueryMode)
        furtherSettingsCheckboxGroup = createFurtherSettingsCheckboxGroup()

        querySpecificSettingsRow.add(
            queryModeRadioButtonGroup,
            referenceSetRadioButtonGroup,
            previewRowsIntegerField,
            furtherSettingsCheckboxGroup
        )
        return querySpecificSettingsRow
    }

    class QueryModeChangedEvent(val queryMode: QueryMode, source: QueryViewTopSettingsRow?) :
        ComponentEvent<QueryViewTopSettingsRow>(source, false)

    fun addQueryModeChangedListener(listener: (QueryModeChangedEvent) -> Unit): Registration =
        eventBus.addListener(QueryModeChangedEvent::class.java, listener)

    fun getCurrentQueryMode(): QueryMode {
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        return if (userSettings.userInterfaceUseSessionStorage)
            VaadinSession.getCurrent().getAttribute(SESSION_ATTRIBUTE_QUERY_MODE) as? QueryMode ?: QueryMode.STATISTICS
        else
            currentQueryModeForNonSessionStorage
    }

    private fun setCurrentQueryMode(queryMode: QueryMode) {
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        if (userSettings.userInterfaceUseSessionStorage)
            VaadinSession.getCurrent().setAttribute(SESSION_ATTRIBUTE_QUERY_MODE, queryMode)
        else
            currentQueryModeForNonSessionStorage = queryMode
        eventBus.fireEvent(QueryModeChangedEvent(queryMode, this))
    }

    enum class QueryMode {
        STATISTICS,
        INDIVIDUAL_DATA
    }

    private fun createQueryModeRadioButtonGroup(initialQueryMode: QueryMode): RadioButtonGroup<QueryMode> {
        val queryModeRadioButtonGroup = RadioButtonGroup<QueryMode>()
        queryModeRadioButtonGroup.label = getTranslation("web-padawan.queryMode")
        queryModeRadioButtonGroup.setItems(QueryMode.entries)
        queryModeRadioButtonGroup.setItemLabelGenerator {
            when (it) {
                QueryMode.STATISTICS -> getTranslation("web-padawan.queryMode.statistics")
                QueryMode.INDIVIDUAL_DATA -> getTranslation("web-padawan.queryMode.individualData")
                else -> throw IllegalArgumentException("Unknown query mode: $it")
            }
        }
        queryModeRadioButtonGroup.value = initialQueryMode
        queryModeRadioButtonGroup.addValueChangeListener {
            setCurrentQueryMode(it.value)
        }
        queryModeRadioButtonGroup.addClassNames(
            LumoUtility.Border.RIGHT,
            LumoUtility.BorderColor.CONTRAST_30
        )
        val user = PaDaWaNConnector.getUser()
        if (!user.isAllowedToUseCaseQuery && !user.isAdmin)
            queryModeRadioButtonGroup.isVisible = false
        return queryModeRadioButtonGroup
    }

    fun updateQueryModeRadioButtonGroup(queryMode: QueryMode) {
        queryModeRadioButtonGroup.value = queryMode
    }

    fun getCurrentReferenceSet(queryMode: QueryMode): ReferenceSet {
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        val referenceSet = if (userSettings.userInterfaceUseSessionStorage)
            VaadinSession.getCurrent().getAttribute(SESSION_ATTRIBUTE_REFERENCE_SET) as? ReferenceSet
                ?: ReferenceSet.CASES
        else
            currentReferenceSetForNonSessionStorage
        return if (referenceSet == ReferenceSet.GROUPS && queryMode == QueryMode.STATISTICS) {
            setCurrentReferenceSet(ReferenceSet.DOCUMENTS)
            ReferenceSet.DOCUMENTS
        } else {
            referenceSet
        }
    }

    private fun setCurrentReferenceSet(referenceSet: ReferenceSet) {
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        if (userSettings.userInterfaceUseSessionStorage)
            VaadinSession.getCurrent().setAttribute(SESSION_ATTRIBUTE_REFERENCE_SET, referenceSet)
        else
            currentReferenceSetForNonSessionStorage = referenceSet
    }

    enum class ReferenceSet {
        PATIENTS,
        CASES,
        DOCUMENTS,
        GROUPS
    }

    private fun createReferenceSetRadioButtonGroup(initialQueryMode: QueryMode): RadioButtonGroup<ReferenceSet> {
        val referenceSetRadioButtonGroup = RadioButtonGroup<ReferenceSet>()
        referenceSetRadioButtonGroup.label = getTranslation("web-padawan.referenceSet")
        referenceSetRadioButtonGroup.setItems(ReferenceSet.entries)
        referenceSetRadioButtonGroup.setItemEnabledProvider {
            when (it) {
                ReferenceSet.PATIENTS, ReferenceSet.CASES, ReferenceSet.DOCUMENTS -> true
                ReferenceSet.GROUPS -> getCurrentQueryMode() == QueryMode.INDIVIDUAL_DATA
                else -> throw IllegalArgumentException("Unknown reference set: $it")
            }
        }
        referenceSetRadioButtonGroup.setItemLabelGenerator {
            getReferenceSetName(it)
        }
        referenceSetRadioButtonGroup.value = getCurrentReferenceSet(initialQueryMode)
        referenceSetRadioButtonGroup.addValueChangeListener {
            val newValue = it.value
            if (newValue != null)
                setCurrentReferenceSet(newValue)
        }

        referenceSetRadioButtonGroup.addClassName(LumoUtility.BorderColor.CONTRAST_30)
        referenceSetRadioButtonGroup.addClassName(LumoUtility.Border.RIGHT)

        addQueryModeChangedListener {
            referenceSetRadioButtonGroup.dataProvider.refreshAll()
            referenceSetRadioButtonGroup.value = getCurrentReferenceSet(it.queryMode)
        }
        return referenceSetRadioButtonGroup
    }

    fun getReferenceSetName(it: ReferenceSet?): String =
        when (it) {
            ReferenceSet.PATIENTS -> getTranslation("web-padawan.referenceSet.patients")
            ReferenceSet.CASES -> getTranslation("web-padawan.referenceSet.cases")
            ReferenceSet.DOCUMENTS -> getTranslation("web-padawan.referenceSet.documents")
            ReferenceSet.GROUPS -> getTranslation("web-padawan.referenceSet.groups")
            else -> throw IllegalArgumentException("Unknown reference set: $it")
        }

    fun updateReferenceSetRadioButtonGroup(referenceSet: ReferenceSet) {
        referenceSetRadioButtonGroup.value = referenceSet
    }

    fun getPreviewRows(): Int {
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        return if (userSettings.userInterfaceUseSessionStorage)
            VaadinSession.getCurrent().getAttribute(SESSION_ATTRIBUTE_PREVIEW_ROWS) as? Int ?: 10
        else
            currentPreviewRowsForNonSessionStorage
    }

    private fun setPreviewRows(previewRows: Int) {
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        if (userSettings.userInterfaceUseSessionStorage)
            VaadinSession.getCurrent().setAttribute(SESSION_ATTRIBUTE_PREVIEW_ROWS, previewRows)
        else
            currentPreviewRowsForNonSessionStorage = previewRows
    }

    private fun createPreviewRowsIntegerField(initialQueryMode: QueryMode): IntegerField {
        val previewRowsIntegerField = IntegerField()
        previewRowsIntegerField.label = getTranslation("web-padawan.previewRows")
        previewRowsIntegerField.isStepButtonsVisible = true
        previewRowsIntegerField.min = 0
        previewRowsIntegerField.value = getPreviewRows()
        previewRowsIntegerField.addValueChangeListener {
            setPreviewRows(it.value)
        }
        previewRowsIntegerField.addThemeVariants(TextFieldVariant.LUMO_SMALL)

        previewRowsIntegerField.addClassNames(
            LumoUtility.Border.RIGHT,
            LumoUtility.BorderColor.CONTRAST_30,
            LumoUtility.Padding.End.MEDIUM
        )

        if (initialQueryMode == QueryMode.STATISTICS)
            previewRowsIntegerField.addClassName(LumoUtility.Display.HIDDEN)
        addQueryModeChangedListener {
            when (it.queryMode) {
                QueryMode.STATISTICS -> previewRowsIntegerField.addClassName(LumoUtility.Display.HIDDEN)
                QueryMode.INDIVIDUAL_DATA -> previewRowsIntegerField.removeClassName(LumoUtility.Display.HIDDEN)
            }
        }
        return previewRowsIntegerField
    }

    fun updatePreviewRowsIntegerField(previewRows: Int) {
        previewRowsIntegerField.value = previewRows
    }

    fun getFurtherSettings(): Set<FurtherSettings> {
        val currentFurtherSettings = mutableSetOf<FurtherSettings>()
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        if (userSettings.userInterfaceUseSessionStorage)
            (VaadinSession.getCurrent().getAttribute(SESSION_ATTRIBUTE_FURTHER_SETTINGS) as? MutableSet<*>)
                ?.filterIsInstanceTo(currentFurtherSettings)
        else
            currentFurtherSettings.addAll(currentFurtherSettingsForNonSessionStorage)
        return currentFurtherSettings
    }

    private fun setFurtherSettings(furtherSettings: Set<FurtherSettings>) {
        val userSettings = PaDaWaNUserSettingsConnector.getUserSettings(PaDaWaNConnector.getUser())
        if (userSettings.userInterfaceUseSessionStorage)
            VaadinSession.getCurrent().setAttribute(SESSION_ATTRIBUTE_FURTHER_SETTINGS, furtherSettings)
        else {
            currentFurtherSettingsForNonSessionStorage.clear()
            currentFurtherSettingsForNonSessionStorage.addAll(furtherSettings)
        }
    }

    enum class FurtherSettings {
        RETURN_PATIENT_COUNTS,
        RETURN_ONLY_ONE_ROW_PER_PATIENT
    }

    private fun createFurtherSettingsCheckboxGroup(): CheckboxGroup<FurtherSettings> {
        val furtherSettingsCheckboxGroup = CheckboxGroup<FurtherSettings>()
        furtherSettingsCheckboxGroup.label = getTranslation("web-padawan.furtherSettings")
        furtherSettingsCheckboxGroup.setItems(FurtherSettings.entries)
        furtherSettingsCheckboxGroup.setItemEnabledProvider {
            val queryMode = getCurrentQueryMode()
            when (it) {
                FurtherSettings.RETURN_PATIENT_COUNTS -> queryMode == QueryMode.STATISTICS
                FurtherSettings.RETURN_ONLY_ONE_ROW_PER_PATIENT -> queryMode == QueryMode.INDIVIDUAL_DATA
                else -> throw IllegalArgumentException("Unknown further setting: $it")
            }
        }
        furtherSettingsCheckboxGroup.setItemLabelGenerator {
            when (it) {
                FurtherSettings.RETURN_PATIENT_COUNTS -> getTranslation("web-padawan.furtherSettings.returnPatientCounts")
                FurtherSettings.RETURN_ONLY_ONE_ROW_PER_PATIENT -> getTranslation("web-padawan.furtherSettings.returnOnlyOneRowPerPatient")
                else -> throw IllegalArgumentException("Unknown further setting: $it")
            }
        }
        furtherSettingsCheckboxGroup.value = getFurtherSettings()
        furtherSettingsCheckboxGroup.addValueChangeListener {
            val newValue = it.value
            if (it.isFromClient && newValue != null)
                setFurtherSettings(newValue)
        }
        addQueryModeChangedListener {
            furtherSettingsCheckboxGroup.dataProvider.refreshAll()
            furtherSettingsCheckboxGroup.value = getFurtherSettings()
        }
        return furtherSettingsCheckboxGroup
    }

    fun updateFurtherSettingsCheckboxGroup(furtherSettings: Set<FurtherSettings>) {
        furtherSettingsCheckboxGroup.value = furtherSettings
        setFurtherSettings(furtherSettings)
    }

    private fun createSettingsButton(): Button {
        val settingsIcon = FontIcon(FONT_ICON_FAMILY, "fa-cog")
        val settingsButton = Button(settingsIcon)
        settingsButton.addClickListener {
            val settingsDialog = SettingsDialog()
            settingsDialog.open()
            QueryAttributesCustomField.forceUIResize()
        }
        return settingsButton
    }

    fun updateQueryAttributesRowContainersForNonSessionStorage(
        attributesTypeKey: String,
        queryAttributesRowContainer: QueryAttributesRowContainer
    ) {
        queryAttributesRowContainersForNonSessionStorage[attributesTypeKey] = queryAttributesRowContainer
    }

    fun getQueryAttributesRowContainerForNonSessionStorage(attributesTypeKey: String): QueryAttributesRowContainer? =
        queryAttributesRowContainersForNonSessionStorage[attributesTypeKey]

    fun updateLastUsedQueryTitleForNonSessionStorage(queryTitle: String) {
        lastUsedQueryTitleForNonSessionStorage = queryTitle
    }

    fun getLastUsedQueryTitleForNonSessionStorage(): String? = lastUsedQueryTitleForNonSessionStorage
}