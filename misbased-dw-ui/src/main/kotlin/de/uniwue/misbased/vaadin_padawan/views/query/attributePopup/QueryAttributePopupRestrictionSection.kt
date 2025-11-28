package de.uniwue.misbased.vaadin_padawan.views.query.attributePopup

import com.vaadin.flow.component.Component
import com.vaadin.flow.component.checkbox.Checkbox
import com.vaadin.flow.component.datepicker.DatePicker
import com.vaadin.flow.component.datepicker.DatePicker.DatePickerI18n
import com.vaadin.flow.component.html.H4
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.component.select.Select
import com.vaadin.flow.component.textfield.IntegerField
import com.vaadin.flow.component.textfield.NumberField
import com.vaadin.flow.component.textfield.TextField
import com.vaadin.flow.data.binder.Binder
import com.vaadin.flow.data.binder.ValidationResult
import de.uniwue.dw.core.model.data.CatalogEntry
import de.uniwue.dw.core.model.data.CatalogEntryType
import de.uniwue.dw.query.model.lang.ExtractionMode
import de.uniwue.dw.query.model.lang.ReductionOperator
import de.uniwue.misbased.vaadin_padawan.data.model.ContentOperatorForUI
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributeData
import java.time.LocalDate
import kotlin.reflect.KMutableProperty1

class QueryAttributePopupRestrictionSection(
    binder: Binder<QueryAttributeData>,
    private val querySection: QueryAttributePopup.QuerySection,
    private val catalogEntry: CatalogEntry
) : VerticalLayout() {

    companion object {
        private val advancedTextEntryContentOperators = arrayOf(
            ContentOperatorForUI.CONTAINS_NOT,
            ContentOperatorForUI.CONTAINS_POSITIVE,
            ContentOperatorForUI.CONTAINS_NOT_POSITIVE
        )

        private val numericAndDateEntryContentOperators = arrayOf(
            ContentOperatorForUI.EQUALS,
            ContentOperatorForUI.LESS,
            ContentOperatorForUI.LESS_OR_EQUAL,
            ContentOperatorForUI.MORE,
            ContentOperatorForUI.MORE_OR_EQUAL
        )
    }

    init {
        isPadding = false
        isSpacing = false

        val sectionTitle = H4(getTranslation("web-padawan.queryAttributeDialog.restriction"))
        add(sectionTitle)

        val contentOperatorSelect = createContentOperatorSelect(binder)
        contentOperatorSelect.setWidthFull()
        add(contentOperatorSelect)
        if (catalogEntry.dataType == CatalogEntryType.SingleChoice) {
            val singleChoiceSelect = createSingleChoiceSelect(binder, contentOperatorSelect)
            singleChoiceSelect.setWidthFull()
            add(singleChoiceSelect)
        } else if (catalogEntry.dataType == CatalogEntryType.Text) {
            val searchTextInput = createSearchTextInput(binder, contentOperatorSelect)
            searchTextInput.setWidthFull()
            add(searchTextInput)
        } else if (catalogEntry.dataType == CatalogEntryType.Number) {
            val numericRestrictionInputs = createNumericRestrictionInputs(binder, contentOperatorSelect)
            numericRestrictionInputs.forEach {
                it.setWidthFull()
                add(it)
            }
        } else if (catalogEntry.dataType == CatalogEntryType.DateTime) {
            val dateRestrictionInputs = createDateRestrictionInputs(binder, contentOperatorSelect)
            dateRestrictionInputs.forEach {
                if (it is DatePicker)
                    it.setWidthFull()
                else if (it is IntegerField)
                    it.setWidthFull()
                add(it)
            }
        }

        if (querySection == QueryAttributePopup.QuerySection.IndividualData) {
            if (catalogEntry.dataType == CatalogEntryType.Text) {
                val extractionModeSelect = createExtractionModeSelect(binder)
                extractionModeSelect.setWidthFull()
                add(extractionModeSelect)
            }

            val reductionOperatorSelect = createReductionOperatorSelect(binder)
            reductionOperatorSelect.setWidthFull()
            add(reductionOperatorSelect)

            val optionalCheckbox = QueryAttributePopupShowSection.createAttributePopupCheckbox(
                binder,
                "web-padawan.queryAttributeDialog.restriction.optional",
                QueryAttributeData::optional::get,
                QueryAttributeData::optional::set
            )
            optionalCheckbox.setWidthFull()
            add(optionalCheckbox)
        }

        val tempOpAbsMinInputs = createTempOpAbsInput(
            binder,
            "web-padawan.queryAttributeDialog.restriction.useTempOpAbsMin",
            QueryAttributeData::useTempOpAbsMin,
            QueryAttributeData::tempOpAbsMin,
        )
        val tempOpAbsMaxInputs = createTempOpAbsInput(
            binder,
            "web-padawan.queryAttributeDialog.restriction.useTempOpAbsMax",
            QueryAttributeData::useTempOpAbsMax,
            QueryAttributeData::tempOpAbsMax,
            tempOpAbsMinInputs.last() as DatePicker
        )
        val tempOpAbsInputs = tempOpAbsMinInputs + tempOpAbsMaxInputs
        tempOpAbsInputs.forEach {
            if (it is Checkbox)
                it.setWidthFull()
            else if (it is DatePicker)
                it.setWidthFull()
            add(it)
        }
    }

    private fun createContentOperatorSelect(binder: Binder<QueryAttributeData>): Select<ContentOperatorForUI> {
        val contentOperatorSelect = Select<ContentOperatorForUI>()
        contentOperatorSelect.label = getTranslation("web-padawan.queryAttributeDialog.restriction.attribute")
        contentOperatorSelect.isEmptySelectionAllowed = false

        var availableOptions = arrayOf(ContentOperatorForUI.EXISTS, ContentOperatorForUI.NOT_EXISTS)
        if ((querySection == QueryAttributePopup.QuerySection.Rows || querySection == QueryAttributePopup.QuerySection.Columns) && catalogEntry.children.isNotEmpty())
            availableOptions += ContentOperatorForUI.SHOW_SUCCESSORS

        if (catalogEntry.dataType == CatalogEntryType.SingleChoice)
            availableOptions += ContentOperatorForUI.EQUALS
        else if (catalogEntry.dataType == CatalogEntryType.Number ||
            catalogEntry.dataType == CatalogEntryType.DateTime
        ) {
            availableOptions += numericAndDateEntryContentOperators
            availableOptions += ContentOperatorForUI.BETWEEN
            if ((querySection == QueryAttributePopup.QuerySection.Rows || querySection == QueryAttributePopup.QuerySection.Columns)
                && catalogEntry.dataType == CatalogEntryType.DateTime
            )
                availableOptions += ContentOperatorForUI.PER_YEAR
        } else if (catalogEntry.dataType == CatalogEntryType.Text) {
            availableOptions += ContentOperatorForUI.CONTAINS
            availableOptions += advancedTextEntryContentOperators
        }

        contentOperatorSelect.setItems(*availableOptions)

        contentOperatorSelect.setItemLabelGenerator { contentOperator ->
            when (contentOperator) {
                ContentOperatorForUI.EXISTS -> getTranslation("web-padawan.queryAttributeDialog.restriction.attribute.exists")
                ContentOperatorForUI.NOT_EXISTS -> getTranslation("web-padawan.queryAttributeDialog.restriction.attribute.notExists")
                ContentOperatorForUI.SHOW_SUCCESSORS -> getTranslation("web-padawan.queryAttributeDialog.restriction.attribute.showSuccessors")
                ContentOperatorForUI.EQUALS -> {
                    if (catalogEntry.dataType == CatalogEntryType.SingleChoice)
                        getTranslation("web-padawan.queryAttributeDialog.restriction.attribute.is")
                    else
                        "="
                }

                ContentOperatorForUI.LESS -> "<"
                ContentOperatorForUI.LESS_OR_EQUAL -> "<="
                ContentOperatorForUI.MORE -> ">"
                ContentOperatorForUI.MORE_OR_EQUAL -> ">="
                ContentOperatorForUI.BETWEEN -> getTranslation("web-padawan.queryAttributeDialog.restriction.attribute.between")
                ContentOperatorForUI.PER_YEAR -> getTranslation("web-padawan.queryAttributeDialog.restriction.attribute.perYear")
                ContentOperatorForUI.CONTAINS -> getTranslation("web-padawan.queryAttributeDialog.restriction.attribute.contains")
                ContentOperatorForUI.CONTAINS_NOT -> getTranslation("web-padawan.queryAttributeDialog.restriction.attribute.containsNot")
                ContentOperatorForUI.CONTAINS_POSITIVE -> getTranslation("web-padawan.queryAttributeDialog.restriction.attribute.containsPositive")
                ContentOperatorForUI.CONTAINS_NOT_POSITIVE -> getTranslation("web-padawan.queryAttributeDialog.restriction.attribute.containsNotPositive")
                else -> getTranslation("web-padawan.queryAttributeDialog.restriction.attribute.unknown")
            }
        }

        val errorMessage = getTranslation("web-padawan.settings.selectError")
        binder.forField(contentOperatorSelect)
            .asRequired(errorMessage)
            .bind(QueryAttributeData::contentOperator::get, QueryAttributeData::contentOperator::set)
        return contentOperatorSelect
    }

    private fun createSingleChoiceSelect(
        binder: Binder<QueryAttributeData>,
        contentOperatorSelect: Select<ContentOperatorForUI>
    ): Select<String> {
        val singleChoiceSelect = Select<String>()
        singleChoiceSelect.label = getTranslation("web-padawan.queryAttributeDialog.restriction.value")
        singleChoiceSelect.isEmptySelectionAllowed = true

        singleChoiceSelect.setItems(*catalogEntry.singleChoiceChoice.toTypedArray().sortedArray())

        singleChoiceSelect.isVisible = contentOperatorSelect.value == ContentOperatorForUI.EQUALS
        contentOperatorSelect.addValueChangeListener { event ->
            singleChoiceSelect.isVisible = event.value == ContentOperatorForUI.EQUALS
            singleChoiceSelect.value = singleChoiceSelect.emptyValue
        }

        val errorMessage = getTranslation("web-padawan.settings.selectError")
        binder.forField(singleChoiceSelect)
            .asRequired { value, _ ->
                if (singleChoiceSelect.isVisible.not())
                    ValidationResult.ok()
                else
                    if (value == singleChoiceSelect.emptyValue)
                        ValidationResult.error(errorMessage)
                    else
                        ValidationResult.ok()
            }
            .bind(QueryAttributeData::desiredContent::get, QueryAttributeData::desiredContent::set)
        return singleChoiceSelect
    }

    private fun createSearchTextInput(
        binder: Binder<QueryAttributeData>,
        contentOperatorSelect: Select<ContentOperatorForUI>
    ): TextField {
        val searchTextInput = TextField()
        searchTextInput.label = getTranslation("web-padawan.queryAttributeDialog.restriction.value.searchTerm")

        searchTextInput.isVisible = advancedTextEntryContentOperators.contains(contentOperatorSelect.value) ||
                contentOperatorSelect.value == ContentOperatorForUI.CONTAINS
        contentOperatorSelect.addValueChangeListener { event ->
            val previousVisibility = searchTextInput.isVisible
            searchTextInput.isVisible = advancedTextEntryContentOperators.contains(event.value) ||
                    event.value == ContentOperatorForUI.CONTAINS
            if (previousVisibility != searchTextInput.isVisible)
                searchTextInput.value = searchTextInput.emptyValue
        }

        val errorMessage = getTranslation("web-padawan.queryAttributeDialog.restriction.value.searchTerm.error")
        binder.forField(searchTextInput)
            .asRequired { value, _ ->
                if (searchTextInput.isVisible.not())
                    ValidationResult.ok()
                else
                    if (value.isBlank())
                        ValidationResult.error(errorMessage)
                    else
                        ValidationResult.ok()
            }
            .bind(QueryAttributeData::desiredContent::get, QueryAttributeData::desiredContent::set)
        return searchTextInput
    }

    private fun createNumericRestrictionInputs(
        binder: Binder<QueryAttributeData>,
        contentOperatorSelect: Select<ContentOperatorForUI>
    ): List<NumberField> {
        val numericRestrictionInputs = mutableListOf<NumberField>()

        val valueInput = NumberField()
        valueInput.label = getTranslation("web-padawan.queryAttributeDialog.restriction.value")
        valueInput.isVisible = numericAndDateEntryContentOperators.contains(contentOperatorSelect.value)
        contentOperatorSelect.addValueChangeListener { event ->
            val previousVisibility = valueInput.isVisible
            valueInput.isVisible = numericAndDateEntryContentOperators.contains(event.value)
            if (previousVisibility != valueInput.isVisible)
                valueInput.value = valueInput.emptyValue
        }
        val errorMessage = getTranslation("web-padawan.queryAttributeDialog.restriction.value.error")
        binder.forField(valueInput)
            .asRequired { value, _ ->
                if (valueInput.isVisible.not())
                    ValidationResult.ok()
                else
                    if (value == valueInput.emptyValue)
                        ValidationResult.error(errorMessage)
                    else
                        ValidationResult.ok()
            }
            .bind(QueryAttributeData::desiredContentNumeric::get, QueryAttributeData::desiredContentNumeric::set)
        numericRestrictionInputs.add(valueInput)

        val lowerBoundInput = createNumericBoundInput(
            contentOperatorSelect,
            binder,
            "web-padawan.queryAttributeDialog.restriction.value.lowerBound",
            errorMessage,
            QueryAttributeData::desiredContentBetweenLowerBoundNumeric
        )
        numericRestrictionInputs.add(lowerBoundInput)

        val upperBoundInput = createNumericBoundInput(
            contentOperatorSelect,
            binder,
            "web-padawan.queryAttributeDialog.restriction.value.upperBound",
            errorMessage,
            QueryAttributeData::desiredContentBetweenUpperBoundNumeric,
            lowerBoundInput
        )
        numericRestrictionInputs.add(upperBoundInput)

        return numericRestrictionInputs
    }

    private fun createNumericBoundInput(
        contentOperatorSelect: Select<ContentOperatorForUI>,
        binder: Binder<QueryAttributeData>,
        labelTranslationKey: String,
        errorMessage: String,
        numericBoundProperty: KMutableProperty1<QueryAttributeData, Double?>,
        previousBoundField: NumberField? = null
    ): NumberField {
        val numericBoundInput = NumberField()
        numericBoundInput.label = getTranslation(labelTranslationKey)
        numericBoundInput.isVisible = contentOperatorSelect.value == ContentOperatorForUI.BETWEEN
        contentOperatorSelect.addValueChangeListener { event ->
            numericBoundInput.isVisible = event.value == ContentOperatorForUI.BETWEEN
            numericBoundInput.value = numericBoundInput.emptyValue
        }
        binder.forField(numericBoundInput)
            .asRequired { value, _ ->
                if (numericBoundInput.isVisible.not())
                    ValidationResult.ok()
                else
                    if (value == numericBoundInput.emptyValue)
                        ValidationResult.error(errorMessage)
                    else if (previousBoundField != null && previousBoundField.value != previousBoundField.emptyValue && value <= previousBoundField.value)
                        ValidationResult.error(getTranslation("web-padawan.queryAttributeDialog.restriction.value.upperBound.error.numeric"))
                    else
                        ValidationResult.ok()
            }
            .bind(
                numericBoundProperty::get,
                numericBoundProperty::set
            )
        return numericBoundInput
    }

    private fun createDateRestrictionInputs(
        binder: Binder<QueryAttributeData>,
        contentOperatorSelect: Select<ContentOperatorForUI>
    ): List<Component> {
        val dateRestrictionInputs = mutableListOf<Component>()

        val valueInput = DatePicker()
        valueInput.label = getTranslation("web-padawan.queryAttributeDialog.restriction.value")
        valueInput.isWeekNumbersVisible = true
        valueInput.i18n = getLocalizationForDatePicker()
        valueInput.isVisible = numericAndDateEntryContentOperators.contains(contentOperatorSelect.value)
        contentOperatorSelect.addValueChangeListener { event ->
            val previousVisibility = valueInput.isVisible
            valueInput.isVisible = numericAndDateEntryContentOperators.contains(event.value)
            if (previousVisibility != valueInput.isVisible)
                valueInput.value = valueInput.emptyValue
        }
        val errorMessage = getTranslation("web-padawan.queryAttributeDialog.restriction.value.date.error")
        binder.forField(valueInput)
            .asRequired { value, _ ->
                if (valueInput.isVisible.not())
                    ValidationResult.ok()
                else
                    if (value == valueInput.emptyValue)
                        ValidationResult.error(errorMessage)
                    else
                        ValidationResult.ok()
            }
            .bind(QueryAttributeData::desiredContentDate::get, QueryAttributeData::desiredContentDate::set)
        dateRestrictionInputs.add(valueInput)

        val lowerBoundInput = createDateBoundInput(
            contentOperatorSelect,
            binder,
            "web-padawan.queryAttributeDialog.restriction.value.lowerBound",
            errorMessage,
            QueryAttributeData::desiredContentBetweenLowerBoundDate
        )
        dateRestrictionInputs.add(lowerBoundInput)

        val upperBoundInput = createDateBoundInput(
            contentOperatorSelect,
            binder,
            "web-padawan.queryAttributeDialog.restriction.value.upperBound",
            errorMessage,
            QueryAttributeData::desiredContentBetweenUpperBoundDate,
            lowerBoundInput
        )
        dateRestrictionInputs.add(upperBoundInput)

        val errorMessageYear = getTranslation("web-padawan.queryAttributeDialog.restriction.value.year.error")
        val lowerBoundYearInput = createYearBoundInput(
            contentOperatorSelect,
            binder,
            "web-padawan.queryAttributeDialog.restriction.value.lowerBound",
            errorMessageYear,
            QueryAttributeData::desiredContentBetweenLowerBoundYear
        )
        dateRestrictionInputs.add(lowerBoundYearInput)

        val upperBoundYearInput = createYearBoundInput(
            contentOperatorSelect,
            binder,
            "web-padawan.queryAttributeDialog.restriction.value.upperBound",
            errorMessageYear,
            QueryAttributeData::desiredContentBetweenUpperBoundYear,
            lowerBoundYearInput
        )
        dateRestrictionInputs.add(upperBoundYearInput)

        return dateRestrictionInputs
    }

    private fun getLocalizationForDatePicker(): DatePickerI18n {
        val datePickerI18n = DatePickerI18n()
        datePickerI18n.firstDayOfWeek = 1
        datePickerI18n.monthNames = listOf(
            getTranslation("web-padawan.datePicker.months.january"),
            getTranslation("web-padawan.datePicker.months.february"),
            getTranslation("web-padawan.datePicker.months.march"),
            getTranslation("web-padawan.datePicker.months.april"),
            getTranslation("web-padawan.datePicker.months.may"),
            getTranslation("web-padawan.datePicker.months.june"),
            getTranslation("web-padawan.datePicker.months.july"),
            getTranslation("web-padawan.datePicker.months.august"),
            getTranslation("web-padawan.datePicker.months.september"),
            getTranslation("web-padawan.datePicker.months.october"),
            getTranslation("web-padawan.datePicker.months.november"),
            getTranslation("web-padawan.datePicker.months.december")
        )
        datePickerI18n.weekdays = listOf(
            getTranslation("web-padawan.datePicker.weekdays.sunday"),
            getTranslation("web-padawan.datePicker.weekdays.monday"),
            getTranslation("web-padawan.datePicker.weekdays.tuesday"),
            getTranslation("web-padawan.datePicker.weekdays.wednesday"),
            getTranslation("web-padawan.datePicker.weekdays.thursday"),
            getTranslation("web-padawan.datePicker.weekdays.friday"),
            getTranslation("web-padawan.datePicker.weekdays.saturday")
        )
        datePickerI18n.weekdaysShort = listOf(
            getTranslation("web-padawan.datePicker.weekdays.short.sunday"),
            getTranslation("web-padawan.datePicker.weekdays.short.monday"),
            getTranslation("web-padawan.datePicker.weekdays.short.tuesday"),
            getTranslation("web-padawan.datePicker.weekdays.short.wednesday"),
            getTranslation("web-padawan.datePicker.weekdays.short.thursday"),
            getTranslation("web-padawan.datePicker.weekdays.short.friday"),
            getTranslation("web-padawan.datePicker.weekdays.short.saturday")
        )
        datePickerI18n.today = getTranslation("web-padawan.datePicker.today")
        datePickerI18n.cancel = getTranslation("web-padawan.datePicker.cancel")
        return datePickerI18n
    }

    private fun createDateBoundInput(
        contentOperatorSelect: Select<ContentOperatorForUI>,
        binder: Binder<QueryAttributeData>,
        labelTranslationKey: String,
        errorMessage: String,
        dateBoundProperty: KMutableProperty1<QueryAttributeData, LocalDate?>,
        previousBoundField: DatePicker? = null
    ): DatePicker {
        val dateBoundInput = DatePicker()
        dateBoundInput.label = getTranslation(labelTranslationKey)
        dateBoundInput.isWeekNumbersVisible = true
        dateBoundInput.i18n = getLocalizationForDatePicker()
        dateBoundInput.isVisible = contentOperatorSelect.value == ContentOperatorForUI.BETWEEN
        contentOperatorSelect.addValueChangeListener { event ->
            dateBoundInput.isVisible = event.value == ContentOperatorForUI.BETWEEN
            dateBoundInput.value = dateBoundInput.emptyValue
        }
        binder.forField(dateBoundInput)
            .asRequired { value, _ ->
                if (dateBoundInput.isVisible.not())
                    ValidationResult.ok()
                else
                    if (value == dateBoundInput.emptyValue)
                        ValidationResult.error(errorMessage)
                    else if (previousBoundField != null && previousBoundField.value != previousBoundField.emptyValue && value <= previousBoundField.value)
                        ValidationResult.error(getTranslation("web-padawan.queryAttributeDialog.restriction.value.upperBound.error.date"))
                    else
                        ValidationResult.ok()
            }
            .bind(
                dateBoundProperty::get,
                dateBoundProperty::set
            )
        return dateBoundInput
    }

    private fun createYearBoundInput(
        contentOperatorSelect: Select<ContentOperatorForUI>,
        binder: Binder<QueryAttributeData>,
        labelTranslationKey: String,
        errorMessage: String,
        yearBoundProperty: KMutableProperty1<QueryAttributeData, Int?>,
        previousBoundField: IntegerField? = null
    ): IntegerField {
        val yearBoundInput = IntegerField()
        yearBoundInput.label = getTranslation(labelTranslationKey)
        yearBoundInput.isVisible = contentOperatorSelect.value == ContentOperatorForUI.PER_YEAR
        contentOperatorSelect.addValueChangeListener { event ->
            yearBoundInput.isVisible = event.value == ContentOperatorForUI.PER_YEAR
            yearBoundInput.value = yearBoundInput.emptyValue
        }
        binder.forField(yearBoundInput)
            .asRequired { value, _ ->
                if (yearBoundInput.isVisible.not())
                    ValidationResult.ok()
                else
                    if (value == yearBoundInput.emptyValue)
                        ValidationResult.error(errorMessage)
                    else if (previousBoundField != null && previousBoundField.value != previousBoundField.emptyValue && value <= previousBoundField.value)
                        ValidationResult.error(getTranslation("web-padawan.queryAttributeDialog.restriction.value.upperBound.error.year"))
                    else
                        ValidationResult.ok()
            }
            .bind(
                yearBoundProperty::get,
                yearBoundProperty::set
            )
        return yearBoundInput
    }

    private fun createExtractionModeSelect(binder: Binder<QueryAttributeData>): Select<ExtractionMode> {
        val extractionModeSelect = Select<ExtractionMode>()
        extractionModeSelect.label = getTranslation("web-padawan.queryAttributeDialog.restriction.extractionMode")
        extractionModeSelect.isEmptySelectionAllowed = false

        val availableOptions = arrayOf(ExtractionMode.None, ExtractionMode.NextNumber, ExtractionMode.BetweenHits)
        extractionModeSelect.setItems(*availableOptions)

        extractionModeSelect.setItemLabelGenerator { extractionMode ->
            when (extractionMode) {
                ExtractionMode.None -> getTranslation("web-padawan.queryAttributeDialog.restriction.extractionMode.none")
                ExtractionMode.NextNumber -> getTranslation("web-padawan.queryAttributeDialog.restriction.extractionMode.nextNumber")
                ExtractionMode.BetweenHits -> getTranslation("web-padawan.queryAttributeDialog.restriction.extractionMode.betweenHits")
                else -> getTranslation("web-padawan.queryAttributeDialog.restriction.extractionMode.unknown")
            }
        }

        val errorMessage = getTranslation("web-padawan.settings.selectError")
        binder.forField(extractionModeSelect)
            .asRequired(errorMessage)
            .bind(QueryAttributeData::extractionMode::get, QueryAttributeData::extractionMode::set)
        return extractionModeSelect
    }

    private fun createReductionOperatorSelect(binder: Binder<QueryAttributeData>): Select<ReductionOperator> {
        val reductionOperatorSelect = Select<ReductionOperator>()
        reductionOperatorSelect.label = getTranslation("web-padawan.queryAttributeDialog.restriction.reductionOperator")
        reductionOperatorSelect.isEmptySelectionAllowed = false

        var availableOptions = arrayOf(ReductionOperator.NONE, ReductionOperator.EARLIEST, ReductionOperator.LATEST)
        if (catalogEntry.dataType == CatalogEntryType.Number)
            availableOptions += arrayOf(ReductionOperator.MIN, ReductionOperator.MAX)
        reductionOperatorSelect.setItems(*availableOptions)

        reductionOperatorSelect.setItemLabelGenerator { reductionOperator ->
            when (reductionOperator) {
                ReductionOperator.NONE -> getTranslation("web-padawan.queryAttributeDialog.restriction.reductionOperator.none")
                ReductionOperator.EARLIEST -> getTranslation("web-padawan.queryAttributeDialog.restriction.reductionOperator.earliest")
                ReductionOperator.LATEST -> getTranslation("web-padawan.queryAttributeDialog.restriction.reductionOperator.latest")
                ReductionOperator.MIN -> getTranslation("web-padawan.queryAttributeDialog.restriction.reductionOperator.min")
                ReductionOperator.MAX -> getTranslation("web-padawan.queryAttributeDialog.restriction.reductionOperator.max")
                else -> getTranslation("web-padawan.queryAttributeDialog.restriction.reductionOperator.unknown")
            }
        }

        val errorMessage = getTranslation("web-padawan.settings.selectError")
        binder.forField(reductionOperatorSelect)
            .asRequired(errorMessage)
            .bind(QueryAttributeData::reductionOperator::get, QueryAttributeData::reductionOperator::set)
        return reductionOperatorSelect
    }

    private fun createTempOpAbsInput(
        binder: Binder<QueryAttributeData>,
        labelTranslationKey: String,
        tempOpAbsProperty: KMutableProperty1<QueryAttributeData, Boolean>,
        tempOpAbsValueProperty: KMutableProperty1<QueryAttributeData, LocalDate?>,
        previousTempOpAbsField: DatePicker? = null
    ): List<Component> {
        val tempOpAbsInputs = mutableListOf<Component>()

        val useTempOpAbsCheckbox = QueryAttributePopupShowSection.createAttributePopupCheckbox(
            binder,
            labelTranslationKey,
            tempOpAbsProperty::get,
            tempOpAbsProperty::set
        )
        tempOpAbsInputs.add(useTempOpAbsCheckbox)

        val tempOpAbsInput = DatePicker()
        tempOpAbsInput.label = getTranslation("web-padawan.queryAttributeDialog.restriction.value")
        tempOpAbsInput.isWeekNumbersVisible = true
        tempOpAbsInput.i18n = getLocalizationForDatePicker()
        tempOpAbsInput.isVisible = useTempOpAbsCheckbox.value
        useTempOpAbsCheckbox.addValueChangeListener { event ->
            tempOpAbsInput.isVisible = event.value
            tempOpAbsInput.value = tempOpAbsInput.emptyValue
        }
        binder.forField(tempOpAbsInput)
            .asRequired { value, _ ->
                if (tempOpAbsInput.isVisible.not())
                    ValidationResult.ok()
                else
                    if (value == tempOpAbsInput.emptyValue)
                        ValidationResult.error(getTranslation("web-padawan.queryAttributeDialog.restriction.value.date.error"))
                    else if (previousTempOpAbsField != null && previousTempOpAbsField.isVisible && previousTempOpAbsField.value != previousTempOpAbsField.emptyValue && value <= previousTempOpAbsField.value)
                        ValidationResult.error(getTranslation("web-padawan.queryAttributeDialog.restriction.useTempOpAbsMax.error"))
                    else
                        ValidationResult.ok()
            }
            .bind(
                tempOpAbsValueProperty::get,
                tempOpAbsValueProperty::set
            )
        tempOpAbsInputs.add(tempOpAbsInput)
        return tempOpAbsInputs
    }
}