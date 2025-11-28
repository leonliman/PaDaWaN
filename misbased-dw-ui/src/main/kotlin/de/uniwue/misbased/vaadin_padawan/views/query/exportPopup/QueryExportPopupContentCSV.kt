package de.uniwue.misbased.vaadin_padawan.views.query.exportPopup

import com.vaadin.flow.component.html.H3
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.component.select.Select
import com.vaadin.flow.component.textfield.TextField
import com.vaadin.flow.data.binder.Binder
import com.vaadin.flow.data.binder.Setter
import com.vaadin.flow.function.ValueProvider
import de.uniwue.misbased.vaadin_padawan.data.model.ExportConfiguration
import org.apache.commons.csv.QuoteMode

class QueryExportPopupContentCSV(binder: Binder<ExportConfiguration>) : VerticalLayout() {

    init {
        isPadding = true
        isSpacing = true

        val header = H3(getTranslation("web-padawan.exportActions.exportDialog.csv.title"))
        header.setWidthFull()

        val delimiterCharField = createCharField(
            binder,
            "web-padawan.exportActions.exportDialog.csv.delimiter",
            ExportConfiguration::csvDelimiter::get,
            ExportConfiguration::csvDelimiter::set
        )
        delimiterCharField.setWidthFull()
        val recordSeparatorTextField = createRecordSeparatorTextField(binder)
        recordSeparatorTextField.setWidthFull()
        val escapeCharField = createCharField(
            binder,
            "web-padawan.exportActions.exportDialog.csv.escape",
            ExportConfiguration::csvEscape::get,
            ExportConfiguration::csvEscape::set
        )
        escapeCharField.setWidthFull()
        val quoteCharField = createCharField(
            binder,
            "web-padawan.exportActions.exportDialog.csv.quote",
            ExportConfiguration::csvQuote::get,
            ExportConfiguration::csvQuote::set
        )
        quoteCharField.setWidthFull()
        val quoteModeSelect = createQuoteModeSelect(binder)
        quoteModeSelect.setWidthFull()
        val useUTF8Checkbox = QueryExportPopup.createCheckbox(
            binder,
            "web-padawan.exportActions.exportDialog.csv.useUTF8",
            ExportConfiguration::csvUseUTF8::get,
            ExportConfiguration::csvUseUTF8::set,
            this
        )
        useUTF8Checkbox.setWidthFull()

        add(
            header,
            delimiterCharField,
            recordSeparatorTextField,
            escapeCharField,
            quoteCharField,
            quoteModeSelect,
            useUTF8Checkbox
        )
    }

    private fun createCharField(
        binder: Binder<ExportConfiguration>,
        labelKey: String,
        getter: ValueProvider<ExportConfiguration, Char>,
        setter: Setter<ExportConfiguration, Char>
    ): TextField {
        val charField = TextField(getTranslation(labelKey))
        charField.minLength = 1
        charField.maxLength = 1

        binder
            .forField(charField)
            .withValidator(
                { it.length == 1 },
                getTranslation("web-padawan.exportActions.exportDialog.csv.charEntryError")
            )
            .withConverter({ it.single() }, { it.toString() })
            .bind(getter, setter)
        return charField
    }

    private fun createRecordSeparatorTextField(
        binder: Binder<ExportConfiguration>
    ): TextField {
        val recordSeparatorTextField =
            TextField(getTranslation("web-padawan.exportActions.exportDialog.csv.recordSeparator"))
        recordSeparatorTextField.minLength = 1
        recordSeparatorTextField.maxLength = 2

        binder
            .forField(recordSeparatorTextField)
            .withValidator(
                { it != recordSeparatorTextField.emptyValue && it.length <= 2 },
                getTranslation("web-padawan.exportActions.exportDialog.csv.recordSeparator.error")
            )
            .bind(ExportConfiguration::csvRecordSeparator::get, ExportConfiguration::csvRecordSeparator::set)
        return recordSeparatorTextField
    }

    private fun createQuoteModeSelect(binder: Binder<ExportConfiguration>): Select<QuoteMode> {
        val quoteModeSelect = Select<QuoteMode>()
        quoteModeSelect.label = getTranslation("web-padawan.exportActions.exportDialog.csv.quoteMode")
        quoteModeSelect.isEmptySelectionAllowed = false
        quoteModeSelect.setItems(
            QuoteMode.ALL,
            QuoteMode.ALL_NON_NULL,
            QuoteMode.MINIMAL,
            QuoteMode.NON_NUMERIC,
            QuoteMode.NONE
        )
        quoteModeSelect.setItemLabelGenerator { quoteMode ->
            when (quoteMode) {
                QuoteMode.ALL -> getTranslation("web-padawan.exportActions.exportDialog.csv.quoteMode.all")
                QuoteMode.ALL_NON_NULL -> getTranslation("web-padawan.exportActions.exportDialog.csv.quoteMode.allNonNull")
                QuoteMode.MINIMAL -> getTranslation("web-padawan.exportActions.exportDialog.csv.quoteMode.minimal")
                QuoteMode.NON_NUMERIC -> getTranslation("web-padawan.exportActions.exportDialog.csv.quoteMode.nonNumeric")
                QuoteMode.NONE -> getTranslation("web-padawan.exportActions.exportDialog.csv.quoteMode.none")
                else -> getTranslation("web-padawan.exportActions.exportDialog.csv.quoteMode.unknown")
            }
        }

        val errorMessage = getTranslation("web-padawan.settings.selectError")
        binder.forField(quoteModeSelect)
            .asRequired(errorMessage)
            .bind(ExportConfiguration::csvQuoteMode::get, ExportConfiguration::csvQuoteMode::set)
        return quoteModeSelect
    }
}