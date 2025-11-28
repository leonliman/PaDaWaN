package de.uniwue.misbased.vaadin_padawan.views.query.exportPopup

import com.vaadin.flow.component.html.H3
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.component.textfield.IntegerField
import com.vaadin.flow.component.textfield.TextField
import com.vaadin.flow.data.binder.Binder
import com.vaadin.flow.data.binder.Setter
import com.vaadin.flow.function.ValueProvider
import de.uniwue.misbased.vaadin_padawan.data.model.ExportConfiguration

class QueryExportPopupContentExcel(binder: Binder<ExportConfiguration>) : VerticalLayout() {

    init {
        isPadding = true
        isSpacing = true

        val header = H3(getTranslation("web-padawan.exportActions.exportDialog.excel.title"))
        header.setWidthFull()

        val shortenTextCheckbox = QueryExportPopup.createCheckbox(
            binder,
            "web-padawan.exportActions.exportDialog.excel.shortenText",
            ExportConfiguration::excelShortenLongTextContent::get,
            ExportConfiguration::excelShortenLongTextContent::set,
            this
        )
        shortenTextCheckbox.setWidthFull()
        val defaultColumnWidthIntegerField = createDefaultColumnWidthIntegerField(binder)
        defaultColumnWidthIntegerField.setWidthFull()
        val nameSheetTextField = createNameTextField(
            binder,
            "web-padawan.exportActions.exportDialog.excel.nameSheet",
            ExportConfiguration::excelSheetName::get,
            ExportConfiguration::excelSheetName::set
        )
        nameSheetTextField.setWidthFull()
        val nameFileTextField = createNameTextField(
            binder,
            "web-padawan.exportActions.exportDialog.excel.nameFile",
            ExportConfiguration::excelFileName::get,
            ExportConfiguration::excelFileName::set
        )
        nameFileTextField.setWidthFull()

        add(
            header,
            shortenTextCheckbox,
            defaultColumnWidthIntegerField,
            nameSheetTextField,
            nameFileTextField
        )

        val createSumColumnsAndRowsCheckbox = QueryExportPopup.createCheckbox(
            binder,
            "web-padawan.exportActions.exportDialog.excel.createSumColumnsAndRows",
            ExportConfiguration::excelIncludeTotalAndSumRowsAndColumns::get,
            ExportConfiguration::excelIncludeTotalAndSumRowsAndColumns::set,
            this
        )
        createSumColumnsAndRowsCheckbox.setWidthFull()
        add(createSumColumnsAndRowsCheckbox)
    }

    private fun createDefaultColumnWidthIntegerField(binder: Binder<ExportConfiguration>): IntegerField {
        val minBound = 1
        val minimumCountPerEntryField =
            IntegerField(getTranslation("web-padawan.exportActions.exportDialog.excel.defaultColumnWidth"))
        minimumCountPerEntryField.placeholder =
            getTranslation("web-padawan.exportActions.exportDialog.excel.automaticValue")

        val errorMessage = getTranslation("web-padawan.settings.numberEntryError", minBound)
        binder
            .forField(minimumCountPerEntryField)
            .withValidator({ it == minimumCountPerEntryField.emptyValue || it >= minBound }, errorMessage)
            .bind(ExportConfiguration::excelDefaultColumnWidth::get, ExportConfiguration::excelDefaultColumnWidth::set)
        return minimumCountPerEntryField
    }

    private fun createNameTextField(
        binder: Binder<ExportConfiguration>,
        labelKey: String,
        getter: ValueProvider<ExportConfiguration, String?>,
        setter: Setter<ExportConfiguration, String?>
    ): TextField {
        val nameTextField = TextField(getTranslation(labelKey))
        nameTextField.placeholder = getTranslation("web-padawan.exportActions.exportDialog.excel.automaticValue")

        binder
            .forField(nameTextField)
            .bind(getter, setter)
        return nameTextField
    }
}